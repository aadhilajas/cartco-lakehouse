"""Silver transform for CartCo's Shopify sales channel.

Builds on the Bronze contract established in ``ingestion/shopify_bronze.py``:
Bronze lands raw, unmodified Shopify orders into ``bronze.shopify_orders``
partitioned by ``ingestion_date`` (load date). This module reads *today's*
Bronze partition and conforms it to the canonical Silver entity model,
writing the result to ``silver.orders`` (the shared canonical Silver table)
as an Apache Iceberg table on MinIO, via the same Iceberg REST catalog.

IMPORTANT — shared ``silver.orders`` table:
    Both this Shopify Silver transform and the Amazon Silver transform
    (``transforms/amazon_silver.py``) write to the SAME Iceberg table:
    ``silver.orders``. The two channels are distinguished by the ``channel``
    column (``'shopify'`` vs ``'amazon'``). This transform writes only its own
    channel's rows, scoped by a ``channel='shopify'`` overwrite filter, so
    Amazon rows in the same ``order_date`` partitions are never touched.

Silver transformations applied, in order:
    1. Schema canonicalisation: ``status`` -> ``order_status``, mapping
       Bronze's source-faithful values to the canonical vocabulary shared
       across all Silver channels (``delivered`` -> ``completed``,
       ``cancelled`` -> ``cancelled``, ``returned`` -> ``refunded``; see
       ``STATUS_CANONICAL_MAP``). This is Silver's job, not Bronze's: per
       the Bronze contract in ``ingestion/shopify_bronze.py``, Bronze stays
       source-faithful and performs no business logic, so the source ->
       canonical mapping only happens here.
    2. Type enforcement: ``order_date`` is truncated from timestamp to date.
    3. Deduplication: one row per ``order_id``, keeping the row with the
       latest ``order_date`` (handles Bronze re-ingestion of the same order).
    4. Null handling: rows with a null ``order_id`` or ``customer_id`` are
       dropped as unrecoverable; ``quantity``, ``unit_price``, and
       ``product_category`` nulls are filled with defaults.
    5. Derived column: ``order_total = quantity * unit_price``.
    6. Added column: ``transform_timestamp`` (UTC, the moment this Silver
       record was written).

Idempotency: Silver is partitioned by ``order_date`` (business date), not
``ingestion_date``. Each run computes the min/max ``order_date`` present in
today's transformed batch and does a partition-level overwrite of exactly
that date range, so re-running against the same Bronze partition replaces
(never duplicates) those rows.

Lineage: ``run()`` emits START/COMPLETE/FAIL OpenLineage events via
``lineage/emitter.py`` (job ``shopify_bronze_ingestion.transform_to_silver``,
input dataset ``bronze.shopify_orders``, output dataset
``silver.orders``). No-op if ``OPENLINEAGE_URL`` is unset or Marquez
is unreachable -- lineage emission never affects the transform itself. See
``docs/lineage.md``.

Endpoints default to the in-cluster Docker service names; override via env
vars when running outside the compose network (see ``ingestion/shopify_bronze.py``
for the same convention).

Run inside the Airflow container (deps are pre-installed there):
    docker exec cartco-airflow-scheduler \\
        python /opt/airflow/transforms/silver_shopify_orders.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

# repo root is volume-mounted at /opt/airflow but isn't guaranteed to be on
# sys.path for every process that runs this script directly (see the same
# pattern in dags/shopify_bronze_dag.py).
REPO_ROOT = "/opt/airflow"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lineage import emitter as lineage  # noqa: E402

LINEAGE_JOB_NAME = "shopify_bronze_ingestion.transform_to_silver"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("shopify_silver")

# --- Configuration (env-overridable; defaults target the compose network) ---
ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

BRONZE_TABLE_IDENTIFIER = os.environ.get("SHOPIFY_BRONZE_TABLE", "bronze.shopify_orders")

NAMESPACE = "silver"
# Shared canonical Silver table — both Shopify and Amazon channels write here.
# Channel is preserved in the 'channel' column; each transform scopes its
# overwrite filter to its own channel so neither ever clobbers the other.
TABLE_NAME = "orders"
TABLE_IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"

# Canonical Silver schema. Field ids follow this list's order (order_id=1 ..
# transform_timestamp=11) rather than reusing Bronze's ingestion_date id
# (10) — in this schema, field 10 is order_total, not order_date, so the
# partition source must reference order_date's own id (4) below.
TABLE_SCHEMA = Schema(
    NestedField(1, "order_id", StringType(), required=True),
    NestedField(2, "customer_id", StringType(), required=True),
    NestedField(3, "channel", StringType(), required=False),
    NestedField(4, "order_date", DateType(), required=False),
    NestedField(5, "product_id", StringType(), required=False),
    NestedField(6, "product_category", StringType(), required=False),
    NestedField(7, "quantity", IntegerType(), required=False),
    NestedField(8, "unit_price", DoubleType(), required=False),
    NestedField(9, "order_status", StringType(), required=False),
    NestedField(10, "order_total", DoubleType(), required=False),
    NestedField(11, "transform_timestamp", TimestampType(), required=False),
)

# Partition by order_date and channel for cross-channel overwrite isolation
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="order_date"),
    PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="channel"),
)

COLUMN_ORDER = [
    "order_id",
    "customer_id",
    "channel",
    "order_date",
    "product_id",
    "product_category",
    "quantity",
    "unit_price",
    "order_status",
    "order_total",
    "transform_timestamp",
]

# Bronze -> Silver canonical status mapping. Bronze (ingestion/shopify_bronze.py)
# is source-faithful and only ever emits the source system's own status
# vocabulary (delivered/cancelled/returned, enforced by the pre-Silver GE
# checkpoint in transforms/ge_bronze_shopify_checkpoint.py). Silver owns
# translating that source vocabulary into the canonical vocabulary shared
# across all Silver channels — this dict is that translation.
STATUS_CANONICAL_MAP = {
    "delivered": "completed",
    "cancelled": "cancelled",
    "returned": "refunded",
}

# Fallback for any status value outside STATUS_CANONICAL_MAP. We map to
# "unknown" rather than raising: the GE checkpoint is the layer responsible
# for rejecting genuinely invalid Bronze data *before* it reaches Silver, so
# by the time a batch gets here its statuses should already be known-good.
# This fallback is a defensive backstop only — aborting the entire Silver
# batch over one unrecognised categorical value would be disproportionate
# and would make Silver more brittle than the contract it depends on;
# "unknown" keeps the row flowing while still being visibly flagged (via the
# rows_unknown_status stat logged in run()) for follow-up investigation.
UNKNOWN_STATUS_LABEL = "unknown"


def get_catalog():
    """Load the Iceberg REST catalog with S3 (MinIO) credentials."""
    return load_catalog(
        "cartco",
        **{
            "type": "rest",
            "uri": ICEBERG_CATALOG_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
            "s3.region": AWS_REGION,
        },
    )


def ensure_table(catalog):
    """Return the Silver table, creating the namespace/table if needed."""
    try:
        catalog.create_namespace(NAMESPACE)
        logger.info("Created namespace '%s'", NAMESPACE)
    except NamespaceAlreadyExistsError:
        logger.info("Namespace '%s' already exists", NAMESPACE)

    try:
        table = catalog.load_table(TABLE_IDENTIFIER)
        logger.info("Loaded existing table '%s'", TABLE_IDENTIFIER)
    except NoSuchTableError:
        table = catalog.create_table(
            TABLE_IDENTIFIER,
            schema=TABLE_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )
        logger.info("Created table '%s' partitioned by order_date", TABLE_IDENTIFIER)
    return table


def _canonicalize_status_to_order_status(
    table: pa.Table, stats: dict[str, int] | None = None
) -> pa.Table:
    """Map Bronze's source-faithful ``status`` to the canonical ``order_status``.

    Applies ``STATUS_CANONICAL_MAP`` (e.g. ``delivered`` -> ``completed``)
    and renames the column from ``status`` to ``order_status`` in the same
    step. Values outside the map are set to ``UNKNOWN_STATUS_LABEL`` rather
    than raising (see the fallback rationale above ``UNKNOWN_STATUS_LABEL``).
    If a ``stats`` dict is supplied, the count of unmapped rows is recorded
    as ``rows_unknown_status`` for the caller to log/monitor.
    """
    status_col = table.column("status")

    mapped = pa.scalar(UNKNOWN_STATUS_LABEL, type=status_col.type)
    is_known_mask = None
    for source_value, canonical_value in STATUS_CANONICAL_MAP.items():
        is_match = pc.equal(status_col, source_value)
        mapped = pc.if_else(is_match, pa.scalar(canonical_value, type=status_col.type), mapped)
        is_known_mask = is_match if is_known_mask is None else pc.or_(is_known_mask, is_match)

    if stats is not None:
        unknown_count = pc.sum(pc.cast(pc.invert(is_known_mask), pa.int64())).as_py()
        stats["rows_unknown_status"] = unknown_count or 0

    idx = table.schema.get_field_index("status")
    return table.set_column(idx, "order_status", mapped)


def _truncate_order_date_to_date(table: pa.Table) -> pa.Table:
    """Cast ``order_date`` from timestamp to date, dropping the time component."""
    truncated = pc.cast(table.column("order_date"), pa.date32())
    idx = table.schema.get_field_index("order_date")
    return table.set_column(idx, "order_date", truncated)


def _deduplicate_latest_by_order_id(table: pa.Table) -> pa.Table:
    """Keep exactly one row per ``order_id``, preferring the latest ``order_date``.

    Implemented with pure PyArrow (no pandas): sorts by ``order_id`` ascending
    and ``order_date`` descending, then keeps only the first row of each
    ``order_id`` group by comparing each row's ``order_id`` to the previous
    row's in the sorted sequence.
    """
    if table.num_rows == 0:
        return table

    sort_indices = pc.sort_indices(
        table,
        sort_keys=[("order_id", "ascending"), ("order_date", "descending")],
    )
    sorted_table = table.take(sort_indices)

    order_ids = sorted_table.column("order_id")
    if len(order_ids) <= 1:
        return sorted_table

    # ChunkedArray.combine_chunks() returns a single flat Array directly on
    # pyarrow==15.0.2 (verified live against the installed version), not a
    # single-chunk ChunkedArray, so no further .chunk(0) unwrap is needed or
    # valid here.
    flat_order_ids = order_ids.combine_chunks()
    previous = flat_order_ids.slice(0, len(flat_order_ids) - 1)
    current = flat_order_ids.slice(1, len(flat_order_ids) - 1)
    is_group_start = pc.invert(pc.equal(current, previous))
    keep_mask = pa.concat_arrays(
        [pa.array([True], type=pa.bool_()), is_group_start]
    )
    return sorted_table.filter(keep_mask)


def _drop_unrecoverable_nulls(table: pa.Table) -> pa.Table:
    """Drop rows with a null ``order_id`` or ``customer_id`` (unrecoverable)."""
    mask = pc.and_(
        pc.is_valid(table.column("order_id")),
        pc.is_valid(table.column("customer_id")),
    )
    return table.filter(mask)


def _fill_default_nulls(table: pa.Table) -> pa.Table:
    """Fill ``quantity``, ``unit_price``, and ``product_category`` nulls with defaults."""
    quantity_col = table.column("quantity")
    unit_price_col = table.column("unit_price")
    category_col = table.column("product_category")

    quantity_filled = pc.fill_null(quantity_col, pa.scalar(0, type=quantity_col.type))
    unit_price_filled = pc.fill_null(
        unit_price_col, pa.scalar(0.0, type=unit_price_col.type)
    )
    category_filled = pc.fill_null(
        category_col, pa.scalar("UNKNOWN", type=category_col.type)
    )

    table = table.set_column(table.schema.get_field_index("quantity"), "quantity", quantity_filled)
    table = table.set_column(
        table.schema.get_field_index("unit_price"), "unit_price", unit_price_filled
    )
    table = table.set_column(
        table.schema.get_field_index("product_category"), "product_category", category_filled
    )
    return table


def _add_order_total(table: pa.Table) -> pa.Table:
    """Append the derived ``order_total = quantity * unit_price`` column."""
    quantity_as_double = pc.cast(table.column("quantity"), pa.float64())
    order_total = pc.multiply(quantity_as_double, table.column("unit_price"))
    return table.append_column("order_total", order_total)


def _add_transform_timestamp(table: pa.Table, run_timestamp: datetime) -> pa.Table:
    """Append the ``transform_timestamp`` column (UTC, constant for this run)."""
    naive_utc = run_timestamp.astimezone(timezone.utc).replace(tzinfo=None)
    ts_array = pa.array([naive_utc] * table.num_rows, type=pa.timestamp("us"))
    return table.append_column("transform_timestamp", ts_array)


def transform(
    bronze_df_arrow: pa.Table,
    transform_date: date,
    stats: dict[str, int] | None = None,
) -> pa.Table:
    """Apply the Bronze -> Silver canonicalisation pipeline.

    Pure: does not perform any I/O and does not mutate ``bronze_df_arrow``.
    ``transform_date`` is accepted for interface symmetry with the Bronze
    contract (Silver's own partitioning is derived from each row's
    ``order_date``, not from this parameter). If a ``stats`` dict is
    supplied, intermediate row counts (``rows_unknown_status``,
    ``rows_after_dedup``, ``rows_dropped_dedup``, ``rows_after_null_drop``,
    ``rows_dropped_null``) are recorded into it for the caller's audit
    logging; this is an opt-in side channel only — omitting ``stats`` leaves
    the function's data flow fully pure and independently testable.
    """
    del transform_date  # unused: partitioning uses each row's own order_date

    table = _canonicalize_status_to_order_status(bronze_df_arrow, stats=stats)
    table = _truncate_order_date_to_date(table)

    rows_before_dedup = table.num_rows
    table = _deduplicate_latest_by_order_id(table)
    rows_after_dedup = table.num_rows
    if stats is not None:
        stats["rows_after_dedup"] = rows_after_dedup
        stats["rows_dropped_dedup"] = rows_before_dedup - rows_after_dedup

    rows_before_null_drop = table.num_rows
    table = _drop_unrecoverable_nulls(table)
    rows_after_null_drop = table.num_rows
    if stats is not None:
        stats["rows_after_null_drop"] = rows_after_null_drop
        stats["rows_dropped_null"] = rows_before_null_drop - rows_after_null_drop

    table = _fill_default_nulls(table)
    table = _add_order_total(table)
    table = _add_transform_timestamp(table, datetime.now(timezone.utc))

    table = table.select(COLUMN_ORDER)
    expected_arrow_schema = schema_to_pyarrow(TABLE_SCHEMA)
    return table.cast(expected_arrow_schema)


def run() -> None:
    """Orchestrate the Bronze -> Silver transform: read, transform, write, log."""
    start_time = datetime.now(timezone.utc)
    transform_date = start_time.date()
    logger.info("=== Shopify Silver transform START at %s ===", start_time.isoformat())
    logger.info(
        "Source Bronze table: %s | ingestion_date filter: %s",
        BRONZE_TABLE_IDENTIFIER,
        transform_date,
    )
    logger.info(
        "Iceberg REST catalog: %s | S3 endpoint: %s", ICEBERG_CATALOG_URI, S3_ENDPOINT
    )

    catalog = get_catalog()

    bronze_dataset = lineage.table_dataset(BRONZE_TABLE_IDENTIFIER)
    silver_dataset = lineage.table_dataset(TABLE_IDENTIFIER)
    run_id = lineage.new_run_id()
    lineage.emit_start(LINEAGE_JOB_NAME, run_id, inputs=[bronze_dataset], outputs=[silver_dataset])

    try:
        try:
            bronze_table = catalog.load_table(BRONZE_TABLE_IDENTIFIER)
        except NoSuchTableError as exc:
            raise RuntimeError(
                f"Bronze table '{BRONZE_TABLE_IDENTIFIER}' does not exist yet. "
                "Run ingestion/shopify_bronze.py first."
            ) from exc

        bronze_arrow = bronze_table.scan(
            row_filter=EqualTo("ingestion_date", transform_date.isoformat())
        ).to_arrow()
        rows_read = bronze_arrow.num_rows
        logger.info("Rows read from Bronze (ingestion_date=%s): %d", transform_date, rows_read)

        if rows_read == 0:
            logger.warning(
                "No Bronze rows found for ingestion_date=%s; nothing to transform. "
                "Run ingestion/shopify_bronze.py for today first.",
                transform_date,
            )
            lineage.emit_complete(LINEAGE_JOB_NAME, run_id, inputs=[bronze_dataset], outputs=[silver_dataset])
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            logger.info(
                "=== Shopify Silver transform END at %s (%.1fs) ===", end_time.isoformat(), duration
            )
            return

        stats: dict[str, int] = {}
        silver_arrow = transform(bronze_arrow, transform_date, stats=stats)
        rows_written = silver_arrow.num_rows

        logger.info(
            "Rows read from Bronze: %d -> after dedup: %d (dropped %d) -> "
            "after null drop: %d (dropped %d) -> written to Silver: %d",
            rows_read,
            stats["rows_after_dedup"],
            stats["rows_dropped_dedup"],
            stats["rows_after_null_drop"],
            stats["rows_dropped_null"],
            rows_written,
        )

        if stats["rows_unknown_status"] > 0:
            logger.warning(
                "%d row(s) had a status value outside STATUS_CANONICAL_MAP and "
                "were mapped to '%s'; investigate the Bronze source data.",
                stats["rows_unknown_status"],
                UNKNOWN_STATUS_LABEL,
            )

        silver_table = ensure_table(catalog)

        order_date_col = silver_arrow.column("order_date")
        min_order_date = pc.min(order_date_col).as_py()
        max_order_date = pc.max(order_date_col).as_py()

        # Idempotent, partition-level write: replace exactly the order_date range
        # covered by today's transformed Shopify batch, but ONLY for
        # channel='shopify' rows — this ensures we never overwrite Amazon rows
        # that share the same order_date partitions in silver.orders.
        overwrite_filter = And(
            And(
                GreaterThanOrEqual("order_date", min_order_date.isoformat()),
                LessThanOrEqual("order_date", max_order_date.isoformat()),
            ),
            EqualTo("channel", "shopify"),
        )
        for attempt in range(5):
            try:
                fresh_table = catalog.load_table(TABLE_IDENTIFIER)
                fresh_table.overwrite(df=silver_arrow, overwrite_filter=overwrite_filter)
                break
            except Exception as exc:
                if attempt == 4:
                    raise
                logger.warning("silver_table.overwrite attempt %d/5 failed: %s. Retrying in 2s...", attempt + 1, exc)
                time.sleep(2.0)

        written_back = silver_table.scan(row_filter=overwrite_filter).to_arrow()
        rows_confirmed = written_back.num_rows
        logger.info(
            "Rows written to %s (order_date between %s and %s): %d",
            TABLE_IDENTIFIER,
            min_order_date,
            max_order_date,
            rows_confirmed,
        )
    except Exception:
        lineage.emit_fail(LINEAGE_JOB_NAME, run_id, inputs=[bronze_dataset], outputs=[silver_dataset])
        raise

    lineage.emit_complete(LINEAGE_JOB_NAME, run_id, inputs=[bronze_dataset], outputs=[silver_dataset])

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info(
        "=== Shopify Silver transform END at %s (%.1fs) ===", end_time.isoformat(), duration
    )

    if rows_confirmed != rows_written:
        logger.warning(
            "Row count mismatch: wrote %d Silver rows but read-back holds %d",
            rows_written,
            rows_confirmed,
        )


if __name__ == "__main__":
    run()
