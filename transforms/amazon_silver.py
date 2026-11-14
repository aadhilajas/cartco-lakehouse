"""Silver transform for CartCo's Amazon sales channel.

Builds on the Bronze contract established in ``ingestion/amazon_bronze.py``:
Bronze lands raw, unmodified Amazon orders into ``bronze.amazon_orders``
partitioned by ``ingestion_date`` (load date). This module reads the selected batch
Bronze partition and conforms it to the canonical Silver entity model,
writing the result to ``silver.orders`` as an Apache Iceberg table on MinIO,
via the same Iceberg REST catalog.

IMPORTANT — shared ``silver.orders`` table:
    Both the Shopify Silver transform (``transforms/silver_shopify_orders.py``)
    and this Amazon Silver transform write to the SAME Iceberg table:
    ``silver.orders``. The two channels are distinguished by the ``channel``
    column (``'shopify'`` vs ``'amazon'``). Each transform writes only its own
    channel's rows so they never overwrite each other's data. This gives
    Gold/reporting a single canonical source of all orders across channels.

Amazon-specific Silver transformations applied, in order:
    1. Schema canonicalisation: ``status`` -> ``order_status``, mapping
       Bronze's Amazon-native values to the canonical vocabulary shared
       across all Silver channels:
         - ``shipped``          -> ``completed``
         - ``cancelled``        -> ``cancelled``
         - ``refund_initiated`` -> ``refunded``
         - ``pending``          -> ``pending``
       (see ``STATUS_CANONICAL_MAP``). This is Silver's job, not Bronze's.
    2. Column rename: ``asin`` -> ``product_id``, aligning Amazon's ASIN
       field with Shopify's ``product_id`` in the canonical Silver schema.
    3. Deduplication: one row per ``order_id``, keeping the latest original
       ``order_date`` timestamp before truncation.
    4. Type enforcement: ``order_date`` is truncated from timestamp to date.
    5. Null handling: rows with a null ``order_id`` or ``customer_id`` are
       dropped as unrecoverable; ``quantity``, ``unit_price``, and
       ``product_category`` nulls are filled with defaults.
    6. Derived column: ``order_total = quantity * unit_price``.
    7. Added column: ``transform_timestamp`` (UTC, the moment this Silver
       record was written).

Idempotency: the write is partitioned by ``order_date`` AND filtered by
``channel='amazon'``, so Amazon rows can be re-run without touching
Shopify rows in the same ``order_date`` partition.
Before writing, a safety gate rejects missing existing keys or changed business
values. Source-versioned updates/deletes are not supported yet; exact business
replays and new orders with safe coverage are supported.

Lineage: ``run()`` emits START/COMPLETE/FAIL OpenLineage events via
``lineage/emitter.py`` (job ``amazon_bronze_ingestion.transform_to_silver``,
input dataset ``bronze.amazon_orders``, output dataset ``silver.orders``).
No-op if ``OPENLINEAGE_URL`` is unset or Marquez is unreachable.

No pandas in the transform path: all transformations use pure PyArrow
(pyarrow.compute). Pandas is not imported by this module.

Run inside the Airflow container (deps are pre-installed there):
    docker exec cartco-airflow-scheduler \\
        python /opt/airflow/transforms/amazon_silver.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
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
# sys.path for every process that runs this script directly.
REPO_ROOT = str(Path(__file__).resolve().parents[1])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lineage import emitter as lineage  # noqa: E402
from pipeline.batch import resolve_batch_date  # noqa: E402
from pipeline.silver_safety import UnsafeSnapshotError, validate_snapshot_replacement  # noqa: E402

LINEAGE_JOB_NAME = "amazon_bronze_ingestion.transform_to_silver"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("amazon_silver")

# --- Configuration (env-overridable; defaults target the compose network) ---
ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

BRONZE_TABLE_IDENTIFIER = os.environ.get("AMAZON_BRONZE_TABLE", "bronze.amazon_orders")

# Both channels write to the same canonical Silver table so Gold/reporting
# has a single cross-channel source of truth.
NAMESPACE = "silver"
TABLE_NAME = "orders"
TABLE_IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"

# Canonical Silver schema — field ids and column order are shared with the
# Shopify Silver transform. Both channels write identical schema to this table.
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

# Bronze -> Silver canonical status mapping for the Amazon source vocabulary.
# Bronze (ingestion/amazon_bronze.py) is source-faithful and only ever emits
# Amazon's own status values (shipped/cancelled/refund_initiated/pending).
# Silver owns translating that to the canonical vocabulary shared across all
# Silver channels (completed/cancelled/refunded/pending).
STATUS_CANONICAL_MAP = {
    "shipped": "completed",
    "cancelled": "cancelled",
    "refund_initiated": "refunded",
    "pending": "pending",
}

# Fallback for any status value outside STATUS_CANONICAL_MAP.
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
    """Return the shared Silver table, creating the namespace/table if needed."""
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
    """Map Bronze's Amazon-native ``status`` to the canonical ``order_status``.

    Applies ``STATUS_CANONICAL_MAP`` (e.g. ``shipped`` -> ``completed``)
    and renames the column from ``status`` to ``order_status`` in the same
    step. Values outside the map are set to ``UNKNOWN_STATUS_LABEL`` rather
    than raising. If a ``stats`` dict is supplied, the count of unmapped rows
    is recorded as ``rows_unknown_status`` for the caller to log/monitor.
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


def _rename_asin_to_product_id(table: pa.Table) -> pa.Table:
    """Rename the Amazon-specific ``asin`` column to the canonical ``product_id``.

    This aligns Amazon's ASIN field with Shopify's ``product_id`` in the
    shared ``silver.orders`` schema, allowing Gold/reporting to join or
    aggregate across channels on a single ``product_id`` column.
    """
    idx = table.schema.get_field_index("asin")
    asin_col = table.column("asin")
    return table.set_column(idx, "product_id", asin_col)


def _truncate_order_date_to_date(table: pa.Table) -> pa.Table:
    """Cast ``order_date`` from timestamp to date, dropping the time component."""
    truncated = pc.cast(table.column("order_date"), pa.date32())
    idx = table.schema.get_field_index("order_date")
    return table.set_column(idx, "order_date", truncated)


def _deduplicate_latest_by_order_id(table: pa.Table) -> pa.Table:
    """Keep exactly one row per ``order_id``, preferring the latest ``order_date``."""
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
    """Apply the Amazon Bronze -> Silver canonicalisation pipeline.

    Pure: does not perform any I/O and does not mutate ``bronze_df_arrow``.
    ``transform_date`` is accepted for interface symmetry with the Shopify
    Silver transform (Silver's own partitioning is derived from each row's
    ``order_date``, not from this parameter). If a ``stats`` dict is
    supplied, intermediate row counts are recorded into it for the caller's
    audit logging.
    """
    del transform_date  # unused: partitioning uses each row's own order_date

    table = _canonicalize_status_to_order_status(bronze_df_arrow, stats=stats)
    table = _rename_asin_to_product_id(table)
    rows_before_dedup = table.num_rows
    table = _deduplicate_latest_by_order_id(table)
    rows_after_dedup = table.num_rows
    table = _truncate_order_date_to_date(table)
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


def run(batch_date=None) -> None:
    """Orchestrate the Amazon Bronze -> Silver transform: read, transform, write, log."""
    start_time = datetime.now(timezone.utc)
    transform_date = resolve_batch_date(batch_date)
    logger.info("=== Amazon Silver transform START at %s ===", start_time.isoformat())
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
                "Run ingestion/amazon_bronze.py first."
            ) from exc

        bronze_arrow = bronze_table.scan(
            row_filter=EqualTo("ingestion_date", transform_date.isoformat())
        ).to_arrow()
        rows_read = bronze_arrow.num_rows
        logger.info("Rows read from Bronze (ingestion_date=%s): %d", transform_date, rows_read)

        if rows_read == 0:
            raise RuntimeError(
                f"No Bronze rows found for ingestion_date={transform_date}; "
                "the requested batch must be ingested and validated before Silver."
            )

        stats: dict[str, int] = {}
        silver_arrow = transform(bronze_arrow, transform_date, stats=stats)
        rows_written = silver_arrow.num_rows
        if rows_written == 0:
            raise RuntimeError("No valid Silver rows remain; refusing an empty overwrite")
        if set(silver_arrow.column("channel").to_pylist()) != {"amazon"}:
            raise ValueError("Silver input contains a missing or unexpected source channel")
        if silver_arrow.column("order_date").null_count:
            raise ValueError("Silver order_date cannot be null when writing partitions")

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
        # covered by the selected transformed Amazon batch, but ONLY for channel='amazon'
        # rows — this ensures we never overwrite Shopify rows in the same partition.
        overwrite_filter = And(
            And(
                GreaterThanOrEqual("order_date", min_order_date.isoformat()),
                LessThanOrEqual("order_date", max_order_date.isoformat()),
            ),
            EqualTo("channel", "amazon"),
        )
        for attempt in range(5):
            try:
                fresh_table = catalog.load_table(TABLE_IDENTIFIER)
                existing_channel = fresh_table.scan(row_filter=EqualTo("channel", "amazon")).to_arrow()
                validate_snapshot_replacement(existing_channel, silver_arrow, "amazon")
                fresh_table.overwrite(df=silver_arrow, overwrite_filter=overwrite_filter)
                break
            except UnsafeSnapshotError:
                raise
            except Exception as exc:
                if attempt == 4:
                    raise
                logger.warning("silver_table.overwrite attempt %d/5 failed: %s. Retrying in 2s...", attempt + 1, exc)
                time.sleep(2.0)

        silver_table = catalog.load_table(TABLE_IDENTIFIER)
        written_back = silver_table.scan(row_filter=overwrite_filter).to_arrow()
        rows_confirmed = written_back.num_rows
        if rows_confirmed != rows_written:
            raise RuntimeError(f"Silver verification failed: expected {rows_written} rows, found {rows_confirmed}")
        logger.info(
            "Rows written to %s (channel=amazon, order_date between %s and %s): %d",
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
        "=== Amazon Silver transform END at %s (%.1fs) ===", end_time.isoformat(), duration
    )


if __name__ == "__main__":
    run()
