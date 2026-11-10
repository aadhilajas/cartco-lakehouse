"""Gold mart: lifetime channel performance metrics (gold.channel_performance).

Reads canonical orders from ``silver.orders`` and computes lifetime channel metrics:
    - total_revenue (completed-order sales only; exact-cent sums)
    - total_orders (count of order_id)
    - cancellation_rate (cancelled order count / total_orders)
    - refund_rate (refunded order count / total_orders)
    - avg_fulfillment_days (null — fulfillment lead time is not captured in current Silver schema)

One row per channel (e.g. shopify, amazon).

Writes result to ``gold.channel_performance`` as an Apache Iceberg table on MinIO.

Idempotent load: performs a full table overwrite on ``gold.channel_performance``.

Lineage: emits START/COMPLETE/FAIL OpenLineage events via ``lineage/emitter.py``.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, IntegerType, NestedField, StringType

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lineage import emitter as lineage  # noqa: E402
from pipeline.commerce_metrics import money_schema, amount, validated_orders, legacy_money  # noqa: E402

LINEAGE_JOB_NAME = "gold_marts.channel_performance"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gold_channel_performance")

# --- Configuration (env-overridable) ---
ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

SILVER_TABLE_IDENTIFIER = os.environ.get("SILVER_ORDERS_TABLE", "silver.orders")

NAMESPACE = "gold"
TABLE_NAME = "channel_performance"
TABLE_IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"

TABLE_SCHEMA = Schema(
    NestedField(1, "channel", StringType(), required=False),
    NestedField(2, "total_revenue", DoubleType(), required=False),
    NestedField(3, "total_orders", IntegerType(), required=False),
    NestedField(4, "cancellation_rate", DoubleType(), required=False),
    NestedField(5, "refund_rate", DoubleType(), required=False),
    NestedField(6, "avg_fulfillment_days", DoubleType(), required=False),
)


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


def _retry(func, retries=5, delay=2.0):
    for attempt in range(retries):
        try:
            return func()
        except Exception as exc:
            if attempt == retries - 1:
                raise
            logger.warning("Catalog operation failed (attempt %d/%d): %s. Retrying in %.1fs...", attempt + 1, retries, exc, delay)
            time.sleep(delay)


def ensure_table(catalog):
    """Return the Gold channel_performance table, creating namespace/table if needed."""
    try:
        catalog.create_namespace(NAMESPACE)
        logger.info("Created namespace '%s'", NAMESPACE)
    except NamespaceAlreadyExistsError:
        logger.info("Namespace already exists")

    def _get_or_create():
        try:
            return catalog.load_table(TABLE_IDENTIFIER)
        except NoSuchTableError:
            return catalog.create_table(TABLE_IDENTIFIER, schema=TABLE_SCHEMA)

    return _retry(_get_or_create)


def transform(silver_table: pa.Table, *, decimal_output=False) -> pa.Table:
    """Pure PyArrow transformation: aggregates silver.orders by channel to compute lifetime performance metrics."""
    money_values = lambda values: pa.array([amount(v) for v in values], pa.decimal128(18, 2)) if decimal_output else legacy_money(values)
    silver_table = validated_orders(silver_table)
    if silver_table.num_rows == 0:
        expected_arrow_schema = money_schema(schema_to_pyarrow(TABLE_SCHEMA), ('total_revenue',), decimal_output)
        return pa.Table.from_batches([], schema=expected_arrow_schema)

    is_cancelled = pc.cast(pc.equal(silver_table.column("order_status"), "cancelled"), pa.int64())
    is_refunded = pc.cast(pc.equal(silver_table.column("order_status"), "refunded"), pa.int64())

    retained = pc.if_else(pc.equal(silver_table["order_status"], "completed"), silver_table["order_total"], pa.scalar(0, type=silver_table["order_total"].type))
    prep = silver_table.set_column(silver_table.schema.get_field_index("order_total"), "order_total", retained)
    prep = prep.append_column("is_cancelled", is_cancelled).append_column("is_refunded", is_refunded)

    agg = prep.group_by(["channel"]).aggregate([
        ("order_total", "sum"),
        ("order_id", "count"),
        ("is_cancelled", "sum"),
        ("is_refunded", "sum"),
    ])

    total_rev = money_values(agg.column("order_total_sum").to_pylist())
    total_ords = pc.cast(agg.column("order_id_count"), pa.int32())
    canc_rate = pc.divide(pc.cast(agg.column("is_cancelled_sum"), pa.float64()), pc.cast(total_ords, pa.float64()))
    ref_rate = pc.divide(pc.cast(agg.column("is_refunded_sum"), pa.float64()), pc.cast(total_ords, pa.float64()))
    avg_fulf = pa.nulls(agg.num_rows, type=pa.float64())

    res = pa.Table.from_arrays(
        [
            agg.column("channel"),
            total_rev,
            total_ords,
            canc_rate,
            ref_rate,
            avg_fulf,
        ],
        names=[
            "channel",
            "total_revenue",
            "total_orders",
            "cancellation_rate",
            "refund_rate",
            "avg_fulfillment_days",
        ],
    )

    sort_idx = pc.sort_indices(res, sort_keys=[("channel", "ascending")])
    res = res.take(sort_idx)

    expected_arrow_schema = money_schema(schema_to_pyarrow(TABLE_SCHEMA), ('total_revenue',), decimal_output)
    return res.cast(expected_arrow_schema)


def run() -> None:
    """Orchestrate Gold channel_performance transform: read Silver, aggregate, overwrite Gold."""
    start_time = datetime.now(timezone.utc)
    logger.info("=== Gold channel_performance transform START at %s ===", start_time.isoformat())

    catalog = get_catalog()

    silver_dataset = lineage.table_dataset(SILVER_TABLE_IDENTIFIER)
    gold_dataset = lineage.table_dataset(TABLE_IDENTIFIER)
    run_id = lineage.new_run_id()
    lineage.emit_start(LINEAGE_JOB_NAME, run_id, inputs=[silver_dataset], outputs=[gold_dataset])

    try:
        try:
            silver_table = catalog.load_table(SILVER_TABLE_IDENTIFIER)
        except NoSuchTableError as exc:
            raise RuntimeError(
                f"Silver table '{SILVER_TABLE_IDENTIFIER}' does not exist yet. "
                "Run Silver transforms first."
            ) from exc

        silver_arrow = silver_table.scan().to_arrow()
        rows_read = silver_arrow.num_rows
        logger.info("Rows read from Silver (%s): %d", SILVER_TABLE_IDENTIFIER, rows_read)

        gold_arrow = transform(silver_arrow)
        rows_written = gold_arrow.num_rows

        gold_table = ensure_table(catalog)
        _retry(lambda: gold_table.overwrite(df=gold_arrow))

        logger.info(
            "Successfully wrote %d channel performance rows to %s", rows_written, TABLE_IDENTIFIER
        )
    except Exception:
        lineage.emit_fail(LINEAGE_JOB_NAME, run_id, inputs=[silver_dataset], outputs=[gold_dataset])
        raise

    lineage.emit_complete(LINEAGE_JOB_NAME, run_id, inputs=[silver_dataset], outputs=[gold_dataset])

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info("=== Gold channel_performance transform END at %s (%.1fs) ===", end_time.isoformat(), duration)


if __name__ == "__main__":
    run()
