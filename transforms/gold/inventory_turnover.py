"""Gold mart: inventory turnover and units sold by category and channel (gold.inventory_turnover).

Reads canonical orders from ``silver.orders`` and computes:
    - units_sold (sum of quantity)
    - turnover_period (string date range representation, e.g. 'YYYY-MM-DD to YYYY-MM-DD')

Grouped by product_category and channel over the available date range.

Writes result to ``gold.inventory_turnover`` as an Apache Iceberg table on MinIO.

Idempotent load: performs a full table overwrite on ``gold.inventory_turnover``.

Lineage: emits START/COMPLETE/FAIL OpenLineage events via ``lineage/emitter.py``.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

REPO_ROOT = "/opt/airflow"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lineage import emitter as lineage  # noqa: E402

LINEAGE_JOB_NAME = "gold_marts.inventory_turnover"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gold_inventory_turnover")

# --- Configuration (env-overridable) ---
ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

SILVER_TABLE_IDENTIFIER = os.environ.get("SILVER_ORDERS_TABLE", "silver.orders")

NAMESPACE = "gold"
TABLE_NAME = "inventory_turnover"
TABLE_IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"

TABLE_SCHEMA = Schema(
    NestedField(1, "product_category", StringType(), required=False),
    NestedField(2, "units_sold", IntegerType(), required=False),
    NestedField(3, "channel", StringType(), required=False),
    NestedField(4, "turnover_period", StringType(), required=False),
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
    """Return the Gold inventory_turnover table, creating namespace/table if needed."""
    try:
        catalog.create_namespace(NAMESPACE)
        logger.info("Created namespace '%s'", NAMESPACE)
    except Exception:
        pass

    def _get_or_create():
        try:
            return catalog.load_table(TABLE_IDENTIFIER)
        except NoSuchTableError:
            return catalog.create_table(TABLE_IDENTIFIER, schema=TABLE_SCHEMA)

    return _retry(_get_or_create)


def transform(silver_table: pa.Table) -> pa.Table:
    """Pure PyArrow transformation: aggregates silver.orders by product_category and channel."""
    if silver_table.num_rows == 0:
        expected_arrow_schema = schema_to_pyarrow(TABLE_SCHEMA)
        return pa.Table.from_batches([], schema=expected_arrow_schema)

    order_dates = silver_table.column("order_date")
    min_date = pc.min(order_dates).as_py()
    max_date = pc.max(order_dates).as_py()
    period_str = f"{min_date} to {max_date}" if min_date and max_date else "ALL_TIME"

    agg = silver_table.group_by(["product_category", "channel"]).aggregate([
        ("quantity", "sum"),
    ])

    units_sold = pc.cast(agg.column("quantity_sum"), pa.int32())
    periods = pa.array([period_str] * agg.num_rows, type=pa.string())

    res = pa.Table.from_arrays(
        [
            agg.column("product_category"),
            units_sold,
            agg.column("channel"),
            periods,
        ],
        names=["product_category", "units_sold", "channel", "turnover_period"],
    )

    sort_idx = pc.sort_indices(
        res,
        sort_keys=[
            ("product_category", "ascending"),
            ("channel", "ascending"),
        ],
    )
    res = res.take(sort_idx)

    expected_arrow_schema = schema_to_pyarrow(TABLE_SCHEMA)
    return res.cast(expected_arrow_schema)


def run() -> None:
    """Orchestrate Gold inventory_turnover transform: read Silver, aggregate, overwrite Gold."""
    start_time = datetime.now(timezone.utc)
    logger.info("=== Gold inventory_turnover transform START at %s ===", start_time.isoformat())

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
            "Successfully wrote %d inventory turnover rows to %s", rows_written, TABLE_IDENTIFIER
        )
    except Exception:
        lineage.emit_fail(LINEAGE_JOB_NAME, run_id, inputs=[silver_dataset], outputs=[gold_dataset])
        raise

    lineage.emit_complete(LINEAGE_JOB_NAME, run_id, inputs=[silver_dataset], outputs=[gold_dataset])

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info("=== Gold inventory_turnover transform END at %s (%.1fs) ===", end_time.isoformat(), duration)


if __name__ == "__main__":
    run()
