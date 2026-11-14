"""Gold mart: customer 360 profile & lifetime history (gold.customer_360).

Reads canonical orders from ``silver.orders`` and computes customer metrics:
    - total_orders (count of order_id)
    - total_spend (completed-order spend only)
    - first_order_date (min order_date)
    - last_order_date (max order_date)
    - favorite_category (product_category with highest total quantity purchased)

NOTE ON CROSS-CHANNEL CUSTOMER IDENTITIES:
    Synthetic customer_id pools are generated independently per source channel
    (e.g., Shopify customer_ids vs Amazon customer_ids do not share a common
    global identity pool). Therefore, Customer 360 metrics are aggregated per
    (customer_id, channel). We deliberately avoid fabricating artificial cross-channel
    customer identity matching without explicit identity resolution rules (e.g. email/phone).

Writes result to ``gold.customer_360`` as an Apache Iceberg table on MinIO.

Idempotent load: performs a full table overwrite on ``gold.customer_360``.

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
from pyiceberg.types import DateType, DoubleType, IntegerType, NestedField, StringType

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lineage import emitter as lineage  # noqa: E402
from pipeline.commerce_metrics import money_schema, amount, completed_orders, legacy_money  # noqa: E402

LINEAGE_JOB_NAME = "gold_marts.customer_360"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gold_customer_360")

# --- Configuration (env-overridable) ---
ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

SILVER_TABLE_IDENTIFIER = os.environ.get("SILVER_ORDERS_TABLE", "silver.orders")

NAMESPACE = "gold"
TABLE_NAME = "customer_360"
TABLE_IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"

TABLE_SCHEMA = Schema(
    NestedField(1, "customer_id", StringType(), required=False),
    NestedField(2, "channel", StringType(), required=False),
    NestedField(3, "total_orders", IntegerType(), required=False),
    NestedField(4, "total_spend", DoubleType(), required=False),
    NestedField(5, "first_order_date", DateType(), required=False),
    NestedField(6, "last_order_date", DateType(), required=False),
    NestedField(7, "favorite_category", StringType(), required=False),
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
    """Return the Gold customer_360 table, creating namespace/table if needed."""
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
    """Pure PyArrow transformation: aggregates silver.orders by customer_id and channel."""
    money_values = lambda values: pa.array([amount(v) for v in values], pa.decimal128(18, 2)) if decimal_output else legacy_money(values)
    silver_table = completed_orders(silver_table)
    if silver_table.num_rows == 0:
        expected_arrow_schema = money_schema(schema_to_pyarrow(TABLE_SCHEMA), ('total_spend',), decimal_output)
        return pa.Table.from_batches([], schema=expected_arrow_schema)

    # 1. Compute customer lifetime order counts, total spend, min date, max date
    cust_agg = silver_table.group_by(["customer_id", "channel"]).aggregate([
        ("order_id", "count"),
        ("order_total", "sum"),
        ("order_date", "min"),
        ("order_date", "max"),
    ])

    # 2. Determine favorite product_category (highest total quantity) per customer_id and channel
    cat_agg = silver_table.group_by(["customer_id", "channel", "product_category"]).aggregate([
        ("quantity", "sum")
    ])
    sort_idx = pc.sort_indices(
        cat_agg,
        sort_keys=[
            ("customer_id", "ascending"),
            ("channel", "ascending"),
            ("quantity_sum", "descending"),
            ("product_category", "ascending"),
        ],
    )
    sorted_cat = cat_agg.take(sort_idx)

    c_cust = sorted_cat.column("customer_id").combine_chunks()
    c_chan = sorted_cat.column("channel").combine_chunks()

    if len(c_cust) <= 1:
        fav_cat_table = sorted_cat
    else:
        prev_cust = c_cust.slice(0, len(c_cust) - 1)
        curr_cust = c_cust.slice(1, len(c_cust) - 1)
        prev_chan = c_chan.slice(0, len(c_chan) - 1)
        curr_chan = c_chan.slice(1, len(c_chan) - 1)
        is_diff = pc.or_(pc.invert(pc.equal(curr_cust, prev_cust)), pc.invert(pc.equal(curr_chan, prev_chan)))
        mask = pa.concat_arrays([pa.array([True], type=pa.bool_()), is_diff])
        fav_cat_table = sorted_cat.filter(mask)

    # Align cust_agg and fav_cat_table by sorting both on (customer_id, channel)
    sort_cust = pc.sort_indices(cust_agg, sort_keys=[("customer_id", "ascending"), ("channel", "ascending")])
    sorted_cust_agg = cust_agg.take(sort_cust)

    sort_fav = pc.sort_indices(fav_cat_table, sort_keys=[("customer_id", "ascending"), ("channel", "ascending")])
    sorted_fav_cat = fav_cat_table.take(sort_fav)

    total_orders = pc.cast(sorted_cust_agg.column("order_id_count"), pa.int32())
    total_spend = money_values(sorted_cust_agg.column("order_total_sum").to_pylist())

    res = pa.Table.from_arrays(
        [
            sorted_cust_agg.column("customer_id"),
            sorted_cust_agg.column("channel"),
            total_orders,
            total_spend,
            sorted_cust_agg.column("order_date_min"),
            sorted_cust_agg.column("order_date_max"),
            sorted_fav_cat.column("product_category"),
        ],
        names=[
            "customer_id",
            "channel",
            "total_orders",
            "total_spend",
            "first_order_date",
            "last_order_date",
            "favorite_category",
        ],
    )

    expected_arrow_schema = money_schema(schema_to_pyarrow(TABLE_SCHEMA), ('total_spend',), decimal_output)
    return res.cast(expected_arrow_schema)


def run() -> None:
    """Orchestrate Gold customer_360 transform: read Silver, aggregate, overwrite Gold."""
    start_time = datetime.now(timezone.utc)
    logger.info("=== Gold customer_360 transform START at %s ===", start_time.isoformat())

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
            "Successfully wrote %d customer 360 rows to %s", rows_written, TABLE_IDENTIFIER
        )
    except Exception:
        lineage.emit_fail(LINEAGE_JOB_NAME, run_id, inputs=[silver_dataset], outputs=[gold_dataset])
        raise

    lineage.emit_complete(LINEAGE_JOB_NAME, run_id, inputs=[silver_dataset], outputs=[gold_dataset])

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info("=== Gold customer_360 transform END at %s (%.1fs) ===", end_time.isoformat(), duration)


if __name__ == "__main__":
    run()
