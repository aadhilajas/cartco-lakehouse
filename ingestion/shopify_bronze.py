"""Bronze ingestion for CartCo's Shopify sales channel.

Lands the raw synthetic Shopify orders CSV into the Bronze layer as an Apache
Iceberg table (``bronze.shopify_orders``) on MinIO, via the Iceberg REST
catalog. Per the Bronze contract this performs no business logic, filtering, or
deduplication — it only converts format (CSV -> Parquet/Iceberg) and stamps the
load with an ``ingestion_date`` partition column.

Idempotency: the table is partitioned by ``ingestion_date`` and each run does a
partition-level overwrite of *today's* partition, so re-running the same day
replaces (never duplicates) that day's rows while leaving prior days intact.

Endpoints default to the in-cluster Docker service names. From the host these
same services are reachable at localhost:8181 (Iceberg REST) and
localhost:9000 (MinIO S3); override via env vars when running outside the
compose network.

Run inside the Airflow container (deps are pre-installed there):
    docker exec cartco-airflow-scheduler \\
        python /opt/airflow/ingestion/shopify_bronze.py
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv  # noqa: F401  (kept for parity; pandas is used for parsing)
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("shopify_bronze")

# --- Configuration (env-overridable; defaults target the compose network) ---
ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

DATA_DIR = os.environ.get("DATA_DIR", "data")
CSV_PATH = os.environ.get(
    "SHOPIFY_CSV_PATH", os.path.join(DATA_DIR, "raw", "shopify_orders.csv")
)

NAMESPACE = "bronze"
TABLE_NAME = "shopify_orders"
TABLE_IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"

# Explicit Iceberg schema: CSV columns + the added ingestion_date partition column.
TABLE_SCHEMA = Schema(
    NestedField(1, "order_id", StringType(), required=False),
    NestedField(2, "customer_id", StringType(), required=False),
    NestedField(3, "channel", StringType(), required=False),
    NestedField(4, "order_date", TimestampType(), required=False),
    NestedField(5, "product_id", StringType(), required=False),
    NestedField(6, "product_category", StringType(), required=False),
    NestedField(7, "quantity", IntegerType(), required=False),
    NestedField(8, "unit_price", DoubleType(), required=False),
    NestedField(9, "status", StringType(), required=False),
    NestedField(10, "ingestion_date", DateType(), required=False),
)

# Partition by the load date (identity transform on ingestion_date).
PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=10, field_id=1000, transform=IdentityTransform(), name="ingestion_date"
    )
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
    "status",
    "ingestion_date",
]


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
    """Return the Bronze table, creating the namespace/table if needed."""
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
        logger.info(
            "Created table '%s' partitioned by ingestion_date", TABLE_IDENTIFIER
        )
    return table


def read_source_csv(csv_path: str, ingestion_day: date) -> tuple[pd.DataFrame, pa.Table]:
    """Read the raw CSV and convert to an Arrow table matching the Iceberg schema."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Source CSV not found at '{csv_path}'. Run "
            "generate_synthetic_shopify_data.py first."
        )

    df = pd.read_csv(csv_path, parse_dates=["order_date"])
    # Bronze rule: no filtering/dedup — only add the load-time partition column.
    df["ingestion_date"] = pd.Timestamp(ingestion_day)
    df = df[COLUMN_ORDER]

    expected_arrow_schema = schema_to_pyarrow(TABLE_SCHEMA)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False).cast(
        expected_arrow_schema
    )
    return df, arrow_table


def run() -> None:
    start_time = datetime.now(timezone.utc)
    ingestion_day = start_time.date()
    logger.info("=== Shopify Bronze ingestion START at %s ===", start_time.isoformat())
    logger.info("Source CSV: %s", CSV_PATH)
    logger.info("Iceberg REST catalog: %s | S3 endpoint: %s", ICEBERG_CATALOG_URI, S3_ENDPOINT)

    df, arrow_table = read_source_csv(CSV_PATH, ingestion_day)
    rows_read = arrow_table.num_rows
    logger.info("Rows read from source: %d", rows_read)

    catalog = get_catalog()
    table = ensure_table(catalog)

    # Idempotent, partition-level write: replace only today's ingestion_date
    # partition. On first load the filter matches nothing and this is an append.
    table.overwrite(
        df=arrow_table,
        overwrite_filter=EqualTo("ingestion_date", ingestion_day.isoformat()),
    )

    # Confirm what landed in today's partition by reading it back.
    written = table.scan(
        row_filter=EqualTo("ingestion_date", ingestion_day.isoformat())
    ).to_arrow()
    rows_written = written.num_rows
    logger.info("Rows written to %s (ingestion_date=%s): %d", TABLE_IDENTIFIER, ingestion_day, rows_written)

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info("=== Shopify Bronze ingestion END at %s (%.1fs) ===", end_time.isoformat(), duration)

    if rows_written != rows_read:
        logger.warning(
            "Row count mismatch: read %d but today's partition holds %d",
            rows_read,
            rows_written,
        )


if __name__ == "__main__":
    run()
