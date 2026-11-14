"""Actual Spark/Iceberg/Nessie/S3 compatibility and restart acceptance.

Uses a new uniquely named table per creation run. Never touches baseline tables.
The --verify pass is read-only and compares retained identity/data/snapshots.
"""

# Support direct CLI execution and imports from any working directory.
import sys as _sys
from pathlib import Path as _Path
_SCRIPT_PATH = _Path(__file__).resolve()
_REPOSITORY_ROOT = next(
    (parent for parent in _SCRIPT_PATH.parents
     if (parent / "pipeline/source_identity.py").is_file()
     and (parent / "scripts/layout.json").is_file()),
    _SCRIPT_PATH.parent,  # Individually copied stdlib tools do not need a repo.
)
if str(_REPOSITORY_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPOSITORY_ROOT))
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import platform
import time
from urllib.request import urlopen
from uuid import uuid4


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def wait_http(url, seconds=180):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=5) as response:
                if response.status == 200:
                    return
        except Exception as error:
            last = error
        time.sleep(2)
    raise RuntimeError(f"Readiness timeout: {url}") from last


def session():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("cartco-phase1-compatibility")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    )
    options = {
        "": "org.apache.iceberg.spark.SparkCatalog",
        ".catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        ".uri": os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2"),
        ".ref": "main",
        ".authentication.type": "NONE",
        ".warehouse": os.environ.get("ICEBERG_WAREHOUSE", "s3://cartco-phase1/warehouse"),
        ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        ".s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        ".s3.path-style-access": "true",
        ".s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        ".s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
        ".client.region": "us-east-1",
    }
    for key, value in options.items():
        builder = builder.config("spark.sql.catalog.nessie" + key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def checkpoint(spark, table):
    # Table names originate from our UUID or a validated prior evidence file.
    import re
    require(bool(re.fullmatch(r"nessie\.phase1_checks\.probe_[0-9a-f]{32}", table)), "Unsafe probe name")
    rows = [r.asDict() for r in spark.sql(f"SELECT * FROM {table} ORDER BY order_id").collect()]
    encoded = json.dumps(rows, sort_keys=True, default=str).encode()
    iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    snapshots = sorted(r.snapshot_id for r in spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").collect())
    return {
        "table": table,
        "uuid": str(iceberg.uuid()),
        "metadata_location": str(iceberg.operations().current().metadataFileLocation()),
        "current_snapshot": iceberg.currentSnapshot().snapshotId(),
        "retained_snapshots": snapshots,
        "schema": spark.table(table).schema.jsonValue(),
        "partition_spec": str(iceberg.spec()),
        "properties": {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()},
        "row_count": len(rows),
        "row_sha256": hashlib.sha256(encoded).hexdigest(),
        "revenue_inr": str(spark.sql(f"SELECT sum(net_amount) AS total FROM {table}").first().total),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, default=Path("/evidence/compatibility.json"))
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    wait_http(os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2") + "/config")
    wait_http(os.environ.get("S3_ENDPOINT", "http://minio:9000") + "/minio/health/ready")
    spark = session()
    try:
        require(spark.version == "3.5.6", "Unexpected Spark version")
        java = spark._jvm.java.lang.System.getProperty("java.version")
        require(java.startswith("17."), "Spark must use Java 17")
        require(platform.python_version().startswith("3.11."), "Spark Python must be 3.11")
        scala = spark._jvm.scala.util.Properties.versionNumberString()
        iceberg_version = spark._jvm.org.apache.iceberg.IcebergBuild.fullVersion()
        require(scala.startswith("2.12."), "Unexpected Scala binary version")
        require("1.12.0" in iceberg_version, "Unexpected Iceberg runtime")
        if args.verify:
            previous = json.loads(args.evidence.read_text(encoding="utf-8"))
            require(previous.get("acceptance_source_sha256") == source_sha256, "Acceptance source changed between creation and restart verification")
            actual = checkpoint(spark, previous["checkpoint"]["table"])
            require(actual == previous["checkpoint"], "Catalog identity/data changed after recreation")
            report = {"verified_at": datetime.now(timezone.utc).isoformat(), "checkpoint_match": True, "checkpoint": actual}
            target = args.evidence.with_name("restart-verification.json")
        else:
            # Real DataFrame execution with two input partitions and a shuffle,
            # in addition to the catalog SQL path below. No Python UDF is needed.
            frame = spark.range(0, 10000, numPartitions=2).selectExpr("id % 4 AS bucket")
            grouped = frame.groupBy("bucket").count().orderBy("bucket").collect()
            require([(r.bucket, r['count']) for r in grouped] == [(0, 2500), (1, 2500), (2, 2500), (3, 2500)], "DataFrame shuffle aggregation mismatch")
            table = "nessie.phase1_checks.probe_" + uuid4().hex
            spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.phase1_checks")
            spark.sql(f"""CREATE TABLE {table} (
                order_id BIGINT COMMENT 'Synthetic order key',
                channel STRING COMMENT 'Commerce channel',
                net_amount DECIMAL(18,2) COMMENT 'Net sales in INR',
                ingestion_date DATE COMMENT 'UTC landing date'
            ) USING iceberg PARTITIONED BY (ingestion_date)
            COMMENT 'Isolated CartCo Phase 1 compatibility probe'
            TBLPROPERTIES ('format-version'='2', 'cartco.owner'='cartco-data-platform')""")
            spark.sql(f"""INSERT INTO {table} VALUES
                (1, 'shopify', 1299.95, DATE '2026-11-11'),
                (2, 'amazon', 499.50, DATE '2026-11-11'),
                (3, 'pos', 250.00, DATE '2026-11-12'),
                (4, 'shopify', 75.25, DATE '2026-11-12')""")
            first = checkpoint(spark, table)
            require(Decimal(first["revenue_inr"]) == Decimal("2124.70"), "Initial decimal sum mismatch")
            spark.sql(f"""MERGE INTO {table} t USING
                (SELECT 1L AS order_id, CAST(999.95 AS DECIMAL(18,2)) AS net_amount) s
                ON t.order_id=s.order_id WHEN MATCHED THEN UPDATE SET t.net_amount=s.net_amount""")
            spark.sql(f"ALTER TABLE {table} ADD COLUMN note STRING COMMENT 'Additive evolution probe'")
            spark.sql(f"INSERT INTO {table} VALUES (5, 'pos', 100.00, DATE '2026-11-12', 'new column')")
            old = spark.sql(f"SELECT count(*) AS n, sum(net_amount) AS total FROM {table} VERSION AS OF {first['current_snapshot']}").first()
            require(old.n == 4 and old.total == Decimal("2124.70"), "Time travel mismatch")
            current = checkpoint(spark, table)
            require(current["row_count"] == 5 and Decimal(current["revenue_inr"]) == Decimal("1924.70"), "MERGE/append mismatch")
            require(len(current["retained_snapshots"]) >= 3, "Missing snapshots")
            require("ingestion_date" in current["partition_spec"], "Missing date partition")
            require(current["properties"].get("cartco.owner") == "cartco-data-platform", "Owner metadata missing")
            groups = [r.asDict() for r in spark.sql(f"SELECT channel, count(*) AS orders, sum(net_amount) AS revenue FROM {table} GROUP BY channel ORDER BY channel").collect()]
            report = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "acceptance_source_sha256": source_sha256,
                "runtime": {"spark": spark.version, "python": platform.python_version(), "java": java,
                            "scala": scala,
                            "iceberg": iceberg_version,
                            "architecture": platform.machine()},
                "checks": ["DataFrame_shuffle", "catalog_create", "S3_write_read", "decimal_sum", "MERGE", "add_column", "time_travel", "date_partition", "Spark_aggregate"],
                "initial_snapshot": first["current_snapshot"], "channel_aggregate": groups,
                "checkpoint": current,
            }
            target = args.evidence
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
        print(json.dumps(report, default=str), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
