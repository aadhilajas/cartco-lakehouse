"""Spark catalog integration and immutable, snapshot-pinned release candidates."""
import csv
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F
from pipeline.spark_commerce import STATE_SCHEMA, normalize, merge_state, gold_marts, inventory_turnover


def session():
    builder = SparkSession.builder.master("local[2]").appName("cartco-assessed-commerce").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC").config("spark.sql.ansi.enabled", "true").config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    if os.environ.get("CARTCO_SPARK_JARS"):
        builder = builder.config("spark.jars", os.environ["CARTCO_SPARK_JARS"])
    builder = builder.config("spark.driver.extraJavaOptions", "-XX:ActiveProcessorCount=2")
    if os.environ.get("CARTCO_SPARK_CLASSPATH"):
        # Local Windows verification: classpath avoids Hadoop's jar-copy chmod,
        # which requires an external winutils.exe. Container jars are preinstalled.
        builder = builder.config("spark.driver.extraClassPath", os.environ["CARTCO_SPARK_CLASSPATH"]).config("spark.executor.extraClassPath", os.environ["CARTCO_SPARK_CLASSPATH"])
    options = {"": "org.apache.iceberg.spark.SparkCatalog", ".catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        ".uri": os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2"), ".ref": "main", ".authentication.type": "NONE",
        ".warehouse": os.environ.get("ICEBERG_WAREHOUSE", "s3://cartco-phase1/assessed"), ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        ".s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"), ".s3.path-style-access": "true",
        ".s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"], ".s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"], ".client.region": "us-east-1"}
    for key, value in options.items():
        builder = builder.config("spark.sql.catalog.nessie" + key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def csv_frame(spark, path):
    # CSV decoding belongs to ingestion, not to business computation. Every row
    # then passes through Spark casts/windows/aggregates. No reference transform
    # or precomputed mart is imported into this compute environment.
    with Path(path).open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        columns, rows = reader.fieldnames, list(reader)
    if not columns or any(not name or not re.fullmatch(r"[a-z_][a-z0-9_]*", name) for name in columns):
        raise ValueError("Invalid delivery CSV header")
    if len(rows) > 10000:
        raise ValueError("Bounded demo input exceeds 10000 records")
    return spark.createDataFrame([tuple(row[name] for name in columns) for row in rows], ",".join(name + " string" for name in columns))


def table_identity(spark, table):
    iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    snapshot = iceberg.currentSnapshot()
    return {"table": table, "uuid": str(iceberg.uuid()), "snapshot_id": snapshot.snapshotId() if snapshot else None,
        "metadata_location": str(iceberg.operations().current().metadataFileLocation()),
        "schema": spark.table(table).schema.jsonValue(), "partition_spec": str(iceberg.spec()),
        "description": str(iceberg.properties().get("comment")), "owner": str(iceberg.properties().get("cartco.owner"))}


def write_table(spark, frame, table, *, description, partition=None):
    if not re.fullmatch(r"nessie\.cartco_assessed(?:_[a-z0-9]+)?\.[a-z][a-z0-9_]*", table):
        raise ValueError("Refusing non-assessed table namespace")
    documented = frame.select(*[F.col(name).alias(name, metadata={"comment": name.replace("_", " ") + "; synthetic CartCo contract"}) for name in frame.columns])
    if spark.catalog.tableExists(table):
        documented.writeTo(table).overwrite(F.lit(True))
    else:
        writer = documented.writeTo(table).using("iceberg").tableProperty("format-version", "2").tableProperty("cartco.owner", "cartco-data-platform").tableProperty("comment", description)
        if partition:
            writer = writer.partitionedBy(F.col(partition))
        writer.create()
    identity = table_identity(spark, table)
    if identity["owner"] != "cartco-data-platform":
        raise ValueError("Catalog owner metadata not retained")
    return identity


def run(spark, manifest_path, *, namespace="cartco_assessed", candidate_path):
    started = time.monotonic()
    if not re.fullmatch(r"cartco_assessed(?:_[a-z0-9]+)?", namespace):
        raise ValueError("Unsafe assessed namespace")
    manifest_path = Path(manifest_path).resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if set(manifest["deliveries"]) != {"shopify", "amazon", "pos", "sftp"} or manifest["synthetic"] is not True:
        raise ValueError("All four explicitly synthetic deliveries are required")
    base = "nessie." + namespace
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {base}")
    day = manifest["batch_date"]
    date.fromisoformat(day)
    applied = datetime.now(timezone.utc).isoformat()
    incoming = None
    ingestion_day = applied[:10]
    inputs = {}
    bronze_frames = []
    for source in ["shopify", "amazon", "pos"]:
        path = Path(manifest["deliveries"][source])
        if not path.is_file():
            path = manifest_path.parent / source / "deliveries" / path.parent.name / path.name
        complete = json.loads(path.with_name("COMPLETE.json").read_text())
        if hashlib.sha256(path.read_bytes()).hexdigest() != complete["csv_sha256"]:
            raise ValueError("Delivery changed after acceptance")
        raw = csv_frame(spark, path)
        frame = normalize(raw, source=source, contract=complete["contract"], applied_at=applied)
        incoming = frame if incoming is None else incoming.unionByName(frame)
        # Queryable source-shaped records plus raw wire objects below. The new
        # table is date-partitioned; original batch-partitioned tables stay intact.
        bronze_frames.append(raw.select(F.lit(source).alias("source"), F.lit(complete["delivery_id"]).alias("batch_id"), F.lit(ingestion_day).cast("date").alias("ingestion_date"), F.to_json(F.struct(*raw.columns)).alias("raw_payload")))
        inputs[source] = {"delivery_id": complete["delivery_id"], "contract": complete["contract"]}
    bronze = bronze_frames[0].unionByName(bronze_frames[1]).unionByName(bronze_frames[2])
    bronze_table = base + ".bronze_orders"
    if spark.catalog.tableExists(bronze_table):
        # An accepted batch is immutable: append only unseen source/batch identities.
        keys = spark.table(bronze_table).select("source", "batch_id").distinct()
        unseen = bronze.join(keys, ["source", "batch_id"], "left_anti")
        if unseen.limit(1).count():
            unseen.writeTo(bronze_table).append()
        bronze_id = table_identity(spark, bronze_table)
    else:
        bronze_id = write_table(spark, bronze, bronze_table, description="Immutable source-shaped order records; UTC ingestion date and immutable batch identity", partition="ingestion_date")
    # Original response/message bytes are queryable too; retain binary without
    # JSON reserialization, including failed HTTP attempts and Kafka offsets.
    objects = []
    for source in ["shopify", "amazon", "pos", "sftp"]:
        folder = manifest_path.parent / source
        for path in sorted(folder.glob("**/objects/*")):
            if path.is_file():
                content = path.read_bytes()
                if hashlib.sha256(content).hexdigest() != path.name:
                    raise ValueError("Raw protocol object checksum differs")
                objects.append((source, ingestion_day, path.name, content))
    wire = spark.createDataFrame(objects, "source string, ingestion_date string, sha256 string, payload binary").withColumn("ingestion_date", F.to_date("ingestion_date"))
    wire_table = base + ".bronze_wire_objects"
    if spark.catalog.tableExists(wire_table):
        unseen = wire.join(spark.table(wire_table).select("source", "sha256"), ["source", "sha256"], "left_anti")
        if unseen.limit(1).count():
            unseen.writeTo(wire_table).append()
        wire_id = table_identity(spark, wire_table)
    else:
        wire_id = write_table(spark, wire, wire_table, description="Original HTTP response, Kafka message and SFTP bytes; SHA-256 addressed", partition="ingestion_date")
    state_table = base + ".silver_order_state"
    fence = Path(candidate_path).parent / "state-identities" / (namespace + ".json")
    if fence.exists():
        expected = json.loads(fence.read_text())
        if not spark.catalog.tableExists(state_table) or table_identity(spark, state_table)["uuid"] != expected["uuid"]:
            raise ValueError("Assessed state identity disappeared/changed; restore coordinated state instead of empty initialization")
    existing = spark.table(state_table) if spark.catalog.tableExists(state_table) else spark.createDataFrame([], STATE_SCHEMA)
    state = merge_state(existing, incoming).cache()
    # Materialize before overwriting a table that occurs in the input plan.
    state.count()
    unchanged = spark.catalog.tableExists(state_table) and not state.exceptAll(existing).limit(1).count() and not existing.exceptAll(state).limit(1).count()
    silver_id = table_identity(spark, state_table) if unchanged else write_table(spark, state, state_table, description="Latest source-versioned orders with retained tombstones, exact INR and channel-qualified identities", partition="channel")
    fence.parent.mkdir(parents=True, exist_ok=True)
    if not fence.exists():
        with fence.open("x", encoding="utf-8") as stream:
            json.dump({"table": state_table, "uuid": silver_id["uuid"]}, stream)
    inventory_path = Path(manifest["deliveries"]["sftp"])
    if not inventory_path.is_file():
        inventory_path = manifest_path.parent / "sftp/deliveries" / inventory_path.parent.name / inventory_path.name
    inventory_marker = json.loads(inventory_path.with_name("COMPLETE.json").read_text())
    if hashlib.sha256(inventory_path.read_bytes()).hexdigest() != inventory_marker["sha256"]:
        raise ValueError("Accepted inventory delivery changed")
    inventory = csv_frame(spark, inventory_path)
    inventory_table = base + ".bronze_inventory"
    inventory_batch = hashlib.sha256(inventory_path.with_name("COMPLETE.json").read_bytes()).hexdigest()
    landed_inventory = inventory.withColumn("ingestion_date", F.lit(ingestion_day).cast("date")).withColumn("batch_id", F.lit(inventory_batch))
    if spark.catalog.tableExists(inventory_table):
        if not spark.table(inventory_table).where(F.col("batch_id") == inventory_batch).limit(1).count():
            landed_inventory.writeTo(inventory_table).append()
        inventory_id = table_identity(spark, inventory_table)
    else:
        inventory_id = write_table(spark, landed_inventory, inventory_table, description="Immutable SFTP daily source-qualified SKU/location stock and fixed standard cost", partition="ingestion_date")
    marts = gold_marts(state)
    from datetime import timedelta
    start = str(date.fromisoformat(day) - timedelta(days=1))
    end = str(date.fromisoformat(day) + timedelta(days=1))
    marts["inventory_turnover"] = inventory_turnover(state, inventory, period_start=start, period_end=end)
    release = "r" + uuid4().hex
    tables = {}
    for name, frame in marts.items():
        tables[name] = write_table(spark, frame, base + "." + name + "_" + release, description=f"Immutable candidate {name}; release {release}; synthetic INR")
    # Small bounded export for independent GE/reference reconciliation. It is a
    # candidate only: CURRENT must be advanced by the separate validation gate.
    candidate = {"schema_version": 1, "release_id": release, "is_synthetic": True, "batch_date": day, "computed_at": applied,
        "compute_engine": {"spark": spark.version, "master": spark.sparkContext.master}, "inputs": inputs,
        "catalog": {"bronze_orders": bronze_id, "bronze_wire_objects": wire_id, "bronze_inventory": inventory_id, "silver": silver_id, "gold": tables},
        "silver_rows": [r.asDict() for r in state.collect()], "inventory_rows": [r.asDict() for r in inventory.collect()],
        "inventory_period": {"period_start": start, "period_end": end},
        "marts": {name: [r.asDict() for r in spark.table(identity["table"]).collect()] for name, identity in tables.items()},
        "explain": marts["daily_revenue"]._jdf.queryExecution().toString()}
    candidate["execution"] = {"mode": "Airflow task" if os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") else "Local Spark batch",
        "duration_seconds": round(time.monotonic() - started, 2)}
    from pipeline.source_identity import source_identity
    candidate["source_bundle_sha256"] = source_identity()
    target = Path(candidate_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(candidate, indent=2, default=str) + "\n", encoding="utf-8")
    state.unpersist()
    return target
