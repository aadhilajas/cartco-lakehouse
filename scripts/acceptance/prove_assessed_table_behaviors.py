"""Disposable real Iceberg evolution/time travel/compaction and pruning proof."""

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
from datetime import date
import argparse
import hashlib
import json
from pathlib import Path
import sys
from uuid import uuid4
sys.path.insert(0, str(_REPOSITORY_ROOT))
from pipeline.spark_lakehouse import session, write_table
from pyspark.sql import functions as F


def digest(spark, table):
    rows = [row.asDict() for row in spark.table(table).orderBy("id").collect()]
    return hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    spark = session()
    try:
        namespace = "cartco_assessed_behaviors"
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie." + namespace)
        short = "probe_" + uuid4().hex
        table = "nessie." + namespace + "." + short
        frame = spark.range(120).withColumn("ingestion_date", F.when(F.col("id") < 60, F.lit("2026-11-11")).otherwise(F.lit("2026-11-12")).cast("date"))
        write_table(spark, frame.repartition(6), table, description="Disposable optimization/evolution proof; not commerce production data", partition="ingestion_date")
        initial = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at").first()[0]
        spark.sql(f"ALTER TABLE {table} ADD COLUMN note STRING COMMENT 'Safe additive evolution probe'")
        spark.sql(f"UPDATE {table} SET note = 'retained' WHERE id = 0")
        # Separate commits deliberately create small files. Repartition alone
        # does not guarantee fragmentation: Iceberg may coalesce the write.
        for index in range(3):
            spark.range(120 + index * 10, 130 + index * 10).withColumn("ingestion_date", F.lit("2026-11-12").cast("date")).withColumn("note", F.lit(None).cast("string")).writeTo(table).append()
        assert spark.sql(f"SELECT count(*) FROM {table} VERSION AS OF {initial}").first()[0] == 120
        assert spark.sql(f"SELECT note FROM {table} WHERE id=0").first()[0] == "retained"
        try:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN id TYPE STRING")
        except Exception:
            incompatible_refused = True
        else:
            raise AssertionError("Incompatible long-to-string evolution unexpectedly accepted")
        before = {"files": spark.sql(f"SELECT count(*) FROM {table}.files").first()[0], "rows_sha256": digest(spark, table)}
        compaction = [row.asDict() for row in spark.sql(f"CALL nessie.system.rewrite_data_files(table => '{namespace}.{short}', options => map('rewrite-all','true','min-input-files','2','target-file-size-bytes','134217728'))").collect()]
        after = {"files": spark.sql(f"SELECT count(*) FROM {table}.files").first()[0], "rows_sha256": digest(spark, table)}
        assert before["rows_sha256"] == after["rows_sha256"] and after["files"] < before["files"]
        filtered = spark.table(table).where("ingestion_date = DATE '2026-11-12'")
        assert filtered.count() == 90
        plan = filtered._jdf.queryExecution().toString()
        assert "ingestion_date" in plan and "BatchScan" in plan
        iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
        expression = spark._jvm.org.apache.iceberg.expressions.Expressions.equal("ingestion_date", "2026-11-12")
        planned = iceberg.newScan().filter(expression).planFiles()
        try:
            iterator = planned.iterator()
            selected_files = []
            while iterator.hasNext():
                selected_files.append(str(iterator.next().file().path()))
        finally:
            planned.close()
        assert 0 < len(selected_files) < after["files"]
        # Broadcast a bounded dimension and retain the real physical plan.
        joined = spark.table(table).join(F.broadcast(spark.range(5).withColumn("category", F.lit("fixture"))), "id")
        assert joined.count() == 5
        report = {"table": table, "initial_snapshot": initial, "time_travel_rows": 120, "additive_evolution": True,
            "incompatible_evolution_refused": incompatible_refused, "before": before, "after": after, "compaction": compaction,
            "pruned_rows": 90, "pruned_data_files": selected_files, "pruning_plan": plan, "broadcast_plan": joined._jdf.queryExecution().toString()}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print("Open-table behavior proof passed:", args.output)
    finally:
        spark.stop()


if __name__ == "__main__": main()
