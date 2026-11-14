"""Four meaningful assessed DAGs; the source fixtures are explicitly synthetic."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import os

sys.path.insert(0, str(Path(__file__).resolve().parents[1] if Path(__file__).resolve().parents[1].name == "airflow" else Path(__file__).resolve().parents[3]))
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from pipeline import assessed_orchestration as tasks

DEFAULTS = {"owner": "cartco-data-platform", "retries": 2, "retry_delay": timedelta(seconds=10), "execution_timeout": timedelta(minutes=22)}
OPTIONS = {"start_date": datetime(2026, 11, 12, tzinfo=timezone.utc), "schedule": None, "catchup": False,
           "max_active_runs": 1, "default_args": DEFAULTS, "tags": ["assessed", "synthetic", "commerce"]}

with DAG("assessed_orders_ingestion", description="Paginated GraphQL/SP-API and bounded Kafka POS landing", **OPTIONS) as orders:
    extracts = [PythonOperator(task_id="extract_" + source, python_callable=tasks.orders, op_kwargs={"source": source}) for source in ["shopify", "amazon", "pos"]]
    complete = PythonOperator(task_id="publish_order_delivery_manifest", python_callable=tasks.combine_orders)
    extracts >> complete

with DAG("assessed_inventory_ingestion", description="Verified SSH/SFTP completed inventory delivery", **OPTIONS) as inventory:
    PythonOperator(task_id="extract_completed_sftp_inventory", python_callable=tasks.inventory)

def sources_ready():
    return all((tasks.root() / path).is_file() for path in ["ready/orders.json", "ready/inventory.json"])

with DAG("assessed_commerce_release", description="Spark medallion, exact reconciliation/GE, Marquez and atomic dashboard export", **OPTIONS) as release:
    ready = PythonSensor(task_id="wait_for_durable_sources", python_callable=sources_ready, timeout=180, poke_interval=5, mode="reschedule")
    manifest = PythonOperator(task_id="validate_source_manifests", python_callable=tasks.prepare_manifest)
    compute = PythonOperator(task_id="spark_bronze_silver_gold", python_callable=tasks.compute)
    quality = PythonOperator(task_id="reconcile_ge_and_publish", python_callable=tasks.publish)
    lineage = PythonOperator(task_id="verify_marquez_column_lineage", python_callable=tasks.lineage)
    export = PythonOperator(task_id="export_validated_dashboard", python_callable=tasks.export)
    ready >> manifest >> compute >> quality >> lineage >> export

with DAG("assessed_system_health", description="Read-only dependency and published-release integrity audit", **OPTIONS) as health:
    PythonOperator(task_id="check_dependencies_and_release", python_callable=tasks.health)
