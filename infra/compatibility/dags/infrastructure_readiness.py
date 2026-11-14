"""A paused diagnostic DAG; assessed commerce DAGs are wired in later phases."""
from datetime import datetime, timezone
from urllib.request import urlopen
from airflow import DAG
from airflow.operators.python import PythonOperator


def check_catalog():
    with urlopen("http://nessie:19120/api/v2/config", timeout=15) as response:
        if response.status != 200:
            raise RuntimeError("Nessie API unavailable")


with DAG(
    "cartco_infrastructure_readiness",
    start_date=datetime(2026, 11, 11, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["phase1", "diagnostic"],
) as dag:
    PythonOperator(task_id="nessie_api", python_callable=check_catalog)
