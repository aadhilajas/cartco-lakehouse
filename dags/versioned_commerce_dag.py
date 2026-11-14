"""Manual three-source versioned ingestion and one coordinated local Gold release."""
from datetime import timedelta
from pathlib import Path
import sys

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline import runtime

BATCH_DATE = "{{ dag_run.conf.get('batch_date', ds) }}"
RUN_ID = "{{ run_id }}"

with DAG(
    dag_id="versioned_commerce_release",
    description="Frozen deliveries -> sequential versioned source merges -> optional inventory -> coordinated Gold",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=90),
    default_args={"owner": "cartco", "retries": 2, "retry_delay": timedelta(minutes=1), "execution_timeout": timedelta(minutes=30),
                  "on_failure_callback": runtime.task_failure_callback, "on_retry_callback": runtime.task_retry_callback},
    tags=["versioned", "three-source", "controlled-release"],
) as dag:
    common = {"batch_date": BATCH_DATE, "run_id": RUN_ID}
    prepare = PythonOperator(task_id="prepare_run", python_callable=runtime.prepare_task, op_kwargs=common)
    shopify = PythonOperator(task_id="ingest_shopify", python_callable=runtime.ingest_task, op_kwargs={**common, "source": "shopify"})
    amazon = PythonOperator(task_id="ingest_amazon", python_callable=runtime.ingest_task, op_kwargs={**common, "source": "amazon"})
    pos = PythonOperator(task_id="ingest_pos", python_callable=runtime.ingest_task, op_kwargs={**common, "source": "pos"})
    inventory = PythonOperator(task_id="prepare_inventory", python_callable=runtime.inventory_task, op_kwargs=common)
    publish = PythonOperator(task_id="publish_gold", python_callable=runtime.publish_task, op_kwargs=common)
    prepare >> shopify >> amazon >> pos >> inventory >> publish
