"""End-to-end CartCo Shopify pipeline: Bronze ingestion -> quality gate -> Silver.

Four tasks, in order:
    1. ``generate_data``      -- generate the synthetic Shopify orders CSV.
    2. ``ingest_to_bronze``   -- land it into ``bronze.shopify_orders`` (Iceberg
                                 on MinIO), partitioned by ``ingestion_date``.
    3. ``run_ge_checkpoint``  -- run the pre-Silver Great Expectations quality
                                 gate against ``bronze.shopify_orders``. Raises
                                 if any expectation fails, which fails this
                                 task and halts the DAG before Silver runs.
    4. ``transform_to_silver`` -- read Bronze, apply canonical schema +
                                 dedup + null handling, write
                                 ``silver.orders`` partitioned by
                                 ``order_date``.

Re-runnable: every task is idempotent (Bronze does a partition-level
overwrite on ``ingestion_date``; Silver does a partition-level overwrite on
the ``order_date`` range of the batch it just transformed). The GE gate in
task 3 halts the DAG if Bronze quality thresholds are not met, so dirty
Bronze data never reaches Silver.

Tasks 1-2 run the existing standalone scripts via ``BashOperator`` (unchanged
from the original two-task DAG). Tasks 3-4 call the ``run()`` functions in
``transforms/`` directly via ``PythonOperator``, since those modules are
pure-Python and already return/raise cleanly with no CLI wrapping needed.
"""

from __future__ import annotations

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# transforms/ is volume-mounted at /opt/airflow/transforms (docker-compose.yml),
# but /opt/airflow itself isn't guaranteed to be on sys.path for every Airflow
# process that imports this DAG file, so it's added explicitly here.
AIRFLOW_HOME = "/opt/airflow"
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

from transforms import ge_bronze_shopify_checkpoint, silver_shopify_orders  # noqa: E402

INGESTION_DIR = "/opt/airflow/ingestion"

default_args = {
    "owner": "cartco",
    "retries": 1,
}

with DAG(
    dag_id="shopify_bronze_ingestion",
    default_args=default_args,
    description=(
        "End-to-end Shopify pipeline: synthetic data generation -> Bronze "
        "Iceberg ingestion -> GE quality gate -> Silver canonical transform"
    ),
    doc_md="""
    ### Shopify end-to-end pipeline

    End-to-end Shopify pipeline: synthetic data generation -> Bronze Iceberg
    ingestion -> GE quality gate -> Silver canonical transform. Re-runnable:
    each task is idempotent. GE gate halts the DAG if Bronze quality
    thresholds are not met.

    **Chain:** `generate_data >> ingest_to_bronze >> run_ge_checkpoint >> transform_to_silver`
    """,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "silver", "shopify"],
) as dag:
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"python {INGESTION_DIR}/generate_synthetic_shopify_data.py",
        cwd="/opt/airflow",
        env={"CARTCO_BATCH_DATE": "{{ ds }}"},
        append_env=True,
        doc="Generates the synthetic Shopify orders CSV consumed by ingest_to_bronze.",
    )

    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=f"python {INGESTION_DIR}/shopify_bronze.py",
        cwd="/opt/airflow",
        env={"CARTCO_BATCH_DATE": "{{ ds }}"},
        append_env=True,
        doc=(
            "Lands the synthetic Shopify CSV into bronze.shopify_orders "
            "(Iceberg on MinIO), partitioned by ingestion_date."
        ),
    )

    run_ge_checkpoint = PythonOperator(
        task_id="run_ge_checkpoint",
        python_callable=ge_bronze_shopify_checkpoint.run,
        op_kwargs={"batch_date": "{{ ds }}"},
        doc_md=(
            "Runs Great Expectations quality gate on bronze.shopify_orders. "
            "Fails the DAG if any expectation is violated — Silver will not "
            "be written."
        ),
    )

    transform_to_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=silver_shopify_orders.run,
        op_kwargs={"batch_date": "{{ ds }}"},
        doc_md=(
            "Reads bronze.shopify_orders, applies canonical schema + dedup + "
            "null handling, writes silver.orders partitioned by "
            "order_date."
        ),
    )

    generate_data >> ingest_to_bronze >> run_ge_checkpoint >> transform_to_silver
