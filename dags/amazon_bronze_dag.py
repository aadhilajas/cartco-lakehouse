"""CartCo Amazon end-to-end pipeline: generate -> Bronze -> GE gate -> Silver.

Four tasks, in order:
    1. ``generate_data``      -- generate the synthetic Amazon orders CSV.
    2. ``ingest_to_bronze``   -- land it into ``bronze.amazon_orders`` (Iceberg
                                 on MinIO), partitioned by ``ingestion_date``.
    3. ``run_ge_checkpoint``  -- run the pre-Silver Great Expectations quality
                                 gate against ``bronze.amazon_orders``. Raises
                                 if any expectation fails, which fails this
                                 task and halts the DAG before Silver runs.
    4. ``transform_to_silver`` -- read Bronze, apply canonical schema +
                                 dedup + null handling, write
                                 ``silver.orders`` partitioned by
                                 ``order_date`` (Amazon rows only, so
                                 Shopify rows in the same table are not
                                 disturbed).

Re-runnable: every task is idempotent (Bronze does a partition-level
overwrite on ``ingestion_date``; Silver does a partition-level overwrite
filtered by ``channel='amazon'`` and the ``order_date`` range of the batch
it just transformed). The GE gate in task 3 halts the DAG if Bronze quality
thresholds are not met, so dirty Bronze data never reaches Silver.
"""

from __future__ import annotations

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = "/opt/airflow"
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

from transforms import ge_bronze_amazon_checkpoint, amazon_silver  # noqa: E402

INGESTION_DIR = "/opt/airflow/ingestion"

default_args = {
    "owner": "cartco",
    "retries": 1,
}

with DAG(
    dag_id="amazon_bronze_ingestion",
    default_args=default_args,
    description=(
        "End-to-end Amazon pipeline: synthetic data generation -> Bronze "
        "Iceberg ingestion -> GE quality gate -> Silver canonical transform"
    ),
    doc_md="""
    ### Amazon end-to-end pipeline

    End-to-end Amazon pipeline: synthetic data generation -> Bronze Iceberg
    ingestion -> GE quality gate -> Silver canonical transform. Re-runnable:
    each task is idempotent. GE gate halts the DAG if Bronze quality
    thresholds are not met.

    **Chain:** `generate_data >> ingest_to_bronze >> run_ge_checkpoint >> transform_to_silver`
    """,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "silver", "amazon"],
) as dag:
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"python {INGESTION_DIR}/generate_synthetic_amazon_data.py",
        cwd="/opt/airflow",
        env={"CARTCO_BATCH_DATE": "{{ ds }}"},
        append_env=True,
        doc="Generates the synthetic Amazon orders CSV consumed by ingest_to_bronze.",
    )

    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=f"python {INGESTION_DIR}/amazon_bronze.py",
        cwd="/opt/airflow",
        env={"CARTCO_BATCH_DATE": "{{ ds }}"},
        append_env=True,
        doc=(
            "Lands the synthetic Amazon CSV into bronze.amazon_orders "
            "(Iceberg on MinIO), partitioned by ingestion_date."
        ),
    )

    run_ge_checkpoint = PythonOperator(
        task_id="run_ge_checkpoint",
        python_callable=ge_bronze_amazon_checkpoint.run,
        op_kwargs={"batch_date": "{{ ds }}"},
        doc_md=(
            "Runs Great Expectations quality gate on bronze.amazon_orders. "
            "Fails the DAG if any expectation is violated — Silver will not "
            "be written."
        ),
    )

    transform_to_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=amazon_silver.run,
        op_kwargs={"batch_date": "{{ ds }}"},
        doc_md=(
            "Reads bronze.amazon_orders, applies canonical schema + dedup + "
            "null handling + status mapping + asin->product_id rename, "
            "writes silver.orders (channel='amazon' rows only) partitioned "
            "by order_date."
        ),
    )

    generate_data >> ingest_to_bronze >> run_ge_checkpoint >> transform_to_silver
