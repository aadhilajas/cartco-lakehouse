"""CartCo Amazon Bronze pipeline: synthetic data generation -> Bronze ingestion.

Two tasks, in order:
    1. ``generate_data``      -- generate the synthetic Amazon orders CSV.
    2. ``ingest_to_bronze``   -- land it into ``bronze.amazon_orders`` (Iceberg
                                 on MinIO), partitioned by ``ingestion_date``.

Re-runnable: every task is idempotent (Bronze does a partition-level
overwrite on ``ingestion_date``).
"""

from __future__ import annotations

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = "/opt/airflow"
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

INGESTION_DIR = "/opt/airflow/ingestion"

default_args = {
    "owner": "cartco",
    "retries": 1,
}

with DAG(
    dag_id="amazon_bronze_ingestion",
    default_args=default_args,
    description=(
        "Amazon Bronze pipeline: synthetic data generation -> Bronze Iceberg ingestion"
    ),
    doc_md="""
    ### Amazon Bronze pipeline

    Amazon Bronze pipeline: synthetic data generation -> Bronze Iceberg ingestion.
    Re-runnable: each task is idempotent.

    **Chain:** `generate_data >> ingest_to_bronze`
    """,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "amazon"],
) as dag:
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"python {INGESTION_DIR}/generate_synthetic_amazon_data.py",
        cwd="/opt/airflow",
        doc="Generates the synthetic Amazon orders CSV consumed by ingest_to_bronze.",
    )

    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=f"python {INGESTION_DIR}/amazon_bronze.py",
        cwd="/opt/airflow",
        doc=(
            "Lands the synthetic Amazon CSV into bronze.amazon_orders "
            "(Iceberg on MinIO), partitioned by ingestion_date."
        ),
    )

    generate_data >> ingest_to_bronze
