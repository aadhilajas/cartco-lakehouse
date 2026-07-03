"""Orchestrate CartCo Shopify Bronze ingestion.

Lands Shopify website order data into the Bronze layer (``bronze.shopify_orders``)
so analytics has a raw, queryable copy of D2C website sales. Runs synthetic data
generation first, then the Bronze load into Iceberg on MinIO.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

INGESTION_DIR = "/opt/airflow/ingestion"

default_args = {
    "owner": "cartco",
    "retries": 1,
}

with DAG(
    dag_id="shopify_bronze_ingestion",
    default_args=default_args,
    description="Generate synthetic Shopify orders and load into Bronze Iceberg table",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "shopify"],
) as dag:
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"python {INGESTION_DIR}/generate_synthetic_shopify_data.py",
        cwd="/opt/airflow",
    )

    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=f"python {INGESTION_DIR}/shopify_bronze.py",
        cwd="/opt/airflow",
    )

    generate_data >> ingest_to_bronze
