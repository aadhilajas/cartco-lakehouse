"""CartCo Gold Layer DAG: builds business-facing analytical marts from silver.orders.

Sensor:
    - ``silver_ready_sensor`` -- verifies that ``silver.orders`` exists and holds data.

Parallel Tasks (4 marts):
    - ``daily_revenue``       -- gold.daily_revenue
    - ``channel_performance`` -- gold.channel_performance
    - ``inventory_turnover``  -- gold.inventory_turnover
    - ``customer_360``        -- gold.customer_360

Structure:
    silver_ready_sensor >> [daily_revenue, channel_performance, inventory_turnover, customer_360]
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

AIRFLOW_HOME = "/opt/airflow"
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

from transforms.gold import (  # noqa: E402
    channel_performance,
    customer_360,
    daily_revenue,
    inventory_turnover,
)


def _check_silver_orders_ready() -> bool:
    """Sensor check: returns True if silver.orders exists and contains rows."""
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import NoSuchTableError

    catalog = load_catalog(
        "cartco",
        **{
            "type": "rest",
            "uri": os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"),
            "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
        },
    )
    try:
        table = catalog.load_table("silver.orders")
        return table.scan().to_arrow().num_rows > 0
    except (NoSuchTableError, Exception):
        return False


default_args = {
    "owner": "cartco",
    "retries": 1,
}

with DAG(
    dag_id="gold_marts_ingestion",
    default_args=default_args,
    description="Gold layer business marts: daily revenue, channel performance, inventory turnover, customer 360",
    doc_md="""
    ### Gold Layer DAG
    Reads unified `silver.orders` table and builds 4 parallel business-facing Iceberg marts.

    **Chain:** `silver_ready_sensor >> [daily_revenue, channel_performance, inventory_turnover, customer_360]`
    """,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "analytics", "marts"],
) as dag:
    silver_ready_sensor = PythonSensor(
        task_id="silver_ready_sensor",
        python_callable=_check_silver_orders_ready,
        poke_interval=5,
        timeout=60,
        mode="poke",
        doc_md="Waits for silver.orders table to exist and contain at least 1 row.",
    )

    t_daily_revenue = PythonOperator(
        task_id="daily_revenue",
        python_callable=daily_revenue.run,
        doc_md="Computes daily revenue aggregated by order_date and channel.",
    )

    t_channel_performance = PythonOperator(
        task_id="channel_performance",
        python_callable=channel_performance.run,
        doc_md="Computes lifetime channel performance metrics.",
    )

    t_inventory_turnover = PythonOperator(
        task_id="inventory_turnover",
        python_callable=inventory_turnover.run,
        doc_md="Computes units sold and turnover period by product_category and channel.",
    )

    t_customer_360 = PythonOperator(
        task_id="customer_360",
        python_callable=customer_360.run,
        doc_md="Computes Customer 360 profiles per customer_id and channel.",
    )

    silver_ready_sensor >> [
        t_daily_revenue,
        t_channel_performance,
        t_inventory_turnover,
        t_customer_360,
    ]
