"""Isolated restored-stack acceptance only; never mounted by normal Compose."""
from datetime import timedelta
import json
import os
from pathlib import Path
import sys
import time

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0,'/opt/airflow')
from pipeline import runtime


def pos_commit_then_wait(batch_date,run_id):
    result=runtime.ingest_task(batch_date,run_id,'pos')
    marker=Path(os.environ['CARTCO_DRILL_MARKER'])
    if not marker.exists():
        # Only this acceptance task holds after a real receipt/commit. No
        # production runtime hooks or scheduler process signals are introduced.
        stat=Path(f'/proc/{os.getpid()}/stat').read_text()
        start_time=stat[stat.rfind(')')+2:].split()[19]
        marker.write_text(json.dumps(dict(pid=os.getpid(),process_start_time=start_time,
            run_id=run_id,dag_id='cartco_recovery_worker_drill',task_id='ingest_pos',
            receipt_result=result)),encoding='utf-8')
        time.sleep(300)
    return result


with DAG('cartco_recovery_worker_drill',schedule=None,start_date=pendulum.datetime(2026,1,1,tz='Asia/Kolkata'),
         catchup=False,is_paused_upon_creation=True,max_active_runs=1,
         default_args=dict(owner='cartco',retries=1,retry_delay=timedelta(seconds=5),
             execution_timeout=timedelta(minutes=10),on_retry_callback=runtime.task_retry_callback,
             on_failure_callback=runtime.task_failure_callback)) as dag:
    common=dict(batch_date="{{ dag_run.conf['batch_date'] }}",run_id='{{ run_id }}')
    prepare=PythonOperator(task_id='prepare_run',python_callable=runtime.prepare_task,op_kwargs=common)
    shopify=PythonOperator(task_id='ingest_shopify',python_callable=runtime.ingest_task,op_kwargs={**common,'source':'shopify'})
    amazon=PythonOperator(task_id='ingest_amazon',python_callable=runtime.ingest_task,op_kwargs={**common,'source':'amazon'})
    pos=PythonOperator(task_id='ingest_pos',python_callable=pos_commit_then_wait,op_kwargs=common)
    inventory=PythonOperator(task_id='prepare_inventory',python_callable=runtime.inventory_task,op_kwargs=common)
    publish=PythonOperator(task_id='publish_gold',python_callable=runtime.publish_task,op_kwargs=common)
    prepare >> shopify >> amazon >> pos >> inventory >> publish
