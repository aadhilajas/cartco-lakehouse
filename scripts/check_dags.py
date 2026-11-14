"""Import real DAGs and validate their graph, without contacting the lakehouse.

Run in the Airflow image (Airflow is not supported natively on Windows).
"""
from pathlib import Path

from airflow.models import DagBag
from airflow.serialization.serialized_objects import SerializedDAG


def main() -> None:
    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    if bag.import_errors:
        raise RuntimeError(f"DAG import errors: {bag.import_errors}")
    source_chain = ["generate_data", "ingest_to_bronze", "run_ge_checkpoint", "transform_to_silver"]
    runtime_chain = ["prepare_run", "ingest_shopify", "ingest_amazon", "ingest_pos", "prepare_inventory", "publish_gold"]
    expected = {
        "shopify_bronze_ingestion": set(source_chain),
        "amazon_bronze_ingestion": set(source_chain),
        "versioned_commerce_release": set(runtime_chain),
        "gold_marts_ingestion": {"silver_ready_sensor", "daily_revenue", "channel_performance", "inventory_turnover", "customer_360"},
    }
    if set(bag.dags) != set(expected):
        raise RuntimeError(f"Unexpected DAG inventory: {sorted(bag.dags)}")
    for name, task_ids in expected.items():
        dag = bag.dags[name]
        if set(dag.task_ids) != task_ids:
            raise RuntimeError(f"Unexpected tasks in {name}: {dag.task_ids}")
        if name == "versioned_commerce_release":
            if dag.max_active_runs != 1 or dag.schedule_interval is not None or dag.catchup or not dag.is_paused_upon_creation or str(dag.timezone) != "Asia/Kolkata":
                raise RuntimeError("Versioned runtime must be manual, paused and serialized in business time")
            for index, task_id in enumerate(runtime_chain):
                task = dag.get_task(task_id)
                expected_downstream = {runtime_chain[index + 1]} if index + 1 < len(runtime_chain) else set()
                if task.downstream_task_ids != expected_downstream or task.trigger_rule != "all_success":
                    raise RuntimeError(f"Unsafe runtime dependency: {task_id}")
                if task.op_kwargs.get("run_id") != "{{ run_id }}" or task.op_kwargs.get("batch_date") != "{{ dag_run.conf.get('batch_date', ds) }}":
                    raise RuntimeError(f"Missing runtime context: {task_id}")
                if getattr(task.on_failure_callback, "__name__", None) != "task_failure_callback" or getattr(task.on_retry_callback, "__name__", None) != "task_retry_callback":
                    raise RuntimeError(f"Missing runtime failure/retry alert callbacks: {task_id}")
                if task.retries < 1 or task.execution_timeout is None:
                    raise RuntimeError(f"Missing bounded runtime retry/timeout: {task_id}")
            for source in ("shopify", "amazon", "pos"):
                if dag.get_task(f"ingest_{source}").op_kwargs.get("source") != source:
                    raise RuntimeError(f"Incorrect runtime source: {source}")
        elif name != "gold_marts_ingestion":
            if dag.max_active_runs != 1:
                raise RuntimeError(f"Overlapping source runs are unsafe: {name}")
            for task_id in source_chain[:2]:
                task = dag.get_task(task_id)
                if task.env.get("CARTCO_BATCH_DATE") != "{{ ds }}" or not task.append_env:
                    raise RuntimeError(f"Missing source batch date: {name}.{task_id}")
            for task_id in source_chain[2:]:
                if dag.get_task(task_id).op_kwargs.get("batch_date") != "{{ ds }}":
                    raise RuntimeError(f"Missing transform batch date: {name}.{task_id}")
            for index, task_id in enumerate(source_chain):
                downstream = {source_chain[index + 1]} if index + 1 < len(source_chain) else set()
                if dag.get_task(task_id).downstream_task_ids != downstream:
                    raise RuntimeError(f"Incorrect source dependency: {name}.{task_id}")
        else:
            marts = task_ids - {"silver_ready_sensor"}
            if dag.get_task("silver_ready_sensor").downstream_task_ids != marts:
                raise RuntimeError("Gold sensor must precede every mart")
            if any(dag.get_task(task_id).downstream_task_ids for task_id in marts):
                raise RuntimeError("Gold marts must be terminal tasks")
        restored = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        if restored.dag_id != name or set(restored.task_ids) != task_ids or any(
            restored.get_task(task_id).downstream_task_ids != dag.get_task(task_id).downstream_task_ids for task_id in task_ids
        ):
            raise RuntimeError(f"DAG serialization changed its graph: {name}")
        print(f"PASS {name}: {len(task_ids)} tasks, serialization graph preserved")


if __name__ == "__main__":
    main()
