"""Import real DAGs and validate their graph, without contacting the lakehouse.

Run in the Airflow image (Airflow is not supported natively on Windows).
"""
from pathlib import Path

from airflow.models import DagBag


def main() -> None:
    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    if bag.import_errors:
        raise RuntimeError(f"DAG import errors: {bag.import_errors}")
    source_chain = ["generate_data", "ingest_to_bronze", "run_ge_checkpoint", "transform_to_silver"]
    expected = {
        "shopify_bronze_ingestion": set(source_chain),
        "amazon_bronze_ingestion": set(source_chain),
        "gold_marts_ingestion": {"silver_ready_sensor", "daily_revenue", "channel_performance", "inventory_turnover", "customer_360"},
    }
    if set(bag.dags) != set(expected):
        raise RuntimeError(f"Unexpected DAG inventory: {sorted(bag.dags)}")
    for name, task_ids in expected.items():
        dag = bag.dags[name]
        if set(dag.task_ids) != task_ids:
            raise RuntimeError(f"Unexpected tasks in {name}: {dag.task_ids}")
        if name != "gold_marts_ingestion":
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
        print(f"PASS {name}: {len(task_ids)} tasks")


if __name__ == "__main__":
    main()
