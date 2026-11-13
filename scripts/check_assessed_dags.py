"""Actual Airflow import, cycle and serialized-DAG roundtrip verification."""
import json
from airflow.models import DagBag
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils.dag_cycle_tester import check_cycle

bag = DagBag("/opt/airflow/dags", include_examples=False)
expected = {"assessed_orders_ingestion", "assessed_inventory_ingestion", "assessed_commerce_release", "assessed_system_health"}
if bag.import_errors or set(bag.dags) != expected:
    raise RuntimeError("Assessed DAG imports/IDs differ: " + json.dumps({"errors": bag.import_errors, "dags": sorted(bag.dags)}))
tasks = {}
for name, dag in bag.dags.items():
    check_cycle(dag)
    restored = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
    if set(restored.task_ids) != set(dag.task_ids): raise RuntimeError("DAG serialization changed tasks")
    tasks[name] = sorted(dag.task_ids)
print(json.dumps({"dags": tasks, "import_errors": {}, "serialized_roundtrip": True}))
