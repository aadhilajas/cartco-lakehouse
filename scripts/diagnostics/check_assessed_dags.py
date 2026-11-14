"""Actual Airflow import, cycle and serialized-DAG roundtrip verification."""

# Support direct CLI execution and imports from any working directory.
import sys as _sys
from pathlib import Path as _Path
_SCRIPT_PATH = _Path(__file__).resolve()
_REPOSITORY_ROOT = next(
    (parent for parent in _SCRIPT_PATH.parents
     if (parent / "pipeline/source_identity.py").is_file()
     and (parent / "scripts/layout.json").is_file()),
    _SCRIPT_PATH.parent,  # Individually copied stdlib tools do not need a repo.
)
if str(_REPOSITORY_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPOSITORY_ROOT))
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
