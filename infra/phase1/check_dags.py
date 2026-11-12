"""Container acceptance: fail on import errors or a missing diagnostic DAG."""
import json
from airflow.models import DagBag

bag = DagBag(include_examples=False)
print(json.dumps({"dags": sorted(bag.dags), "import_errors": bag.import_errors}))
if bag.import_errors or "cartco_infrastructure_readiness" not in bag.dags:
    raise SystemExit(1)
