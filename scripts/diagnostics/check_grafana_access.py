"""Read-only role proof; negative statements have an always-false predicate."""

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
import psycopg2

connection = psycopg2.connect(host="airflow-db", dbname="airflow", user="cartco_grafana", password="grafana-local-only")
with connection.cursor() as cursor:
    cursor.execute("SELECT count(*) FROM cartco_monitoring.dag_health")
    rows = cursor.fetchone()[0]
connection.rollback()
for query in ["SELECT conf FROM public.dag_run WHERE false", "DELETE FROM cartco_monitoring.dag_health WHERE false"]:
    try:
        with connection.cursor() as cursor: cursor.execute(query)
    except (psycopg2.errors.InsufficientPrivilege, psycopg2.errors.ReadOnlySqlTransaction):
        connection.rollback()
    else:
        connection.rollback()
        raise RuntimeError("Grafana role has broader access than its read-only health view")
connection.close()
print(json.dumps({"health_rows": rows, "raw_conf_denied": True, "write_denied": True}))
