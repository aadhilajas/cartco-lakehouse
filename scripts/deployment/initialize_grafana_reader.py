"""Expose a minimal Airflow health view through a read-only PostgreSQL role."""

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
import os
import time
import psycopg2
from psycopg2 import sql


def main():
    connection = None
    for attempt in range(60):
        try:
            connection = psycopg2.connect(os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].replace("postgresql+psycopg2://", "postgresql://"))
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass('public.dag_run')")
                if cursor.fetchone()[0]: break
            connection.close()
            connection = None
        except psycopg2.Error:
            if connection: connection.close()
            connection = None
        time.sleep(2)
    if connection is None: raise RuntimeError("Airflow schema did not become ready")
    password = os.environ["CARTCO_GRAFANA_DB_PASSWORD"]
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname='cartco_grafana'")
            if not cursor.fetchone(): cursor.execute("CREATE ROLE cartco_grafana LOGIN")
            cursor.execute(sql.SQL("ALTER ROLE cartco_grafana PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION").format(sql.Literal(password)))
            cursor.execute("ALTER ROLE cartco_grafana SET default_transaction_read_only=on")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS cartco_monitoring")
            cursor.execute("CREATE OR REPLACE VIEW cartco_monitoring.dag_health AS SELECT dag_id,run_id,state,start_date,end_date FROM public.dag_run")
            cursor.execute("GRANT CONNECT ON DATABASE airflow TO cartco_grafana")
            cursor.execute("GRANT USAGE ON SCHEMA cartco_monitoring TO cartco_grafana")
            cursor.execute("GRANT SELECT ON cartco_monitoring.dag_health TO cartco_grafana")
        print("Grafana role can read only the declared Airflow health view")
    finally:
        connection.close()


if __name__ == "__main__": main()
