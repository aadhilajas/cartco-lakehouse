#!/usr/bin/env bash
# Disposable CI stack only. Workflow teardown owns storage cleanup.
set -euo pipefail
mkdir -p test-results
for attempt in $(seq 1 60); do
  status=$(docker inspect -f '{{.State.Health.Status}}' cartco-airflow-scheduler 2>/dev/null || echo starting)
  if [ "$status" = healthy ]; then break; fi
  if [ "$attempt" -eq 60 ]; then echo 'Scheduler never became healthy' >&2; exit 1; fi
  sleep 5
done
docker compose exec -T airflow-scheduler python - <<'PY'
import time
import urllib.request
for attempt in range(60):
    try:
        urllib.request.urlopen("http://iceberg-rest:8181/v1/config", timeout=5).close()
        break
    except Exception:
        if attempt == 59:
            raise
        time.sleep(2)
PY
run_date=$(date -u +%Y-%m-%dT00:00:00+00:00)
# Explicit fixtures/new-catalog permission only in the disposable CI stack.
for pass in first replay; do
  docker compose exec -T -e CARTCO_RUNTIME_MODE=synthetic -e CARTCO_RUNTIME_INVENTORY_MODE=synthetic \
    -e CARTCO_RUNTIME_ALLOW_EMPTY_INIT=true -e CARTCO_RUNTIME_ROWS_PER_SOURCE=50 airflow-scheduler \
    airflow dags test versioned_commerce_release "$run_date" 2>&1 | tee "test-results/versioned-${pass}.log"
done
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/check_versioned_runtime.py --rows-per-source 50 \
  | tee test-results/versioned-runtime.json
docker compose exec -T airflow-scheduler python - <<'PY' | tee test-results/runtime-observability.json
import json
from pathlib import Path
from scripts.check_runtime_observability import inspect_run
root = Path("/opt/airflow/data/versioned")
reports = []
for path in (root / "runs").glob("*/*/run.json"):
    manifest = json.loads(path.read_text())
    report = inspect_run(root, manifest["batch_date"], manifest["run_id"])
    if not report["clean_history"]:
        raise RuntimeError("Versioned runtime observability history is incomplete or failed")
    reports.append(report)
if not reports:
    raise RuntimeError("No versioned observability journal was retained")
print(json.dumps(reports, indent=2))
PY
for dag in shopify_bronze_ingestion amazon_bronze_ingestion gold_marts_ingestion; do
  docker compose exec -T airflow-scheduler airflow dags test "$dag" "$run_date" 2>&1 | tee "test-results/${dag}.log"
done
docker compose exec -T -e RUN_INTEGRATION_TESTS=1 -e CARTCO_STRICT_INTEGRATION=1 -e "CARTCO_BATCH_DATE=${run_date%%T*}" airflow-scheduler \
  python -m pytest -m integration tests/integration -v --junitxml=/tmp/cartco-integration.xml \
  2>&1 | tee test-results/integration.log
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/check_live_baseline.py \
  | tee test-results/live-baseline.json
