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
for dag in shopify_bronze_ingestion amazon_bronze_ingestion gold_marts_ingestion; do
  docker compose exec -T airflow-scheduler airflow dags test "$dag" "$run_date" 2>&1 | tee "test-results/${dag}.log"
done
docker compose exec -T -e RUN_INTEGRATION_TESTS=1 -e CARTCO_STRICT_INTEGRATION=1 airflow-scheduler \
  python -m pytest -m integration tests/integration -v --junitxml=/tmp/cartco-integration.xml \
  2>&1 | tee test-results/integration.log
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/check_live_baseline.py \
  | tee test-results/live-baseline.json
