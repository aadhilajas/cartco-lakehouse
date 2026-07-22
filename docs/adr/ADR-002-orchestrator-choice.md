# ADR-002: Orchestrator Choice

**Status:** Accepted
**Date:** 2026-07-02

## Context

CartCo's lakehouse pipeline consists of multiple dependent tasks — synthetic data generation, Bronze ingestion, a Great Expectations quality gate, and Silver transformation — that must execute in a strict order. A failed quality gate must halt the pipeline before Silver is written. The orchestrator must support task dependencies, retries, manual re-triggers, and be runnable on a single developer machine via Docker Compose without requiring a distributed cluster (Kubernetes, Celery workers, or cloud-managed services). The internship spec explicitly names Apache Airflow.

## Decision

Use **Apache Airflow 2.9.0** with **LocalExecutor** for all pipeline orchestration. The stack runs as two Airflow containers (webserver + scheduler) backed by a **PostgreSQL 14** metadata database, all defined in `docker-compose.yml`. DAGs use `BashOperator` for scripts that run as separate processes (data generation, Bronze ingestion) and `PythonOperator` for modules with a callable `run()` function (GE checkpoint, Silver transform).

## Consequences

**Positive:**
- LocalExecutor runs all tasks in the scheduler process — no Celery broker, no Redis, no worker containers. This keeps the Docker Compose stack to 2 Airflow containers + 1 Postgres.
- Task dependency chaining (`generate_data >> ingest_to_bronze >> run_ge_checkpoint >> transform_to_silver`) enforces the quality-gate-before-Silver contract at the orchestration level.
- `retries: 1` in `default_args` gives transient failures (e.g. catalog unavailability) one automatic retry before marking the task failed.
- The Airflow UI at `localhost:8080` provides run history, task logs, and manual trigger capability — useful for demos and debugging.
- Python-native task definitions (no YAML/config-driven workflows) align with the rest of the codebase.

**Negative:**
- LocalExecutor has no parallelism across multiple DAG runs — if Shopify and Amazon DAGs trigger simultaneously, tasks execute serially. This is acceptable for the current scale (2 DAGs, < 100k rows each).
- Airflow's own `apache-airflow-providers-openlineage` listener was explicitly disabled (`AIRFLOW__OPENLINEAGE__DISABLED=true`) because it produced empty dataset lineage for `BashOperator`/`PythonOperator` tasks, requiring manual OpenLineage emission instead (see `lineage/emitter.py`).
- The Airflow Docker image (`Dockerfile.airflow`) is large (~2 GB) because it bundles PySpark, PyIceberg, Great Expectations, and Faker on top of the base `apache/airflow:2.9.0` image.

## Alternatives Considered

- **Dagster:** Modern asset-based orchestration with built-in lineage and type checking. Rejected because the internship spec required Airflow, and Dagster's asset-centric model would have needed a different architectural approach than the task-based medallion pipeline already designed.
- **Prefect 2.x:** Simpler Python-native orchestration with less boilerplate than Airflow. Rejected for the same spec-alignment reason, plus Prefect's cloud-first deployment model adds friction for a fully local Docker Compose stack.
- **Plain cron + shell scripts:** No dependency management, no retry semantics, no UI for monitoring. Rejected as insufficiently robust for a multi-task pipeline with a quality gate.
