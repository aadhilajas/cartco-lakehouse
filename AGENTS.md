# Agent Guidance — CartCo Unified Commerce Lakehouse

This repository implements a medallion lakehouse (Bronze → Silver → Gold) for CartCo, a multi-channel retailer.

## Tech stack (locked)

- **Storage:** MinIO
- **Table format:** Apache Iceberg
- **Compute:** PySpark
- **Orchestrator:** Apache Airflow (LocalExecutor)
- **Data quality:** Great Expectations
- **Lineage:** OpenLineage + Marquez

## Layer rules

- **Bronze:** Land raw data unmodified except format conversion (CSV/JSON → Parquet). Partition by `ingestion_date=YYYY-MM-DD`. No business logic, filtering, or deduplication.
- **Silver:** Schema enforcement, deduplication, null handling, canonical entity model. Every Silver transform must have a matching Great Expectations checkpoint before writing.
- **Gold:** Business-facing marts only (revenue, channel performance, inventory turnover, customer 360). No raw or intermediate fields.

## DAG requirements

Every DAG must include: `dag_id`, retries (minimum 1), a `tags` list identifying the layer (`bronze`, `silver`, `gold`), and a docstring explaining the business question it answers.

## Ingestion logging

Every ingestion script must log start time, row count ingested, and end time.

## Deliverables

Submission evidence lives in `deliverables/` only — never place runnable source code there.
