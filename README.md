# CartCo Unified Commerce Lakehouse

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-pending-lightgrey)](https://github.com/aadhilajas/cartco-lakehouse)

**One source of truth for revenue across 5 sales channels.**

---

## Demo

> **Video:** [LOOM_LINK_PENDING] — end-to-end pipeline walkthrough to be recorded after Docker Compose stack is running.

### Local deployment (once stack is configured)

```bash
git clone https://github.com/aadhilajas/cartco-lakehouse.git
cd cartco-lakehouse
docker compose up -d
```

- **Airflow UI:** `http://localhost:8080` (credentials TBD in docker-compose.yml)
- **MinIO Console:** `http://localhost:9001`
- **Marquez UI:** `http://localhost:3000`

---

## Problem Statement

CartCo is a multi-channel retailer doing approximately **₹4,000 Cr GMV** across five disconnected sales channels — its own D2C website (Shopify), Amazon marketplace, physical stores (POS/Kafka), distributor SFTP feeds, and internal ERP inventory exports. Each system reports revenue differently, on different schedules, with inconsistent schemas. Finance cannot trust any single number for board reporting, marketing cannot attribute channel performance accurately, and operations lacks a unified inventory view. This project builds a medallion lakehouse that ingests raw data from all five sources, enforces quality and schema contracts in a Silver layer, and publishes trusted Gold marts — giving CartCo one reconciled revenue figure and channel-level insights.

---

## Architecture

![C4 Context Diagram](docs/architecture/c4_context.png)

Data flows through three medallion layers on MinIO object storage, managed as Apache Iceberg tables. **Bronze** lands raw source data partitioned by `ingestion_date` with no business logic. **Silver** applies schema enforcement, deduplication, and null handling to produce canonical entity tables validated by Great Expectations. **Gold** exposes business-facing marts — revenue, channel performance, inventory turnover, and customer 360 — consumed by analysts and downstream tools. Apache Airflow orchestrates the pipeline; OpenLineage emits lineage events to Marquez.

![C4 Container Diagram](docs/architecture/c4_container.png)

---

## Tech Stack

| Component | Choice | Why |
|-----------|--------|-----|
| Storage | MinIO (S3-compatible) | Local-first object storage; identical API to production S3 |
| Table format | **Apache Iceberg** | ACID writes, time travel, schema evolution, open governance |
| Compute | PySpark | Single engine for ingestion and transforms; native Iceberg support |
| Orchestrator | **Apache Airflow** (LocalExecutor) | Industry-standard DAG orchestration; runs on a single machine for local dev |
| Data quality | Great Expectations | Checkpoint-based validation before every Silver write |
| Lineage | OpenLineage + Marquez | End-to-end column-level lineage across all pipeline stages |
| Catalog | Hive Metastore | Lightweight local catalog compatible with Iceberg and Docker Compose |

---

## Quickstart

Follow these steps to get the project running locally. Target: **under 15 minutes** once the Docker stack is fully configured.

### Prerequisites

- **Git** 2.30+
- **Docker Desktop** 4.x with Docker Compose v2
- **Python** 3.11+ (for running ingestion scripts and tests outside containers)
- **8 GB RAM** minimum (Spark + Airflow containers)

### Install

```bash
# 1. Clone the repository
git clone https://github.com/aadhilajas/cartco-lakehouse.git
cd cartco-lakehouse

# 2. Create a Python virtual environment (for local script execution)
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

# 3. Install Python dependencies (requirements.txt to be added)
pip install -r requirements.txt
```

### Run

```bash
# Start the full local stack (MinIO, Airflow, Spark, Marquez)
docker compose up -d

# Verify services are healthy
docker compose ps

# Trigger a Bronze ingestion DAG (once implemented)
# docker compose exec airflow airflow dags trigger bronze_shopify
```

### Test

```bash
# Run unit and integration tests (once implemented)
pytest tests/

# Run Great Expectations checkpoints (once configured)
# great_expectations checkpoint run silver_orders_checkpoint
```

---

## Data

See [docs/data.md](docs/data.md) for source schemas and synthetic dataset documentation.

All data used in this project is **synthetically generated** — representing Shopify orders, Amazon settlements, in-store POS transactions, distributor shipments, and ERP inventory exports with no real PII.

---

## Architecture Decision Records

Full ADRs live in [docs/adr/](docs/adr/).

| ADR | Summary |
|-----|---------|
| [ADR-001](docs/adr/ADR-001-table-format-choice.md) | Apache Iceberg chosen for ACID writes, time travel, and multi-engine support |
| [ADR-002](docs/adr/ADR-002-orchestrator-choice.md) | Apache Airflow (LocalExecutor) for batch DAG orchestration |
| [ADR-003](docs/adr/ADR-003-partition-strategy.md) | `ingestion_date=YYYY-MM-DD` partitioning across Bronze and downstream layers |
| [ADR-004](docs/adr/ADR-004-ingestion-tool-choice.md) | Per-source PySpark ingestion scripts orchestrated by Airflow |
| [ADR-005](docs/adr/ADR-005-schema-evolution-policy.md) | Bronze accepts all columns; Silver enforces canonical schema with staged extensions |

---

## Known Limitations

- **Kafka streaming source** not yet implemented — in-store POS ingestion is stubbed.
- **Single-node Spark only** — no cluster mode or dynamic allocation configured.
- **Docker Compose stack** is a skeleton — service definitions pending in `docker-compose.yml`.
- **Architecture diagrams** are placeholders — full C4 diagrams to be exported in Week 1 deliverables.
- **No cloud deployment** — MinIO and Hive Metastore are local-only; no Glue or production S3 integration yet.
- **Great Expectations** checkpoints not yet wired to Silver transforms.

---

## Roadmap

With two additional weeks beyond the current internship timeline, the next priorities would be:

1. **Kafka Structured Streaming** — real-time Bronze ingestion for in-store POS with micro-batch fallback.
2. **Data contracts** — formal schema contracts between Silver entities and Gold marts with breaking-change alerts.
3. **Terraform IaC** — reproducible cloud deployment (S3 + EMR Serverless + MWAA) mirroring the local stack.
4. **Feast feature store** — serve Gold customer-360 features for ML-driven personalization models.

---

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgements

Built as part of the **Futurense Data Platform & Pipeline Engineering Internship** (Problem Code B1). Thanks to the Futurense program for the structured learning path and CartCo business scenario.
