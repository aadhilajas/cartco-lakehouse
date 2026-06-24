# CartCo Unified Commerce Lakehouse

A medallion-style data platform for a multi-channel retail business, built with MinIO, Apache Iceberg, PySpark, Airflow, Great Expectations, and OpenLineage/Marquez. 

## Project Overview

CartCo sells through its own website, major marketplaces, and physical stores. The data lives in disconnected systems, which makes revenue, inventory, and channel reporting inconsistent across teams. This project builds a single trusted pipeline that ingests raw data, cleans and standardizes it, and publishes business-ready marts for reporting and analysis.

## Why This Project

This project is designed to demonstrate practical data engineering skills in a way that is both interview-defensible and realistic to complete within the internship timeline. It focuses on ingestion, orchestration, data quality, lineage, and reproducible infrastructure.

## What It Does

- Ingests data from multiple retail sources.
- Stores raw data in a bronze layer.
- Cleans and standardizes data in a silver layer.
- Produces business-facing marts in a gold layer.
- Runs scheduled workflows through Airflow.
- Validates critical tables with Great Expectations.
- Captures lineage with OpenLineage and Marquez.
- Runs locally using Docker Compose.

## Tech Stack

| Layer | Tool |
|---|---|
| Object Storage | MinIO |
| Table Format | Apache Iceberg |
| Compute | PySpark |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations |
| Lineage | OpenLineage + Marquez |
| Infrastructure | Docker Compose |
| IaC (stretch) | Terraform |

## Scope

### In Scope
- 3 synthetic source connectors
- Bronze, silver, and gold layers
- Airflow DAGs
- Data quality checks
- Lineage tracking
- Reproducible local setup

### Out of Scope
- Real-time sub-second streaming
- Multi-region high availability
- More than 3 initial sources

### Stretch Goals
- Kafka streaming source
- Data contracts
- Feast feature store
- Terraform-based cloud deployment

## Repository Structure

```text
cartco-lakehouse/
├── README.md
├── LICENSE
├── .gitignore
├── docs/
│   ├── initial_design_doc.docx
│   ├── architecture/
│   ├── adr/
│   ├── test_report.md
│   └── data.md
├── src/
├── dags/
├── tests/
├── configs/
└── data/
```

## Quick Start

### Prerequisites
- Python 3.11+
- Docker and Docker Compose
- Git

### Setup
```bash
git clone <your-repo-url>
cd cartco-lakehouse
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Run Locally
```bash
docker compose up -d
python src/main.py
```

### Testing
```bash
pytest
```
