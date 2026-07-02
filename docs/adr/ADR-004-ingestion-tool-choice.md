# ADR-004: Ingestion Tool Choice

**Status:** Accepted  
**Date:** 2026-07-02

## Context

CartCo ingests from five disconnected sources: Shopify API, Amazon reports, Kafka POS stream, SFTP distributor files, and internal ERP exports. Each source has different delivery semantics.

## Decision

Use **PySpark batch scripts** for file/API-based sources and a **Kafka consumer script** (PySpark Structured Streaming or batch micro-batches) for in-store POS. Orchestrate all ingestion via Airflow DAGs.

## Consequences

- A single compute engine (PySpark) handles both ingestion and transforms, reducing context switching.
- Dedicated connector scripts per source (`ingestion/`) keep Bronze logic isolated and testable.
- Fivetran/Airbyte were out of scope for the local-first internship deliverable.
