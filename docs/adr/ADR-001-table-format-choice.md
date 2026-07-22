# ADR-001: Table Format Choice

**Status:** Accepted
**Date:** 2026-07-02

## Context

CartCo's medallion lakehouse stores data across three layers (Bronze, Silver, Gold) on MinIO, a local S3-compatible object store. The table format must support ACID transactional writes so that concurrent or retried DAG runs never leave a table in a corrupt state. It must also support partition-level overwrite for idempotent loads — Bronze overwrites today's `ingestion_date` partition on re-run, and Silver overwrites the `order_date` range it just transformed. Schema evolution is essential because upstream source systems (Shopify, Amazon, in-store POS) change columns over time, and the format must handle additive changes without rewriting the entire table.

## Decision

Use **Apache Iceberg** as the table format for all layers (Bronze, Silver, and Gold), accessed via **PyIceberg 0.7.1** and the **Iceberg REST catalog** (`tabulario/iceberg-rest`). Tables are stored as Parquet data files in the `s3://cartco-lakehouse` bucket on MinIO, with metadata managed by the REST catalog.

## Consequences

**Positive:**
- ACID writes via PyIceberg's `table.overwrite(df, overwrite_filter=...)` enable the idempotent partition-level overwrite pattern used in both `ingestion/shopify_bronze.py` and `transforms/silver_shopify_orders.py`.
- Hidden partitioning means queries on `order_date` or `ingestion_date` are automatically pruned without callers needing to know the partition layout.
- Partition evolution allows changing the partition scheme (e.g. from daily to monthly) without rewriting historical data.
- Multi-engine support: PyIceberg (current), PySpark (installed, future), Trino, and Flink can all read the same tables.
- The REST catalog (`tabulario/iceberg-rest`) runs as a single container with no JVM dependency on the client side — PyIceberg talks to it over HTTP.

**Negative:**
- PyIceberg 0.7.1 is relatively early in its development; some operations (e.g. row-level deletes, merge-on-read) are not yet supported and would require PySpark.
- The REST catalog is a single point of failure in the local stack (no HA). Acceptable for an internship project but not for production.
- Debugging Iceberg metadata issues requires inspecting snapshot JSON files in MinIO, which has a steeper learning curve than simple Parquet directories.

## Alternatives Considered

- **Delta Lake (delta-rs / delta-spark):** Supports ACID writes and time travel. Rejected because Iceberg's multi-engine openness (REST catalog, no vendor lock-in) and the availability of the lightweight `tabulario/iceberg-rest` Docker image made it a better fit for a local-only, multi-tool stack. Delta Lake's tighter coupling to Spark and Databricks was a concern for a PyArrow-first project.
- **Raw Parquet on MinIO (no table format):** Simplest option, but provides no ACID guarantees, no partition-level overwrite, and no schema evolution. The idempotent re-run requirement rules this out.
