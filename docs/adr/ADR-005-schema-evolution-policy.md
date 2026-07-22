# ADR-005: Schema Evolution Policy

**Status:** Accepted
**Date:** 2026-07-02

## Context

CartCo ingests from multiple upstream systems (Shopify, Amazon, in-store POS) that each have their own schema and may add, rename, or remove columns over time. The lakehouse must handle additive schema changes (new upstream columns) gracefully without breaking downstream Silver and Gold consumers. At the same time, Silver must enforce a canonical schema so that Gold marts and analytics queries have a stable contract to depend on. The boundary between "accept everything" (Bronze) and "enforce a contract" (Silver) must be explicit.

## Decision

- **Bronze:** Accept all source columns as-is, with no renaming, filtering, or type coercion beyond what Iceberg requires. The Bronze schema is defined explicitly per source in each ingestion script (e.g. `TABLE_SCHEMA` in `ingestion/shopify_bronze.py`) and matches the source's own column names and types. If an upstream source adds a new column, the Bronze schema is updated to include it — Iceberg's schema evolution handles this as an additive change without rewriting existing data.
- **Silver:** Enforce a canonical entity schema that is shared across all channels for the same business entity. Silver scripts (e.g. `transforms/silver_shopify_orders.py`) explicitly define `TABLE_SCHEMA`, `COLUMN_ORDER`, and transformation logic (renaming `status` → `order_status`, deriving `order_total`, truncating `order_date` to date). A **Great Expectations quality gate** (`transforms/ge_bronze_shopify_checkpoint.py`) validates Bronze data against 8 expectations *before* Silver runs — if Bronze data violates schema or value constraints, the DAG halts and Silver is never written.
- **Gold:** Breaking changes (removing or renaming a Gold column) require a new mart version or an ADR amendment. Gold schemas are not yet implemented but will follow this policy.

## Consequences

**Positive:**
- Bronze is a safe, lossless landing zone — no upstream data is ever dropped or altered, making it possible to reprocess historical data through updated Silver logic.
- Silver acts as the schema contract boundary: downstream Gold marts and analytics queries depend only on Silver's canonical schema, not on any source system's raw column names.
- The GE checkpoint enforces the contract at runtime — 8 expectations on `bronze.shopify_orders` (null checks, value-set checks, range checks, row-count bounds) prevent dirty data from reaching Silver. A checkpoint failure raises `RuntimeError` and halts the DAG.
- Source-faithful Bronze column names (e.g. Shopify's `status` with values `delivered/cancelled/returned` vs. Amazon's `status` with values `shipped/cancelled/refund_initiated/pending`) document what each source actually sends, without premature harmonisation.

**Negative:**
- Each Silver transform must explicitly handle the mapping from source-faithful Bronze columns to canonical Silver columns (e.g. `STATUS_CANONICAL_MAP` in `silver_shopify_orders.py`). This per-channel mapping logic is duplicated rather than shared.
- The `_raw_extensions` map column mentioned in the original ADR is not yet implemented — new upstream fields currently require a schema update in both the Bronze ingestion script and any downstream Silver transforms. This is a known gap.
- Bronze schemas are manually defined in Python (`NestedField` lists), not discovered dynamically from the source. A source schema change requires a code change in the ingestion script.

## Alternatives Considered

- **Schema-on-read (no explicit schema in Bronze):** Let Iceberg infer the schema from each Parquet file. Rejected because implicit schemas make it impossible to enforce type consistency across loads — a column that is `int` in one load and `string` in the next would create a broken table. Explicit `TABLE_SCHEMA` definitions prevent this.
- **Single unified schema across all Bronze tables:** Use the same column set for Shopify, Amazon, and in-store, with nulls for source-specific columns. Rejected because it obscures the actual source data and creates a premature union that belongs in Silver or Gold, not Bronze.
- **Schema registry (e.g. Confluent Schema Registry):** Would provide centralised schema versioning and compatibility checks. Out of scope for the local-first internship stack, but would be a natural addition in a production deployment alongside the Kafka in-store connector.
