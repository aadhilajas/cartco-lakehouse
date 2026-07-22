# ADR-003: Partition Strategy

**Status:** Accepted
**Date:** 2026-07-02

## Context

Every Iceberg table in the lakehouse must be partitioned to enable two critical operations: (1) idempotent writes — re-running a pipeline stage for the same day must replace, not duplicate, that day's data; and (2) efficient incremental reads — downstream consumers should scan only the partitions relevant to their query without full table scans. The partition key must balance granularity (too fine = many small files, too coarse = large scans on re-run) against the actual data volumes (~50k rows per Shopify load, ~40k per Amazon load). CartCo's synthetic data spans roughly 2 years of order history.

## Decision

- **Bronze tables** are partitioned by **`ingestion_date`** (identity transform on a `DateType` column), representing the date the data was loaded. The partition field is added by the ingestion script (e.g. `ingestion/shopify_bronze.py` sets `ingestion_date = date.today()`). Idempotent overwrite uses `EqualTo("ingestion_date", today.isoformat())`.
- **Silver tables** are partitioned by **`order_date`** (identity transform on a `DateType` column), representing the business date of the order. The Silver transform (`transforms/silver_shopify_orders.py`) truncates the timestamp `order_date` to `date32()` and overwrites the min–max `order_date` range of the transformed batch using an `And(GreaterThanOrEqual, LessThanOrEqual)` filter.
- **Gold tables** (not yet implemented) will inherit date-based partitioning aligned to the aggregation grain of each mart.

## Consequences

**Positive:**
- Daily Airflow DAG runs map 1:1 to Bronze partition boundaries — a run on 2026-07-22 writes to `ingestion_date=2026-07-22`, and re-running that day overwrites exactly that partition.
- Silver's `order_date` partitioning enables date-range queries from Gold marts without scanning the entire table.
- Backfills can target specific `ingestion_date` (Bronze) or `order_date` (Silver) partitions without touching other data.
- Identity transforms on date columns produce human-readable partition paths in MinIO (e.g. `ingestion_date=2026-07-22/`), making manual debugging straightforward.

**Negative:**
- With ~50k rows per daily load, each Bronze partition is a single small Parquet file (~2–5 MB). At scale this would create a "small files" problem, but Iceberg's compaction and the current data volumes make this acceptable.
- Silver partitioning by `order_date` means a single Bronze load (spanning ~2 years of synthetic order dates) touches ~730 Silver partitions. The range-overwrite filter handles this correctly, but it's a wide overwrite compared to Bronze's single-partition write.
- No entity-level partitioning (e.g. by `customer_id` or `product_category`) — deferred to Gold marts where query patterns justify it.

## Alternatives Considered

- **Partition by `channel`:** Would isolate Shopify/Amazon/in-store data into separate partitions, but the idempotent overwrite requirement is date-based (re-run today's load), not channel-based. Channel isolation is already achieved by having separate tables per source (`bronze.shopify_orders`, `bronze.amazon_orders`).
- **Monthly partitioning (`YearMonth` transform):** Would reduce partition count but make daily idempotent overwrites impossible — a re-run would need to overwrite an entire month's partition, which is disproportionate for a daily load.
- **No partitioning (unpartitioned tables):** Simplest approach, but every overwrite would rewrite the entire table and every scan would read all data. Ruled out for both performance and idempotency reasons.
