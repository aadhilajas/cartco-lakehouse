# ADR-003: Partition Strategy

**Status:** Accepted  
**Date:** 2026-07-02

## Context

Raw and curated tables must be partitioned for efficient incremental reads without over-partitioning small datasets.

## Decision

Partition all Bronze tables by `ingestion_date=YYYY-MM-DD`. Silver and Gold tables inherit date-based partitioning aligned to the business grain of each mart.

## Consequences

- Daily Airflow DAG runs map cleanly to partition boundaries.
- Backfills target specific `ingestion_date` partitions without full table scans.
- Entity-level partitioning (e.g., `customer_id`) is deferred to Gold marts where query patterns justify it.
