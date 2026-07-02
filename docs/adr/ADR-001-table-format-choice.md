# ADR-001: Table Format Choice

**Status:** Accepted  
**Date:** 2026-07-02

## Context

CartCo needs an open table format for the medallion lakehouse that supports ACID writes, time travel, and schema evolution on object storage.

## Decision

Use **Apache Iceberg** as the table format for Bronze, Silver, and Gold layers.

## Consequences

- PySpark and MinIO integrate natively via the Iceberg catalog.
- Partition evolution and hidden partitioning reduce operational overhead as data volume grows.
- Delta Lake was considered but Iceberg's multi-engine support and open governance model better fit the internship stack.
