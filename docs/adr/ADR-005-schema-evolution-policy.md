# ADR-005: Schema Evolution Policy

**Status:** Accepted  
**Date:** 2026-07-02

## Context

Upstream retail systems change columns frequently. The lakehouse must handle additive schema changes without breaking downstream marts.

## Decision

- **Bronze:** Accept all columns as-is; Iceberg schema evolution allows additive changes automatically.
- **Silver:** Enforce a canonical entity schema; new upstream fields are staged in a `_raw_extensions` map column until promoted.
- **Gold:** Breaking changes require a new mart version or ADR amendment.

## Consequences

- Silver acts as the schema contract boundary between raw and curated data.
- Great Expectations checkpoints validate Silver schema conformance on every run.
- Destructive upstream changes trigger an explicit review before Silver promotion.
