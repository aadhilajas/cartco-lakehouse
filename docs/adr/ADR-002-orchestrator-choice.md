# ADR-002: Orchestrator Choice

**Status:** Accepted  
**Date:** 2026-07-02

## Context

Scheduled batch pipelines across Bronze, Silver, and Gold need a reliable orchestrator with retry semantics, dependency management, and local development support.

## Decision

Use **Apache Airflow** with **LocalExecutor** for orchestration.

## Consequences

- Mature DAG ecosystem and Python-native task definitions align with PySpark transforms.
- LocalExecutor keeps the internship stack runnable on a single machine via Docker Compose.
- Dagster and Prefect were considered; Airflow's industry prevalence and internship spec alignment drove the choice.
