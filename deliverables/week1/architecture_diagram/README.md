# Note on this diagram

`cartco_architecture.pdf` is the original target-state design reference for the full CartCo lakehouse (Bronze → Silver → Gold → Analytics, all five source channels, ML feature store, dashboards) — the same scope as `docs/B1_Design_Doc.pdf`. It was not, and was never intended to be, a diagram of what was actually running in Week 1.

**For the diagram of the current, verified working system, see:**

- [`deliverables/week2/architecture_diagram/current_state_architecture.md`](../../week2/architecture_diagram/current_state_architecture.md) — Bronze ingestion, GE quality gate, and Silver transform, all verified against a live Airflow run.
- The **Architecture** section of the root [`README.md`](../../../README.md), which is kept in sync with the current-state diagram above.
