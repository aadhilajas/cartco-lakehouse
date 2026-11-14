# Historical reference: baseline test report

> Preserved from the documentation before the 14 November 2026 navigation cleanup.
> Pending statements and architecture details below describe the earlier application,
> not the current project status. Start with the [documentation index](../README.md).

# CartCo verification report

Updated 2026-11-11. This replaces the historical Shopify-only Week 3 report.

| Evidence | Result | Boundary |
|---|---|---|
| Unit suite | 278 passed, 1 POSIX-only FIFO skip on Windows | Actual pinned Python dependencies |
| Real-file suite | 99 passed | Separate SQLAlchemy 2 / SQLite catalog environment |
| Strict live integration | 4 passed, no skips | Actual REST/MinIO/Airflow container stack |
| DAG imports | Four normal DAGs imported and serialized | Legacy source/Gold and versioned six-task DAG |
| Scheduler | Shopify dispatch and six-stage versioned replay succeeded | Replay retained original data/Gold bytes |
| GE output suites | Seven live suites passed | Exact retained Silver snapshot + five Gold artifacts; isolated new release persisted results |
| Lineage | Typed column facets serialized and live graph queried | 101 source/state/Gold output-field checks, schemas and row counts |
| REST persistence | 11 exact checkpoints survived container replacement | Container ID changed, volumes preserved |
| Volume restore | Five archives / 20,288 inventory entries matched | Checksums, ownership/modes and file contents |
| Restored application | 11 tables, five artifacts, six task records and persisted graph passed | Isolated restored services |
| Fresh/crash drill | 75 new synthetic records; six tasks succeeded after POS kill/retry | Post-commit SIGKILL only; exact catalog unchanged by retry |

GE 0.18.19 validates state keys/source versions/deletion flags, active business
keys/status/currency/ranges, Gold grouping uniqueness, nonnegative counts/money,
rates and explicit inventory coverage states. GE never converts persisted money
to float. Custom exact decimal, source-receipt, snapshot and inventory
reconciliation checks remain mandatory. A negative/duplicate Gold candidate
regression proves GE rejection retains the old CURRENT pointer and failure report.

Column-lineage tests cover native POS identity, source normalization, prior-state
merges, completed-sales filters, aggregation keys and inventory costs/stocks.
Lineage/instrumentation remains fail-open; business/DQ publication remains
fail-closed. A client emit returning is distinguished from backend indexing.

Historical evidence also includes a retained 120,000-row local-file run with
Shopify 50,000 / Amazon 40,000 / POS 30,000. It is a separate fixture from the
150-row live versioned demo and 225-row isolated recovery/crash drill. These
counts are not combined into a throughput benchmark.

Evidence summaries are packaged in `deliverables/week4/evidence/acceptance.json`.
Raw host logs/JUnit/checkpoints/archives are retained in the local review outputs.
No final remote GitHub Actions result, cloud deployment, independent/offsite
backup, arbitrary mid-commit crash or whole-scheduler failure is claimed.
