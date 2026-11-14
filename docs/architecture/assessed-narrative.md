# Assessed architecture narrative

CartCo's business boundary is one synthetic multi-channel retailer. The project
does not onboard real merchants. Its source fixtures exercise GraphQL, REST,
Kafka and SSH/SFTP while keeping identities, prices and delivery windows explicit.
The source API, broker and SFTP fixture are separate containers. Kafka is consumed
as a bounded microbatch, not a continuous low-latency streaming platform.

Airflow exposes private operator controls and schedules four meaningful DAGs:
order ingestion, inventory ingestion, commerce release and system health. The
release DAG waits for durable source manifests. Raw bytes, accepted contracts and
checkpoint receipts are retained before source acknowledgement. SFTP's entity is
inventory, not an invented fourth order channel. Source API credentials and raw
payloads never belong in the public frontend.

Spark runs in a separate Python environment inside the bounded task container.
It executes real DataFrame casts, source-version windows, aggregates and joins.
The main Airflow dependency environment remains independently locked. There is
no Docker socket mount, remote shell executor or anonymous pipeline trigger.
Single-node `local[2]` is sufficient for this demonstration; a distributed cluster
would add cost and operational scope without changing the assessed semantics.

New date-partitioned Bronze tables, source-versioned Silver state and immutable
Gold release tables use Iceberg on MinIO. Nessie is the listed catalog and stores
version history through JDBC2 in its dedicated PostgreSQL database. The earlier
REST/SQLite baseline is preserved in separate resources rather than silently
migrated. Tombstones remain in state so old events cannot resurrect deletions.
An existing state's identity must not disappear and become an empty initialization.

Independent Python/PyArrow reference calculations reconcile Spark's business
outputs. Seven Great Expectations suites then check the persisted candidate.
Gold table commits are independent, so a release manifest pins their exact
identities and one atomic CURRENT pointer exposes the coherent result. A failed
candidate or export leaves the prior public snapshot intact. This is a publication
protocol, not a claim of ACID transactions across multiple tables.

Column derivations are emitted to Marquez and checked against its actual backend
schema and graph. Grafana reads a minimal, read-only PostgreSQL view of actual
Airflow run records. It cannot read raw DAG configuration or modify pipeline
metadata. The React Business, Engineering and Lineage views consume only a safe
snapshot served by a read-only API. Exact decimal strings, absent values and
distinct source/compute/export timestamps prevent invented precision or freshness.

## Public deployment versus local services

The public demonstration uses Vercel to serve a validated snapshot and GitHub
Actions for real on-demand scheduled processing. The runner stack stops after
acceptance/publication; Vercel does not host the data/admin services continuously.
Two successful publication runs and the public release match are recorded in the
[deployment checkpoint](../validation/public-demo-checkpoint.json). See
[ADR-016](../adr/ADR-016-public-snapshot-demo.md) for the decision and boundaries.

Native, scheduled-container, local cold recovery and public deployment checks are
separate evidence. A persistent server would require its own storage/recovery,
capacity and security qualification. Pinned amd64-only Marquez images are not
qualified for ARM merely because other bases are multi-architecture. The project
introduces neither Kubernetes nor a paid service.

Diagram sources are `assessed-containers.dot` and `assessed-components.dot`.
Generate SVG/PNG with Graphviz (`dot -Tsvg`, `dot -Tpng`) and inspect the outputs
before publishing them. They intentionally distinguish logical components,
runtime containers and persistent stores.
