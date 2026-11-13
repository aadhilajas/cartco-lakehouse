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

The container view is a deployment specification; native acceptance and expanded
scheduled-container acceptance are separate evidence. Hosted operation still
requires a real free account/capacity, compatible architecture, protected admin
access, HTTPS and persistence/recovery checks. In particular, amd64-only Marquez
images cannot be treated as qualified ARM images merely because other bases are
multi-architecture. The project introduces neither Kubernetes nor a paid service.

Diagram sources are `assessed-containers.dot` and `assessed-components.dot`.
Generate SVG/PNG with Graphviz (`dot -Tsvg`, `dot -Tpng`) and inspect the outputs
before publishing them. They intentionally distinguish logical components,
runtime containers and persistent stores.
