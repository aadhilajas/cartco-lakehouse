# ADR-010: Frozen runtime inputs and retry-aware publication

Status: Implemented and locally callback-tested; real Airflow/live acceptance pending
Date: 2026-11-11

Provide a separate manual/paused DAG for the versioned path rather than silently
switching the legacy DAGs and tables. Serialize its three source merges with
max_active_runs=1 and all-success dependencies before optional inventory and
one coordinated Gold publication. Logical business date and run ID are explicit
task arguments. Scheduling and source completeness/watermark policies remain
future decisions.

Seal source bytes, delivery contracts and initialization/inventory decisions
per run. New/corrected deliveries require a new run ID. Persist checksummed
completion receipt references outside XCom and reuse them on task retry. Lost
source-task results recover through source-version replay without another state
commit. Inventory inputs are captured after source completion and verified
against that exact state snapshot. Empty initialization and synthetic fixtures
are explicit opt-ins; migrations are never automatic.

Give each runtime publication a frozen-run identity. Persist a prior-pointer
checksum intent before publication and check it under the existing publication
lock. A lost result after successful pointer advancement can recover the current
release by that identity. An old run must not move a newer pointer backward,
including when the old run's success acknowledgement was lost.

Callbacks default to the existing REST/S3 catalog. Tests inject a real isolated
SQLite catalog; SQLAlchemy 2 is not installed into Airflow's SQLAlchemy 1.4
environment. The same functions are accessible through a local runner CLI.
Local proof does not establish DagBag/serialization, scheduler, executor,
REST/MinIO, cross-host locking or operational durability. Prepared live checks
must run in the actual Linux Airflow image before activation.

See the [runtime guide](../phase5/versioned-airflow-runtime.md) for defaults,
commands, recovery limits and the distinction between callback, dags-test and
scheduler acceptance evidence.
