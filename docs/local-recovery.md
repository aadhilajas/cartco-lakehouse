# Local recovery and startup hardening

The SQLite/file-warehouse recovery tool covers the isolated local integration
environment. It does not back up the REST service, MinIO, Airflow Postgres or
Marquez. Use the SQLAlchemy 2 environment in `requirements/file-integration.txt`;
do not upgrade Airflow's SQLAlchemy to run this tool.

Stop every writer before creating a backup. The tool also holds a SQLite write
transaction, inventories source files before and after copying, and refuses an
active publication lock. This is not distributed fencing: filesystem writers
must actually remain stopped. A failed candidate has no final manifest and must
not be used for recovery.

```text
python scripts/local_recovery.py backup --catalog-db <absolute-db> --warehouse <absolute-warehouse> --runtime-root <absolute-runtime> --catalog-name <exact-name> --output <new-backup-directory> --writers-stopped
python scripts/local_recovery.py verify --backup <backup-directory>
python scripts/local_recovery.py restore --backup <backup-directory> --catalog-db <original-db> --warehouse <original-warehouse> --runtime-root <original-runtime> --writers-stopped
```

The complete catalog, warehouse and runtime files are copied, including retained
snapshots, raw deliveries, receipts, inventory evidence, Gold artifacts and local
observability records. The manifest records sizes/SHA256 checksums plus table
UUIDs, schemas, current/retained snapshot IDs and current row multiset hashes.
Gold publication checksums are validated. Hashes detect accidental corruption;
this is not a signed or tamper-proof backup. Store a separate protected copy for
actual disaster recovery.

Restore requires all three original targets to be absent. Preserve damaged data
separately and verify the backup before recovery; the tool never deletes or
overwrites those targets. Iceberg metadata and runtime receipts contain absolute
paths, so automatic relocation is explicitly unsupported. A failed partial
restore remains for investigation; keep writers stopped until the audit passes.
Test recovery periodically in a disposable environment, preserving the original
data before simulating loss. The integration suite exercises historical snapshot
reads, receipt replay and five-artifact publication after recovery.

## Container catalog persistence

Compose now mounts `iceberg-catalog-data` at the pinned image user's existing
home and sets `CATALOG_URI=jdbc:sqlite:/home/iceberg/catalog.db`. MinIO object
files alone are insufficient to recover JDBC catalog table pointers. The pinned
amd64 image config confirms `JdbcCatalog`, the SQLite default, and user
`iceberg:iceberg`. The catalog served the live pipeline successfully; persistence
across container recreation still requires the check below.

**Existing installations:** preserve/export the catalog database from the old
container writable layer before switching URI. Starting with an empty named
volume does not migrate existing pointers. Never use `docker compose down -v`
against data you intend to retain.

Live acceptance still required: capture all table UUIDs, current/retained
snapshot IDs and row hashes through REST, recreate only `iceberg-rest` without
removing volumes, then compare through fresh catalog handles. Separately test
coordinated recovery of the JDBC database, MinIO objects, runtime data and service
databases. The local backup tool cannot substitute for this check.

`scripts/catalog_checkpoint.py capture --check-airflow-quiescent --output <new-file>`
records exact current row multiset hashes (including processing timestamps),
table UUIDs, schema, properties, metadata locations and retained snapshot IDs.
It checks for queued/running DAG runs and unpaused active DAGs before and after
capture. Standalone writers still require operator quiescence. `compare
--before <file> --after <file>` requires exact equality; equality alone does not
establish that a container was recreated. Preserve separate container-ID evidence.
Two fresh live captures matched across all 11 tables, and a deliberately changed
snapshot was rejected. These smoke checks did not restart the service.

## Live acceptance evidence

The 2026-11-11 host acceptance run passed all four live integration tests against
REST/MinIO. The versioned DAG processed 50 records per source and replayed with
three state snapshots, one Bronze snapshot per source and the same five-artifact
release. Legacy DAGs retained 90,000 Shopify/Amazon rows. These executions used
`airflow dags test`; healthy scheduler status is not evidence of scheduler
dispatch, worker restart behavior or recovery.

A subsequent REST-triggered Shopify run was dispatched by the actual scheduler:
all four tasks queued/executed successfully, the original paused state was
restored, all 11 table UUIDs/business row multisets were preserved, and unrelated
table snapshots were unchanged. Processing/ingestion timestamps were excluded
only from this legacy replay business comparison.

The exact successful frozen versioned run was subsequently requeued through the
stable API after preserving its task history. All six tasks completed under the
actual scheduler. All 11 table row hashes/snapshots and every Gold file/pointer
were unchanged, the original paused state was restored, and Marquez indexed all
six new invocation IDs with the same release. This verifies scheduler-managed
cached replay. Fresh-delivery scheduler ingestion and worker-crash recovery
remain separate acceptance requirements.

## Coordinated volume backup drill

The host recovery runner prepared alongside the local review outputs checks that
all DAGs are paused and no runs are queued/running, then stops the demo stack.
No independent CLI/filesystem writers may run during this procedure. It archives
the MinIO, JDBC catalog, Airflow Postgres and Marquez Postgres volumes plus the
shared data tree using Linux tar, preserving Linux ownership/modes. For each
archive it creates a new isolated drill volume, extracts the archive, then
compares canonical inventories including file SHA256, size, ownership/mode,
directories and symlink targets. Special files are rejected. Original volumes
are never overwritten or deleted, and the original stack is requested to resume
in a finally block even if an archive/restore step fails.

`scripts/volume_inventory.py` supplies the inventories. These checks do not
cover ACLs, xattrs, file capabilities, hard-link identity or timestamps. The
final backup manifest records archive checksums, inventory checksums, helper
image IDs and the bundled source-history checksum. Retain the archives and new
drill volumes for investigation. A failure before the final manifest leaves an
incomplete candidate. Resume errors must be resolved before normal operations.

The 2026-11-11 host drill completed all five archives/restores. Independently
verified inventories covered 20,288 entries: MinIO 17,179, catalog 5, Airflow
Postgres 1,507, Marquez Postgres 1,446 and shared data 151. All archive/source
bundle checksums matched, the original scheduler/database resumed healthy, and
all 11 original catalog checkpoints remained identical after restart.

Exact extracted files do not establish successful service boot from restored
volumes. `compose.recovery.yml` prepares a separate network/project using only
the restored external volumes, with no host ports and no scheduler. Its verifier
checks exact catalog checkpoints, Gold checksums, six successful restored Airflow
task records and the persisted Marquez graph. Runtime data mounts read-only.
The host runner checks backup hashes, restored-volume identities and helper
image IDs, starts only isolated services, records evidence and stops those
services afterward without deleting their containers/volumes.

The restored service boot check passed on 2026-11-11 for backup
`20261111172001`. Isolated restored MinIO, JDBC REST catalog, Airflow Postgres
and Marquez services booted successfully. Application verification preserved all
11 exact catalog checkpoints, the five-artifact Gold release, all six successful
versioned Airflow task records and the 18-node/18-edge persisted Marquez graph.
The saved Compose configuration confirmed restored external volumes only and no
host ports. All five isolated services stopped cleanly after the verification;
their volumes and archives remain preserved. The original stack was not stopped
or reconfigured by this boot drill.

Fresh pipeline processing against the restored stack, in-flight worker crash
behavior and an independently protected backup copy remain required.
The manifests are integrity evidence, not signed/WORM authenticity guarantees.

## Fresh processing and controlled worker kill (prepared)

The host restored-stack runner's `-WorkerDrill` mode creates five new isolated
volumes from the verified archives for every attempt. It does not reuse or
overwrite the previous restored-service drill volumes. The regular demo Compose
file does not mount the acceptance DAG. `compose.worker-drill.yml` adds only an
isolated scheduler and makes that fresh copy of runtime data writable.

The controller first verifies the restored baseline, then stages 25 explicitly
synthetic CSV rows per source with delivery contracts for the day following the
backed-up batch. The runtime uses delivered mode: the scheduler must freeze and
process these new files. Expected business state is prepared independently of
catalog writes using the canonical merge, excluding only processing timestamps
from that expected-state hash. The new run uses a separate work/Gold directory
and no inventory input, so it must publish four artifacts. The original backed-up
five-artifact release is retained byte-for-byte.

The acceptance-only POS task calls the real ingestion callback, writes a marker
after its commit/receipt completes and waits. The controller verifies DAG/run/task
identity, running state, matching Airflow PID and `/proc` start time before
sending one SIGKILL to that task process. It refuses mismatched or reused PIDs
and repeat signals. Airflow must retry the task and complete all six stages.
The post-commit checkpoint must remain exactly unchanged by that retry, the fresh
batch must add exactly three state snapshots and one Bronze snapshot per source,
and local evidence must contain the retry callback without a terminal alert.
Expected retry alerts remain part of history rather than being labelled clean.

This mode is implemented, Compose/PowerShell/Python checked, with seven focused
unit checks of comparison and signal guards. It has not run against Docker yet.
The host command captures diagnostics and stops only its isolated project in a
finally block. A successful result would cover one post-commit POS task kill;
arbitrary mid-commit crashes, scheduler/container loss and source outages remain
separate requirements. Archives, fresh drill volumes and reports are retained.

`scripts/check_marquez_runtime.py` independently reads the Marquez API to require
six completed indexed stage runs with the expected orchestration identity,
release/snapshot/count run facets, dataset versions, state schema fields and
source-to-state-to-publication graph edges. It passed against the live backend
with 18 graph nodes and 18 edges. It does not verify custom dataset facets or
backend persistence across restarts. For repeat verification, provide the exact
runtime acceptance summary and orchestration run ID:

```text
python scripts/check_marquez_runtime.py --expected <versioned-runtime.json> --orchestration-run-id <exact-run-id> --output <report.json>
```

This check is read-only, assumes the local demo namespace/storage names, and
fails if latest indexed jobs refer to a different orchestration run. Preserve
the report with its raw API responses for review. It does not rewrite historical
client delivery journals to claim that every historical event was indexed.

## Airflow initialization behavior

The initializer now runs `airflow db migrate`, requires a valid JSON user list,
preserves an existing user and propagates migration/list/create failures. It no
longer masks every error with `|| true`. It retains the official image entrypoint
for arbitrary Airflow UID setup. Defaults remain local demo credentials; the
script accepts `CARTCO_AIRFLOW_ADMIN_USERNAME`, `_PASSWORD` and `_EMAIL` in its
container environment. Existing users are preserved without changing roles or
passwords. A name match does not verify administrator privilege. No password
argument or captured CLI output is printed on failure. The initial live container
setup exited successfully and both Airflow services became healthy. Existing-user
restart and failure paths have function tests; repeat live initialization remains
a separate check.
