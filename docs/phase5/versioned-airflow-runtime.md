# Versioned Airflow runtime: definitions and local callback proof

The new versioned_commerce_release DAG wires the three-source pipeline to one
coordinated Gold publication. Its exact callbacks are tested with real local
SQLite/Iceberg/Parquet files. Real DagBag imports, Airflow operator execution and
REST/MinIO/container acceptance are still pending. Docker and Podman are absent
on this host, and WSL is not installed. No native Windows Airflow installation,
system software installation or remote CI run was attempted.

## DAG and defaults

```text
prepare_run -> ingest_shopify -> ingest_amazon -> ingest_pos
            -> prepare_inventory -> publish_gold
```

The DAG is paused on creation, has no schedule, catchup=false and max_active_runs=1.
Its timezone is Asia/Kolkata. Tasks use all-success dependencies, two retries,
one-minute retry delay and a 30-minute execution timeout; the DAG run timeout is
90 minutes. XCom returns contain only small references and publication summaries.
The shared data volume holds actual files and receipts.

Every task receives the same run_id and business batch date. Supply an explicit
batch_date in manual run configuration; otherwise Airflow's logical ds is used,
never the execution host's current date. Run IDs are hashed before becoming path
components. The per-run directory is WORK_ROOT/runs/DATE/HASH.

| Compose setting | Default / effect |
| --- | --- |
| CARTCO_RUNTIME_MODE | delivered; synthetic requires explicit opt-in |
| CARTCO_RUNTIME_INVENTORY_MODE | none; optional delivered or synthetic |
| CARTCO_RUNTIME_ALLOW_EMPTY_INIT | false; literal true is required to initialize a new empty catalog |
| CARTCO_RUNTIME_ROWS_PER_SOURCE | unset; synthetic-only override of 1..1000000 rows per source |
| CARTCO_RUNTIME_WORK_ROOT | /opt/airflow/data/versioned |
| CARTCO_RUNTIME_INPUT_ROOT | /opt/airflow/data/deliveries |

Settings are captured when a run is first sealed. Retrying the same run reuses
the original inputs and decisions; changing environment variables does not
authorize rewriting its data or changing its initialization/inventory mode.
Use a new run ID for corrected deliveries or changed configuration. Modes and
initialization permission are deployment environment settings, not arbitrary
paths or code supplied through dag_run.conf. Default catalog access is REST/S3
using the existing endpoint/credential environment variables. No SQLite or
SQLAlchemy 2 dependency is added to the Airflow image.

## Delivered inputs

Place all source deliveries and explicit sidecars under INPUT_ROOT/DATE:

```text
2026-07-25/shopify.csv
2026-07-25/shopify.csv.contract.json
2026-07-25/amazon.csv
2026-07-25/amazon.csv.contract.json
2026-07-25/pos.csv
2026-07-25/pos.csv.contract.json
```

See the [source time contract](../phase2/source-time-contracts.md) and
[POS schema](../phase3/pos-source.md). prepare_run reads each file once, validates
compatible source windows/timezones and seals copies and checksums in run.json.
A missing delivery fails preparation; it is never silently generated or treated
as empty. Editing the original delivery after sealing cannot change the run.
Editing a frozen copy causes a checksum failure rather than automatic repair.
The manifest and archive provide API-level immutability, not WORM storage.

Source stages validate archived canonical events and use the existing versioned
merge engine. Completed receipt references are persisted and verified on retry.
A lost task acknowledgement after a state commit is recovered by source-version
replay without another state snapshot. Invalid source data stops downstream
tasks. Earlier successful source commits may remain in Silver, but Gold does
not advance until all receipts pass publication gates. This is not a distributed
transaction spanning all sources.

Empty-state permission never bypasses a nonempty legacy orders table, an existing
v1 state table or an unbound/differently zoned v2 table. Run the explicit reviewed
migration or separate-catalog replay workflow first; the DAG does not migrate,
drop, reset or overwrite existing tables to make a run succeed.

## Inventory transport

Mode none publishes four commerce artifacts. Mode delivered reads the inventory
bundle at INPUT_ROOT/DATE/inventory after source stages complete, validates its
exact state snapshot/timezone, and freezes the original bundle bytes and hashes
inside the run directory. Corrected external files cannot alter an already
accepted inventory stage. Missing or stale inventory fails the stage and can be
retried after a matching bundle is supplied. There is no delivery-wait sensor.

A practical delivered-inventory sequence is to run commerce-only first, obtain
that state snapshot, supply a matching externally valued inventory bundle, then
run the same source deliveries under a new run ID with inventory mode delivered.
Exact source replays preserve the state snapshot. See the
[inventory contract](../phase4/inventory-turnover.md).

Synthetic inventory is allowed only with explicit synthetic source mode. It
invents labelled cost/stock fixtures for the seven business dates preceding the
logical batch day. It never claims actual source inventory or accounting COGS.
Interrupted fixture staging can resume into the same immutable copy; failed
candidate directories remain for investigation/retention rather than being
silently deleted.

## Publication and retry behavior

Gold validates all three source receipts, snapshot ancestry, windows, contracts
and optional inventory before advancing WORK_ROOT/gold/CURRENT.json. Each run
adds a publication_id derived from its frozen manifest. Before publishing, it
persists an intent with the prior pointer checksum; that precondition is checked
inside the publisher's exclusive lock. If another publication intervenes, the
old intent fails instead of replacing the newer pointer.

If publication succeeded but the task result was lost, retry recognizes the
current release's run identity, verifies consumer checksums and persists the
missing task result without another publication. Retrying an old completed run
after a newer release fails without rolling the pointer backward. New run IDs
are distinct publication provenance, even when source merges are exact no-ops.
Manual CLI writers must still coordinate with Airflow; this is a shared local
filesystem/catalog model, not a distributed writer lease or cross-host lock.

## Local callback demonstration without Airflow

Use the isolated requirements/file-integration.txt environment, not the Airflow
environment, for the SQLite option. The following runs the same callback logic
against a new local catalog; it does not emulate or validate the Airflow scheduler:

```sh
python scripts/run_versioned_runtime.py --work-root ../work/runtime-new/r --input-root ../work/runtime-new/in --batch-date 2026-07-25 --run-id local-demo --mode synthetic --inventory-mode synthetic --allow-empty-init --local-catalog-db ../work/runtime-new/catalog.db --local-warehouse ../work/runtime-new/w
```

Add --rows-per-source 7 for a small smoke run. Repeat the exact command to exercise
run-level retry. Default full fixtures contain 50000/40000/30000 records. The
verified demonstration preserved 120,000 active records, three state snapshots,
three completion receipts and five artifacts across an exact run retry. It
reconciled 1,006 standard-cost allocations and 830 inventory pools for the chosen
seven-day period. This is local function/CLI evidence, not Airflow execution.

## Prepared live acceptance, once a Linux container runtime is available

Build the pinned images and run scripts/check_dags.py inside the Airflow image.
That checker imports all four real DAGs with DagBag and checks their graphs,
templates, serialization, manual/paused configuration, timezone and timeouts.
It has not been executed on this host.

For a fresh disposable stack, this explicit fixture command runs actual Airflow
operators while leaving the production/default settings unchanged:

```sh
docker compose exec -T -e CARTCO_RUNTIME_MODE=synthetic -e CARTCO_RUNTIME_INVENTORY_MODE=synthetic -e CARTCO_RUNTIME_ALLOW_EMPTY_INIT=true -e CARTCO_RUNTIME_ROWS_PER_SOURCE=50 airflow-scheduler airflow dags test versioned_commerce_release 2026-07-25T00:00:00+00:00
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/check_versioned_runtime.py --rows-per-source 50
```

The prepared CI workflow builds images, imports real DAGs, runs the versioned DAG
twice on a fresh stack, checks REST/MinIO state/raw snapshot coverage and published
evidence, then runs the legacy baseline and existing four live integration tests.
It uploads JSON/log/test evidence. Its volume-deleting teardown belongs only to
the disposable CI environment. None of these live CI steps ran or were pushed.

For scheduler-driven manual execution, configure the intended environment and
deliveries first, unpause only the new manual DAG, and trigger it with an explicit
business date and unique run ID, for example:

```sh
docker compose exec -T airflow-scheduler airflow dags unpause versioned_commerce_release
docker compose exec -T airflow-scheduler airflow dags trigger versioned_commerce_release --run-id delivered-2026-07-25-v1 --conf '{"batch_date":"2026-07-25"}'
```

This scheduler path still needs separate live evidence; dags test alone is not
scheduler/executor or operational recovery acceptance.

## Remaining scope

Structured runtime stage success/failure logs include run ID and business date.
Detailed lineage facets, verified Marquez delivery and failure alerts remain
pending. Scheduling, freshness SLAs, independent source watermarks, Kafka,
distributed coordination, backup/restore, stale-lock recovery, retention, real
inventory connectors and cloud deployment are also separate work. Legacy DAGs
and table consumers remain unchanged; no remote activation was performed.
