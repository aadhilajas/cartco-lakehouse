> Implementation-stage reference. Pending statements below belong to the
> original stage. See the [documentation index](../README.md) and
> [current verification report](../test_report.md) for later acceptance, remote CI
> and the working public snapshot deployment.

# Runtime lineage and local failure-alert outbox

The versioned runtime now journals OpenLineage events and records local failure
alerts around the same callbacks used by its DAG and standalone CLI. Tests use
the pinned OpenLineage 1.11.3 client with its actual FileTransport. Live HTTP
acceptance, Marquez graph persistence and real Airflow callback dispatch remain
unverified because this host has no Docker/Linux runtime.

## What is instrumented

Each invocation of prepare, Shopify, Amazon, POS, inventory and publish receives
a new UUID. That invocation emits START followed by COMPLETE or FAIL, sharing
the UUID. A retry is a new observable invocation even if its business work reuses
an existing completion receipt or publication. It does not create a new data
version merely to generate lineage.

Job names are versioned_commerce_release.STAGE in the configured
OPENLINEAGE_NAMESPACE. Dataset namespaces use the existing Iceberg catalog
namespace for tables and actual local file-root URIs for staged/published files.

| Stage | Input/output evidence |
| --- | --- |
| prepare | Declared delivered CSVs, or explicit synthetic mode; frozen run manifest |
| each source | Frozen CSV -> raw event table and silver.order_state_v2 |
| inventory | Versioned state and captured inventory manifest when enabled |
| publish | Pinned versioned state, optional inventory -> exact immutable release artifact paths |

The cartco_runtime run facet carries stage, business batch date, orchestration
run ID, invocation UUID and a whitelist of evidence. Source evidence includes
batch/schema IDs, Bronze snapshot, receipt's state snapshot/table UUID, merge
counts and receipt reuse. Table outputs include schema and snapshot facets when
verified receipt evidence is available. A cached receipt reports reusedReceipt,
separately from whether its original receipt recorded a committed state change.

Publication evidence identifies its actual release/publication ID, pinned state,
required sources, artifact row counts and inventory quality counts. Completed
publication outputs use immutable release paths rather than implying that
CURRENT.json is an immutable dataset. START/FAIL dataset declarations can be
planned targets; absent snapshot facets are not proof of a completed write.

A task can fail after its publication has already committed, for example when
its result acknowledgement is lost. If the current verified release belongs to
that run, the FAIL event records committed_before_task_failure and its release
ID. Operators must not assume every failed task left data unchanged. Existing
runtime retry/old-pointer guards still govern recovery.

Custom facet schemas are provided in
[cartco-observability-v1.json](../schemas/cartco-observability-v1.json), identified
by a stable local URN. No unpublished public schema URL is claimed. Tests validate
serialized custom facets against this schema. This is not a claim that Marquez
has accepted or indexed those facets.

## Journal and delivery status

Each run retains observability/INVOCATION-STATE.event.json and a matching
.delivery.json under its frozen run directory. The event is serialized using
the pinned client's Serde before any client call. Its delivery record includes
the event checksum and one of:

- disabled: no lineage endpoint/client configured;
- unavailable: a configured client could not be obtained;
- client_completed: emit() returned without error;
- failed: client construction/access/emission raised, recorded by exception class.

The inspector reports pending when a journal event has no delivery record.
client_completed alone does not prove an HTTP request, backend persistence or
graph correctness: clients can use file transports or filters. The local proof
uses FileTransport and performs no HTTP requests. Compose still configures the
existing local Marquez endpoint for eventual live tests.

The journal is API-immutable with checksum-linked delivery records, not signed
or WORM storage. It does not automatically replay an event after failed delivery.
Endpoint failures, malformed event metadata, missing journal permissions or
instrumentation bugs are best-effort: they cannot replace the original task
exception, turn successful data work into failure, or cause another data commit.
Evidence collection failures are explicit evidenceErrorType metadata rather than
fabricated snapshot/release facts.

The legacy emitter now also catches event-construction failures and accurately
logs disabled/unavailable/failed client outcomes. It no longer logs an unconditional
"emitted" success when delivery was skipped or failed. Legacy jobs do not gain
the versioned runtime's durable journal automatically.

## Failure alerts

WORK_ROOT/alerts contains local cartco-alert.v1 JSON records. No email, Slack,
webhook or other notification is sent, and no remote recipient is configured.
The outbox is intended for inspection and a future explicitly configured alert
consumer, not a claim of operational paging.

- Stage exceptions create warning-level stage_failure records with invocation ID.
- The DAG's on_retry_callback records retry_scheduled with the reported Airflow
  try number.
- Its on_failure_callback records terminal task_failed, including failures such
  as timeouts where a Python body may not finish its journal event.

Repeated scheduler callbacks for the same task/run/reported try/event kind reuse
the same alert identity. Duplicate callbacks do not generate duplicate records.
Stage-attempt warnings and terminal scheduler failures are separate facts. A
reported try number is diagnostic scheduler context, not a source data version.
Alerts/journal facets retain error class, not exception text or raw CSV/customer
values. Normal Airflow/Python traceback logs can contain exception details and
retain their usual access-control requirements.

Alert persistence/callback failures are also best-effort. If a process, machine
or filesystem dies before recording anything, this local outbox cannot promise
delivery. Orphan START events remain visible; a callback does not invent a
missing task-body completion. Durable external paging, incident acknowledgement,
escalation and automatic resolution are not implemented.

## Inspect a retained run

```sh
python scripts/check_runtime_observability.py --work-root LOCAL/runtime --batch-date 2026-07-25 --run-id local-demo
```

The command is read-only and makes no network requests. It verifies event/delivery
checksums, custom facet schemas, run/stage identities, invocation pairing and
alert records. It reports delivery outcomes, stage/event counts, open invocations,
evidence errors and historical alerts. backend_graph_verified and notifications_sent
remain false: inspection cannot establish facts it did not observe.

--require-clean is an acceptance check for all six completed stages, no historical
failed attempts/alerts and no evidence/integrity/pairing gaps. It is deliberately
conservative: a recovered incident still appears in history and requires review;
this flag is not an automatic "current task health" or incident-resolution engine.
--require-client requires all recorded emit() calls to complete, which still does
not prove Marquez graph persistence. Missing journals never count as verified.

Prepared CI now captures the inspector report after the actual versioned DAG
passes. Backend delivery outcomes remain separate from data acceptance; live
Marquez HTTP/query/facet acceptance still requires dedicated evidence. The schema
directory is read-only mounted in the Airflow containers for inspection.

## Local proof and pending work

The retained 120,000-record runtime demonstration was retried twice with the
actual pinned client/FileTransport. It produced 24 schema-validated events,
paired across 12 invocations, while preserving the same release, three state
snapshots and three source completion receipts. Tests also cover source failure,
sanitized alerts, duplicate scheduler callbacks, client/storage outages, evidence
tampering and publication-committed-before-task-failure recovery.

Actual DagBag callback serialization/dispatch, Marquez HTTP/graph verification,
worker termination/restart, journal/alert retention, external notification
consumers and cross-host durable monitoring remain pending. No remote activation
or notifications were performed.
