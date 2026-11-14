> Implementation-stage reference. Pending statements below belong to the
> original stage. See the [documentation index](../README.md) and
> [current verification report](../test_report.md) for later acceptance, remote CI
> and the working public snapshot deployment.

# Immutable source batches and the local versioned ingestion path

Status: implemented and tested locally on 2026-11-10. Legacy DAGs and Gold readers
are unchanged. This explicitly selected path writes separate Bronze event tables
and silver.order_state_v2; it does not publish a new Gold release.

## Execution sequence

1. Read the input bytes once; compute SHA-256 and a batch ID from source, load
   date, checksum and the explicit delivery contract. Preserve original bytes under source/date/batch-ID.
2. Publish a manifest containing that identity, checksum, byte count, headers and
   row count. Original files/manifests are never overwritten by the archive API.
   Malformed CSV bytes are retained with a parse-error manifest.
3. Land each raw record, including unknown source columns, as JSON in a separate
   Iceberg Bronze table. Partition by batch ID; replays verify existing content.
4. Validate source keys, versions, delete flags, required live-order fields,
   statuses and numeric bounds. Reject the whole batch on failure. Raw evidence
   stays present; canonical state is not changed.
5. Merge validated canonical events into ready versioned state. Persist an
   append-only processing receipt with raw/state snapshot IDs, counts and status.

Receipts follow the data commit; they are not a distributed transaction with the
catalog. A receipt failure fails the attempt, and source-version replay can
recover a previously successful commit without duplicating business data.

The manifest now includes the explicit extraction window, source schema version
and timezones; see [source time contracts](source-time-contracts.md). Controlled
Gold release manifests are implemented separately; external artifact retention
policies remain future work. Content addressing is API-level immutability, not WORM storage;
manual changes are detected on replay rather than silently repaired.

## Source contract

Every versioned CSV needs order_id, source_updated_at and is_deleted headers.
Versions must be explicit ISO timestamps with a timezone. Delete flags are the
literal strings true/false. A key-only tombstone is allowed; a live event needs
the complete source order. Shopify status/field names and Amazon status/ASIN
names are conformed independently. Unknown raw columns remain in the archive
and raw Bronze payload.

An empty, schema-valid delivered file is explicitly recorded as zero events.
A missing file or a file with invalid headers is not a successful empty batch.
Source validation is implemented in the versioned adapter; the legacy GE
checkpoints still apply to the legacy DAGs. Silver/Gold output gates remain
separate work.

Synthetic versioned fixtures are opt-in:

```bash
python scripts/dev.py generate --batch-date 2026-07-25 --source-versions
```

The generator declares the snapshot's reference timestamp as its synthetic
source version. It does not invent versions for legacy rows. Different reference
dates remain distinct synthetic scenarios; updates/deletes are explicit events
against existing IDs, not inferred from absence in a later generated scenario.

## Run locally without Docker

Activate the separate Python 3.11 environment installed from the hash-pinned
requirements/file-integration.txt. Use a short warehouse path on Windows because
PyArrow 15 cannot write every long hash-partition path beneath a deep temp root.

```bash
python scripts/versioned_orders.py --catalog-db ../work/versioned/catalog.db --warehouse ../work/vw run --source shopify --csv data/raw/shopify_orders.csv --contract data/raw/shopify_orders.csv.contract.json --archive-root ../work/archive --batch-date 2026-07-25 --initialize-empty-state
python scripts/versioned_orders.py --catalog-db ../work/versioned/catalog.db --warehouse ../work/vw run --source amazon --csv data/raw/amazon_orders.csv --contract data/raw/amazon_orders.csv.contract.json --archive-root ../work/archive --batch-date 2026-07-25
```

The initializer is explicit and refuses a catalog containing nonempty legacy
silver.orders. It cannot bypass the required version-map migration.

## Install a migration candidate

First prepare a candidate using the trusted complete version map described in
[versioned order state](versioned-order-state.md). Then point at the local catalog
that actually contains the exported legacy orders:

```bash
python scripts/versioned_orders.py --catalog-db ../work/versioned/catalog.db --warehouse ../work/vw migrate --candidate candidate.parquet
```

Installation validates the candidate checksum/schema and reconciles its business
rows to the current legacy table before creating a separate state table. It
refuses an existing target. The target starts as initializing; state writers
cannot use it until verification finishes. If legacy changes during migration,
the target remains initializing and ingestion is blocked. Legacy is never
overwritten, and its snapshot remains the rollback source.

Run migrations with legacy ingestion stopped. A failed initializing target needs
explicit diagnosis/recovery; the tool does not delete it or reset source data to
force another attempt. Gold activation is a separate publication step.

## Verified evidence

- 120 unit tests passed in the existing runtime environment.
- 30 real-file Iceberg tests passed in the isolated SQLite catalog environment.
- Full-size CLI run: 50,000 Shopify and 40,000 Amazon orders landed in state.
- Replaying Shopify reported 50,000 unchanged orders and created no state commit.
- The demonstration retained two source manifests, three attempt receipts and
  two state snapshots; it completed in 21.8 seconds on this host.
- Tests cover source rejection, retained raw evidence, concurrent archive
  publication, migration drift, source updates/deletes and stale replays.

This proves the local CLI/API path with real Iceberg files and SQLite metadata.
REST/MinIO, Airflow execution, container builds, Gold reconciliation and runtime
switch-over are still unverified. No remote CI was started and nothing was pushed.

Source live rows now require explicit INR currency and exact-cent prices. See [money-state v2](money-state-v2.md) before running against an older catalog.

The run CLI requires `--contract` with the matching generated sidecar. Existing unbound v2 tables require an explicit rebuild; see the time-contract guide.
