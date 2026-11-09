# Phase 2: explicit batch execution and safe failure boundaries

Status: first implementation slice; source-versioned merging and full Phase 2
acceptance are pending. Phase 1 Docker/live checks also remain pending. All work
stays local; no branch, PR or CI run is published.

## Implemented

- `pipeline.batch.resolve_batch_date` chooses an explicit argument, then
  `CARTCO_BATCH_DATE`, then UTC today for standalone compatibility. Invalid or
  ambiguous date strings fail; they never silently select a different partition.
- Both source DAGs pass `{{ ds }}` to every stage. Each source permits one active
  DAG run to avoid overlapping writes to its shared synthetic input file.
- Bronze stamps the selected load date, GE scans that exact Bronze partition,
  and Silver consumes the same partition. The local runner resolves its date
  once for the complete command. This date is a load partition, not a completed
  business-timezone or financial recognition policy.
- Generators accept an explicit reference date. Seeded instance-local Faker,
  NumPy and UUIDv5 order IDs reproduce every source column and CSV byte for a
  fixed date/seed. IDs include source/date/seed/index; different dates are
  separate synthetic scenarios, not source change-data-capture events.
- Silver chooses the latest original order timestamp before truncating to a
  date. This fixes same-day deduplication; source update timestamps remain the
  required future update-ordering contract.
- Missing Bronze batches, all-rejected Silver output, invalid source channels
  and null partition dates fail before a Silver write. Bronze/Silver reload
  metadata before row-count verification. A mismatch fails the task rather
  than emitting COMPLETE. GE read failures now emit FAIL.

## Guard against destructive legacy overwrites

The current Iceberg writer still replaces a date range within one channel.
Before every attempt it reads the fresh channel state and verifies that:

1. Every existing order inside the replacement range is retained.
2. An existing key has identical business values, including its order date.
3. Keys are unique and dates/channels are valid.

New keys and exact business replays are allowed; a new audit timestamp is fine.
Deletes, changed orders and moved order dates are rejected with
UnsafeSnapshotError until a source-versioned merge is implemented. This is a
fail-closed safety gate, not incremental upsert support. It reads a complete
channel and is deliberately limited to the current in-memory workload. Iceberg
concurrency/commit behavior still needs live acceptance tests.

Existing synthetic tables created with random order IDs may be rejected by this
guard when a newly generated scenario overlaps their dates. Preserve those
tables and plan a deliberate migration; deleting data to make a rerun pass is
not a migration strategy.

## Commands

Use the activated Python 3.11 environment:

```bash
python scripts/dev.py generate --batch-date 2026-07-25
```

With a disposable Docker test stack, the prepared sequential demonstration is:

```bash
python scripts/dev.py run-local --batch-date 2026-07-25
```

Standalone source `run(batch_date="2026-07-25")` calls and an exported
CARTCO_BATCH_DATE are also supported. A historical load can be selected, but
updating an existing Silver business record remains intentionally blocked until
source-versioned merge support exists. No historical backfill acceptance is
claimed from these local unit checks.

## Local verification

Regression tests cover date precedence/validation and midnight boundaries,
fixed-date generation, exact-batch selection, source read failures, same-day
deduplication, fresh verification reads, missing/all-rejected batches, read-back
mismatches and rejection of destructive range replacements.

The catalog double captures immutable handles specifically to catch stale reads;
it does not implement or validate real Iceberg overwrite semantics. Docker-based
source DAG import/execution and real writes remain unexecuted on this host.

Both full-size generators (50,000 Shopify + 40,000 Amazon rows) were executed
twice through the local CLI for 2026-07-25. SHA-256 hashes matched for each CSV.

Full local suite: 86 passed, four live tests skipped, 103 upstream warnings in
14.76 seconds. All 41 Python files parsed successfully; the shared pipeline
Compose mount and the batch-date CLI were checked. No new dependency was added.

## Next implementation work

Immutable raw inputs and manifests; source_updated_at and explicit tombstones;
Iceberg schema migrations and a versioned atomic merge; source business-timezone
and currency/decimal contracts; legitimate empty-source manifests; persisted
quality results and a common publication snapshot. Gold metrics remain unchanged.
