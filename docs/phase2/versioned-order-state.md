> Implementation-stage reference. Pending statements below belong to the
> original stage. See the [documentation index](../README.md) and
> [current verification report](../test_report.md) for later acceptance, remote CI
> and the working public snapshot deployment.

# Source-versioned order state

Date: 2026-11-10. Status: implemented and tested locally; staged separately from
the existing source pipelines and Gold publication. No remote changes are made.

The [immutable source-batch path](source-batches.md) now connects CSV events to
this engine through a local CLI/API and installs verified migration candidates
in a separate state table. The legacy Airflow/Gold path remains unchanged.

## Contract

`pipeline/order_state.py` merges canonical events by `(channel, order_id)` and
an explicit `source_updated_at` timestamp. Source versions are canonical UTC
timestamps; the in-memory/Iceberg timestamp representation is naive UTC.

- A newer source version replaces the stored version.
- An older version is ignored, including after deletion.
- Equal versions with equal payloads are no-ops and preserve the original audit
  timestamp. Conflicting equal versions abort the entire batch before a commit.
- Duplicate events within a batch collapse deterministically to the newest
  version. Conflicting duplicate versions are rejected, even if another newer
  event is also present.
- A live upsert is a complete canonical order, not a partial field patch.
- A tombstone needs the key, source version and `is_deleted=true`. It remains in
  state with null business values so an old replay cannot resurrect the key.
  A genuinely newer live version can explicitly reactivate it.
- The same source order ID in two channels represents independent keys.

The state schema preserves the eleven existing business/audit field IDs and
adds `source_updated_at` (12) and `is_deleted` (13). Customer/date fields are
nullable in state for tombstones; live events still require customer/date.
`active_orders()` produces the eleven business/audit columns plus currency and
excludes tombstones. It does not publish or overwrite legacy `silver.orders`.

Money fields now use decimal(18,2), with currency field ID 14. See the
[money-state v2 guide](money-state-v2.md) for the exact-cent contract and explicit
v1 migration. This changes the default local state target to `silver.order_state_v2`.

## Writer and limits

`write_order_state()` requires an explicitly created state table, for example
`silver.order_state_v2`, with the exact state schema and identity channel partition.
It reads a fresh channel state, computes the merge, then uses one Iceberg
transaction to replace that channel. Unmentioned orders and other channels
remain present. This reconstructs one complete channel in memory; it is suited
to this prototype's data volumes and is not a distributed row-level MERGE.

Only `CommitFailedException` is retried, up to the supplied maximum (default
three). Every retry reloads metadata/data and recomputes the merge. Equal/stale
replays create no new commit. Successful writes are verified by exact content
against the committed snapshot ID, rather than a potentially later snapshot.
Ambiguous transport failures propagate; a caller can replay and discover that
the same version already committed. Orphan data-file cleanup remains a separate
maintenance concern after rejected commits.

## Migration preparation

Legacy rows have no trustworthy source version. The migration helper requires
an externally verified one-to-one version map with exactly these columns:

| Column | Meaning |
|---|---|
| channel | Existing source channel |
| order_id | Existing source order key |
| source_updated_at | Trusted source update version |

Missing/extra keys, duplicate keys, absent versions and incomplete legacy exports
are rejected. Order placement dates, ingestion dates and transformation audit
times are never substituted for missing source versions.

With a complete eleven-column legacy Parquet export and a trusted version map:

```bash
python scripts/prepare_order_state.py --legacy legacy.parquet --versions versions.parquet --output candidate.parquet --applied-at 2026-11-10T00:00:00Z --currency INR --business-timezone UTC
```

The tool prepares and verifies a candidate Parquet file plus a manifest with
input/output SHA-256 checksums and row counts. It refuses existing output paths,
keeps both input files unchanged, and makes no catalog connection. The supplied
version map's source authority must be established outside the tool.

Before activation, populate versioned source events, migrate into a separate
state table, reconcile the active projection to the original exports, and verify
REST/MinIO behavior and a controlled Gold publication boundary. Preserve the
legacy table as a rollback source. Existing pipelines retain their fail-closed
range-overwrite guards until that integration is completed.

## Tests without Docker

Use a separate Python 3.11 virtual environment:

```bash
python -m venv .venv-file-tests
# Activate the new environment before installing.
python -m pip install --require-hashes -r requirements/file-integration.txt
python -m pip check
python -m pytest tests/file_integration -v
```

The optional catalog uses SQLAlchemy 2.0; Airflow 2.9 uses SQLAlchemy 1.4. Do not
install the optional test lock into the Airflow image/environment. Shared runtime
dependency versions stay constrained to the existing test lock.

Real SQLite catalogs and local Parquet/metadata files prove replay, sparse-batch
preservation, cross-channel isolation, tombstones across catalog reopen,
all-or-nothing conflict rejection, actual interleaved commit retry, bounded
retries and safe replay after a lost response following a successful commit.
These checks exercise PyIceberg 0.7.1. They do not replace REST catalog, MinIO,
Airflow or container acceptance checks, which remain pending.

Verified locally on 2026-11-10: 110 unit tests passed in the existing runtime
dependency environment (110 upstream warnings, 12.95 seconds). Separately,
11 real-file tests passed in the isolated SQLAlchemy 2.0 environment (16 upstream
warnings, 3.54 seconds), and its pip check found no broken requirements. The four
Docker integration tests remain unexecuted. No remote CI run was started.
