# Exact money and currency in versioned state

The local versioned path now writes `silver.order_state_v2`. `unit_price` and
`order_total` are Iceberg decimal(18,2), and each live row requires `currency=INR`.
Tombstones retain their keys and source versions and clear business payload,
including currency. Legacy `silver.orders` and v1 `silver.order_state` are not
changed. This is a new table, not an unsupported Iceberg double-to-decimal cast.

## Contract

- Source CSV live events must explicitly include INR currency. Other currencies
  and missing currency are rejected after raw archival, before state writes.
- Prices and totals must be finite, nonnegative, exact cents, below 10^16.
  Fractional cents are rejected; ingestion never silently rounds them.
- Quantity is a nonnegative integer. Total equals quantity times unit price.
- Canonical APIs accept Decimal, integer or decimal strings, never binary floats.
- Generators with `--source-versions` now emit currency. Their default legacy
  output remains compatible with the existing DAGs.
- New Gold release artifacts keep money decimal throughout aggregation and
  serialization. Legacy Gold table functions still default to their double
  schemas and reject totals that cannot preserve cents.
- AOV is rounded to cents with ROUND_HALF_UP; persisted source prices and line
  totals are never rounded. No FX conversion, tax or discount inference occurs.

INR is the explicit synthetic source contract, including Amazon marketplace
labels. A marketplace label does not imply FX conversion or verified real-world
currency. Currency supplied during migration is a caller declaration.

## New local catalog

Use the isolated `requirements/file-integration.txt` environment for the local
SQLite CLI. Generate source-versioned CSVs, then initialize deliberately:

```sh
python scripts/dev.py generate --batch-date 2026-07-25 --source-versions
python scripts/versioned_orders.py --catalog-db local/catalog.db --warehouse local/warehouse run --source shopify --csv input/raw/shopify_orders.csv --archive-root local/archive --batch-date 2026-07-25 --initialize-empty-state
```

Run Amazon with the same date and its CSV, omitting the initialization flag.
Use the resulting v2 completion receipts for `scripts/gold_release.py`.
An existing v1 table blocks empty initialization, even when it has no rows.

## Existing v1 catalog

Stop v1 writers and keep them stopped through cutover. Preserve a catalog and
warehouse backup. Inspect the currency authority before declaring INR:

```sh
python scripts/versioned_orders.py --catalog-db local/catalog.db --warehouse local/warehouse migrate-money --currency INR
```

Migration reads a pinned v1 snapshot and creates v2 in `initializing` status.
It preserves source versions, tombstones and audit times, verifies all values
and the persisted read-back, and checks the original metadata has not changed
before marking v2 ready. Provenance records v1 UUID, snapshot, metadata location
and currency declaration. V1 is never rewritten or deleted. Existing v2 always
causes refusal, including a partially initialized migration.

Legacy floating-point values are interpreted via their shortest decimal string;
they must already represent exact cents and satisfy line-total reconciliation.
For example `0.1` converts to `0.10`, while `0.30000000000000004` is rejected.
Lost historical precision cannot be recovered by a schema migration. Reconcile
such rows against trusted source evidence before preparing a new baseline;
do not automatically round them or invent a newer source version.

Re-ingest explicit-currency source batches to obtain v2 receipts. Old receipts
refer to the v1 table UUID and cannot authorize a v2 Gold release. A same-version
source replay must match the migrated payload exactly. Migrating state alone
does not publish Gold or move its `CURRENT.json` pointer.

If a migration fails after creating v2, it stays initializing and ingestion and
publication are blocked. Retain both tables for investigation; no automatic
drop, retry replacement or lock recovery is implemented. If v1 changes after
the final check, this copy cannot detect it: stopping writers is an operational
precondition, not a distributed lock. To roll back, stop v2 writers and restore
the previous application revision targeting v1; reconcile post-cutover changes
explicitly. The previous Gold release remains readable through its own manifest.

## Legacy orders without trusted source versions

Use the existing reviewed-candidate workflow with the new required
`--currency INR` option on `scripts/prepare_order_state.py`. A complete trusted
key/version map is still required. The candidate targets v2; installation
refuses bypassing an existing v1 table. Legacy source data is preserved.

## Scope still pending

This implements the local persisted money/currency contract. Business timezone,
source extraction intervals and schema versions remain separate work. The
legacy Airflow path still writes its original tables. Docker/REST/MinIO/Airflow
acceptance, distributed writer coordination, operational migration recovery,
and legacy consumer cutover remain pending. No remote deployment is activated.
