> Implementation-stage reference. Pending statements below belong to the
> original stage. See the [documentation index](../README.md) and
> [current verification report](../test_report.md) for later acceptance, remote CI
> and the working public snapshot deployment.

# Corrected Gold metrics and controlled local releases

Status: implemented and verified locally. The new release path reads versioned
state and source completion receipts. It publishes local files through CURRENT.json;
it does not switch the legacy Airflow DAG or overwrite its independent Gold tables.

## Business definitions

| Output | Contract |
|---|---|
| daily_revenue | Completed-order sales/count/AOV by order date and channel. Cancelled, refunded and pending orders contribute no sales. |
| channel_performance | Completed sales; all active placed orders in the rate denominator; cancelled/refunded ratios. New artifacts separately expose placed, cancelled, refunded and pending order values. Fulfillment time is null without measured timestamps. |
| customer_360 | Completed purchases only for spend/count/recency/favorite category. Identity is scoped by channel. Favorite category uses unit quantity, then lexical name for ties. |
| category_unit_sales | Completed units by category/channel and the completed-sales reporting period. This is not an inventory turnover ratio. |

The legacy inventory_turnover function/table name is retained for compatibility,
but new releases use category_unit_sales and reporting_period. Actual turnover
requires cost-of-goods and inventory balances with matching coverage. The optional
[standard-cost inventory path](inventory-turnover.md) now adds a fifth artifact
with those explicitly supplied inputs, pinned to the same state snapshot.
The legacy SQL table remains unchanged.

These are latest-state retained sales under the existing canonical source status
mapping, not accounting recognition, partial-refund history or cash-flow reporting.
Tombstones are excluded. Unknown statuses block publication.

## Money and currency

The versioned release path requires [v2 state](../phase2/money-state-v2.md),
with decimal(18,2) source prices/totals and persisted INR currency. Ingestion
rejects fractional cents, missing currency and unsupported currency. Gold keeps
money decimal throughout grouping and artifact serialization, with AOV rounded
ROUND_HALF_UP to cents. Legacy table functions retain double output compatibility
and fail if their float string round-trip would lose cents; the new artifact
builder no longer uses that conversion.

INR is an explicit synthetic source contract or migration declaration, not FX
conversion or independent confirmation of a real marketplace's currency.

## Delivery time contracts

[Explicit source contracts](../phase2/source-time-contracts.md) are required in
archived manifests and completion receipts. Required sources must share the
same extraction window and business timezone, matching the state declaration.
The release contract records these declarations. Old uncontracted receipts cannot
authorize a new release; existing release artifacts remain readable.

The default required-source set is now Shopify, Amazon and POS. An intentionally
two-source catalog must explicitly request shopify/amazon; undeclared channels
still block publication. See the [POS guide](../phase3/pos-source.md).

## Validation and source readiness

- Validate canonical keys, uniqueness, dates, statuses, finite nonnegative amounts
  and line-total consistency before aggregation.
- Independently reconstruct expected daily/channel/customer/category groups and
  reconcile counts, cents, rates, recency, category ties and unit totals.
- Require exactly one successful original completion receipt per required source
  for the requested batch date. Verify its archived CSV/manifest checksum and raw
  Bronze coverage, state-table UUID and snapshot ancestry.
- Each source receipt must cover its latest state change. A fresh receipt for one
  source cannot conceal a stale receipt for another. Untracked state commits fail.
- Explicit schema-valid empty deliveries are accepted; missing sources are not.
- Pin one state snapshot for every mart and check for observed input drift before
  changing the release pointer.

## Publication protocol

A local exclusive lock serializes publishers. Marts are written to an immutable,
content-identified release directory, serialized back for verification, and given
a checksum/count manifest. Only after validation does an atomic filesystem replace
update CURRENT.json. Readers load that pointer, then verify manifest/artifact
checksums and row counts. An unpublished candidate is never a valid reader target.

Quality failures, missing/stale source receipts and observed state changes retain
the previous pointer. Repeat publication from the same snapshot/receipts/metric
version is idempotent. The lock is not automatically stolen after a crash; inspect
the process/candidate before explicitly recovering a stale lock.

The catalog and filesystem are not one distributed transaction. Ingestion can
advance after the final state check; the published release still identifies its
internally consistent pinned snapshot. Object-store/cloud publication, power-loss
durability, backup/restore and retention require separate operational validation.

## Local commands

Use the isolated requirements/file-integration.txt environment and keep the
original source receipt paths returned by scripts/versioned_orders.py. The
receipt files sit beside their immutable batch manifests; exported copies alone
are not sufficient publication evidence.

PowerShell example after generating versioned fixtures:

```powershell
$shop = python scripts/versioned_orders.py --catalog-db ../work/versioned/catalog.db --warehouse ../work/vw run --source shopify --csv data/raw/shopify_orders.csv --contract data/raw/shopify_orders.csv.contract.json --archive-root ../work/archive --batch-date 2026-07-25 --initialize-empty-state | ConvertFrom-Json
$amazon = python scripts/versioned_orders.py --catalog-db ../work/versioned/catalog.db --warehouse ../work/vw run --source amazon --csv data/raw/amazon_orders.csv --contract data/raw/amazon_orders.csv.contract.json --archive-root ../work/archive --batch-date 2026-07-25 | ConvertFrom-Json
$pos = python scripts/versioned_orders.py --catalog-db ../work/versioned/catalog.db --warehouse ../work/vw run --source pos --csv data/raw/pos_orders.csv --contract data/raw/pos_orders.csv.contract.json --archive-root ../work/archive --batch-date 2026-07-25 | ConvertFrom-Json
python scripts/gold_release.py publish --catalog-db ../work/versioned/catalog.db --warehouse ../work/vw --output-root ../work/gold --batch-date 2026-07-25 --receipt $shop.receipt --receipt $amazon.receipt --receipt $pos.receipt
python scripts/gold_release.py inspect --output-root ../work/gold
```

For a nonempty legacy catalog, complete the explicit state migration first. See
the [source-batch guide](../phase2/source-batches.md). Normal source replay creates
fresh completion receipts without changing the same business versions.

## Verification boundaries

Tests exercise mixed statuses, exact cents, empty completed populations, category
ties, tombstones, invalid totals/keys/currencies, source readiness, drift during
publication, failed metrics and artifact tampering. The local CLI demonstration
uses all 120,000 records across Shopify, Amazon and POS and verifies consumer checksums and repeat publication.

REST/MinIO, real Airflow execution, container builds and the four Docker tests
remain unexecuted on this host. New local release files do not establish those
checks or complete the project. No remote push, PR, CI or deployment is performed.

Add `--inventory-bundle DIRECTORY` to publication for the optional inventory artifact and retained input evidence. Four-artifact releases remain readable; see the inventory guide for required coverage, provenance and cost/period definitions.

The [versioned Airflow runtime](../phase5/versioned-airflow-runtime.md) now transports frozen deliveries and receipt references through a manual DAG. Runtime publication includes a per-run identity and lock-checked prior-pointer precondition; real Airflow/live-stack proof remains pending.
