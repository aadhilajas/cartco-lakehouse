# Source schema, extraction windows and business time

The local versioned path requires an explicit delivery contract for every batch,
including valid empty deliveries. The contract is stored in the immutable raw
manifest and attempt receipts, contributes to batch identity and is checked by
Gold. Historical uncontracted archives remain untouched; they cannot authorize
a new contracted release. Existing release artifacts remain independently readable.

## Generate and run

```sh
python scripts/dev.py generate --batch-date 2026-07-25 --source-versions
python scripts/versioned_orders.py --catalog-db local/catalog.db --warehouse local/warehouse run --source shopify --csv data/raw/shopify_orders.csv --contract data/raw/shopify_orders.csv.contract.json --archive-root local/archive --batch-date 2026-07-25 --initialize-empty-state
```

Use the generator's configured output path if overridden. Run Amazon with its
CSV and sidecar on the same business date. Both generated sidecars explicitly
declare business time `Asia/Kolkata`, naive source order timestamps as `UTC`,
and snapshot delivery. Legacy generation without source versions is unchanged.

Example Shopify sidecar:

```json
{
  "contract_version": 1,
  "source_schema_version": "shopify.orders.v2",
  "business_timezone": "Asia/Kolkata",
  "source_timezone": "UTC",
  "delivery_mode": "snapshot",
  "extraction_start": "2026-07-25T00:00:00+05:30",
  "extraction_end": "2026-07-26T00:00:00+05:30"
}
```

All seven fields are required. Unknown fields, unsupported contract/schema
versions, unknown IANA zones, non-increasing windows and offset-free interval
boundaries are rejected. The extraction start must fall on the declared business
batch date. Explicit multi-day and intraday windows are allowed; Gold requires
the same actual UTC start/end and business timezone across required sources.
Equivalent offset spellings compare as the same window, but distinct declaration
bytes produce different batch identities. JSON key ordering is canonicalized.

## Delivery semantics

| Mode | Source-version rule | State behavior |
| --- | --- | --- |
| incremental | start <= source_updated_at < end | Apply delivered complete versions and explicit tombstones |
| snapshot | source_updated_at < end; older versions allowed | Merge delivered versions; omitted keys are preserved |

Neither mode infers deletions from missing rows. Explicit `is_deleted=true`
events are required. Exact replay is a no-op; a changed declaration creates a
new raw batch even when CSV bytes are unchanged. Invalid business/schema/window
deliveries retain their raw CSV, manifest and Bronze rows and receive a rejected
receipt before state access. Malformed CSV retains its parse-error evidence.
Malformed JSON that cannot be loaded by the CLI is rejected before ingestion.

These checks validate delivered data and producer declarations. They cannot
prove that a source included every update, that an empty delivery is truthful,
or that extraction completed through its declared end. Independent source
watermarks, connector completeness checks and scheduled freshness SLAs remain
orchestration work. Applied/audit time never substitutes for source update time.

## Supported source schemas

`shopify.orders.v2`, `amazon.orders.v2` and `pos.orders.v1` are supported.
The [POS guide](../phase3/pos-source.md) defines its native receipt identity,
single-product grain, status vocabulary and walk-in handling. Online sources require
`order_id`, explicitly offset source_updated_at and literal true/false is_deleted
headers. Key/version/delete-only tombstones and empty files with these headers
are valid. Live rows additionally require customer_id, order_date, product key,
product_category, quantity, unit_price, status and INR currency. Product keys are
product_id for Shopify and asin for Amazon. Unknown CSV columns are retained raw.

Shopify statuses are delivered/cancelled/returned; Amazon statuses are
shipped/cancelled/refund_initiated/pending. They map to the documented canonical
statuses. Quantity is 0..10000 and exact-cent price is 0..500000; totals are derived
exactly. This is a strict supported adapter contract, not automatic schema
evolution. Adding a source schema version requires an explicit adapter change.

## Date interpretation and state protection

Source version timestamps must contain date/time and a UTC offset and are stored
as UTC timestamps. Order timestamps accept ISO date/time (T or space separator).
An explicit offset takes precedence; otherwise source_timezone interprets the
wall time. Ambiguous DST folds and nonexistent DST gaps require an explicit
offset. Convert the resulting instant into business_timezone before deriving
the canonical calendar order_date. For example, `2026-01-04T18:30:00Z` is
`2026-01-05` in Asia/Kolkata. No host-local timezone or clock is used.

New state tables persist cartco.business.timezone. Ingestion checks it before
merging and on each optimistic retry. Gold checks it against every source receipt
and records it in the release contract. A timezone mismatch or an undeclared
existing table blocks writes and publication. Do not change this property by
hand: already persisted order dates would retain their old meaning.

Existing v1 migration and reviewed legacy candidates now require an explicit
`--business-timezone` declaration in addition to INR. The declaration asserts the
meaning of the existing dates; it does not convert dates. Use UTC only when
trusted evidence establishes those dates were derived in UTC. If changing date
meaning, replay trusted complete history, including deletes, into a separate
catalog under the intended contract and reconcile it before consumer cutover.
Unbound existing v2 tables are preserved and require this explicit rebuild; no
automatic metadata adoption or date rewrite is implemented. Preserve the old
catalog, warehouse and Gold pointer for rollback. Pause writers during cutover.

## Verification and remaining scope

Tests cover business midnight, DST gaps/folds, interval endpoints, snapshot
history, schema rejection, immutable metadata identity, raw retention, timezone
drift and mismatched/tampered Gold evidence. The 90,000-order local demonstration
independently verifies every persisted business date and reconciles all marts.

This implements local delivery contracts. Production connector watermarks,
cross-process configuration locking, automated historical timezone migration,
Airflow contract transport, scheduling/backfills and Docker/REST acceptance
remain pending. No remote pipeline is activated.
