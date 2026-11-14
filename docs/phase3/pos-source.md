> Implementation-stage reference. Pending statements below belong to the
> original stage. See the [documentation index](../README.md) and
> [current verification report](../test_report.md) for later acceptance, remote CI
> and the working public snapshot deployment.

# POS v1: third source through local Gold

POS now supports a deterministic synthetic generator and the existing immutable
CSV -> Bronze events -> versioned Silver -> coordinated Gold path. This is a
local batch adapter. The Kafka consumer, offsets, broker setup, acknowledgements
and Airflow activation remain unimplemented. The legacy Kafka stub is not the
POS entry point.

## Grain and identity

Schema `pos.orders.v1` represents exactly one single-product receipt per row.
Quantity may exceed one for that product. Multi-product receipts are rejected,
so current order counts and AOV retain their receipt meaning. Supporting line
items later requires a separate receipt/line model and reviewed mart changes.

Required native identity fields are store_id, terminal_id and transaction_id.
The producer must make transaction_id unique within a terminal across its full
history; a resettable receipt counter alone is insufficient. Use the source's
durable transaction ID, including its date/sequence namespace when necessary.
order_id must match `pipeline.pos.pos_order_id(...)`, a UUIDv5 over an
unambiguously encoded native tuple. Reused numbers in other stores/terminals
remain distinct. All identity fields are case-sensitive canonical nonempty
strings without surrounding whitespace. Do not change them on an update.

The canonical business key remains (channel, order_id), with channel `pos`.
Tombstones require order_id, native identity fields, source_updated_at and
is_deleted=true. Business cells may be empty in a CSV containing both live rows
and tombstones. Omission never deletes an existing receipt.

## Live fields and mapping

| Source field | Meaning / canonical mapping |
| --- | --- |
| sku | product_id; source-specific, not a cross-channel product mapping |
| order_date | Source transaction timestamp -> declared business calendar date |
| customer_id | Loyalty ID, or empty string explicitly declaring a walk-in |
| quantity, unit_price | Integer 0..10000 and exact-cent INR price 0..500000 |
| line_count | Exactly the literal integer representation 1 |
| product_category | Required nonempty category |
| source_updated_at | Explicit-offset authoritative update version |
| is_deleted | Literal true or false |
| currency | INR required on live rows |

Statuses map paid -> completed, voided -> cancelled, refunded -> refunded, and
open -> pending. Unknown statuses, including partial refunds, are rejected.
order_total is derived from quantity times price; if the source supplies it,
it must reconcile exactly. Separate tax_amount/discount_amount, if supplied,
must be exact zero. V1 has no tax, discount, partial-refund or cash ledger model;
do not disguise such transactions as zero adjustments. Extra unmodeled CSV
fields remain preserved in the immutable source and Bronze payload.

Identified customers become `pos:customer:<source-id>`. Each walk-in receipt gets
`pos:guest:<order-id>`, so unrelated walk-ins never form a single customer history.
Guest profiles represent receipts, not verified unique shoppers; they must not
be used as a unique-person count. Customer metrics still group by channel.

## Generate, ingest, publish

Use the isolated requirements/file-integration.txt environment for SQLite CLI
commands. Generator defaults: Shopify 50,000, Amazon 40,000, POS 30,000 receipts.

```sh
python scripts/dev.py generate --batch-date 2026-07-25 --source-versions
python scripts/versioned_orders.py --catalog-db local/catalog.db --warehouse local/warehouse run --source pos --csv data/raw/pos_orders.csv --contract data/raw/pos_orders.csv.contract.json --archive-root local/archive --batch-date 2026-07-25
```

Initialize an empty catalog explicitly with --initialize-empty-state on its
first source run. Existing migration and timezone safeguards still apply.
The POS generator also runs standalone, always producing versioned rows and
a contract sidecar. It uses a fixed seed, stable native/scoped IDs, exact decimal
price strings, INR and Asia/Kolkata business time with UTC source timestamps.
Different reference dates are separate synthetic scenarios, not a CDC feed of
the same orders. Explicit update/delete deliveries must reuse existing IDs.

Gold now requires Shopify, Amazon and POS completion receipts by default:

```sh
python scripts/gold_release.py publish --catalog-db local/catalog.db --warehouse local/warehouse --output-root local/gold --batch-date 2026-07-25 --receipt SHOPIFY_RECEIPT --receipt AMAZON_RECEIPT --receipt POS_RECEIPT
```

For an intentionally two-source catalog, explicitly pass
`--required-source shopify --required-source amazon`. This cannot conceal POS
state already present: undeclared channels block publication. Missing POS blocks
the default release; a validated, explicitly delivered empty POS batch is valid.
All source windows and business timezones must match. Published manifests record
the exact required-source set and source evidence. Legacy independent Gold DAGs
are unchanged.

## Reproducible local acceptance

```sh
python scripts/demo_three_sources.py --root ../work/pos-demo-new --batch-date 2026-07-25
```

The root must not exist. Use a short workspace path on Windows for PyArrow 15.
Add --rows-per-source 5 for a small smoke run. The command creates its own new
catalog, generates all sources and sidecars, ingests and replays each, independently
checks every business date and each channel's completed sales/order counts,
publishes Gold twice and verifies consumer checksums and pointer stability.
It saves summary.json with counts/quality and local artifacts under that root.
It never resets an existing catalog or edits the legacy tables.

The full local proof covers 120,000 records. Automated tests additionally cover
status corrections, tombstones, stale events, conflicting versions, unknown
schemas, forged keys, empty POS, loyalty/walk-in separation, missing-source
gates and raw retention on failure. Kafka delivery guarantees, Docker/REST/MinIO,
Airflow, actual inventory inputs and cloud deployment remain separate work.
