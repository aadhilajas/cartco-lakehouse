> Implementation-stage reference. Pending statements below belong to the
> original stage. See the [documentation index](../README.md) and
> [current verification report](../test_report.md) for later acceptance, remote CI
> and the working public snapshot deployment.

# Scoped standard-cost inventory turnover

The optional local Gold inventory path adds an actual COGS / average daily
inventory ratio, distinct from category unit sales. It requires explicitly
supplied inventory scope, product mappings, COGS allocations and daily stock
snapshots. Without those inputs, the four commerce artifacts remain unchanged;
the system never invents inventory from sales alone.

## Metric contract

The supported cost basis is fixed-standard-cost-v1. Each SKU/location has one
nonnegative exact-cent INR unit cost throughout the reporting period. Both COGS
and daily closing inventory valuation must use that cost. This is a declared
standard-cost operational model, not an actual-cost/FIFO/weighted-average engine
or audited accounting COGS. Cost-method support must be added explicitly.

The period is start-inclusive/end-exclusive in the declared business calendar,
and must contain 1..3660 days. Period end cannot exceed the day after the business
batch date. Basis order_date_latest_state means COGS covers the pinned state's
currently completed orders placed within that period. Fully refunded, cancelled,
pending and deleted orders are excluded. The period is not an expense-posting,
shipment or historical recognition period. Partial refunds and split fulfillment
require expanded inputs and metrics.

For each explicitly scoped (sku, location_id) pool:

```text
COGS = sum(completed order quantity * declared fixed unit cost)
inventory value for a day = closing on-hand units * that same unit cost
average daily inventory value = sum(all calendar daily values) / calendar days
turnover = COGS * calendar days / sum(all calendar daily values)
```

The ratio uses the unrounded denominator. Average inventory is displayed as
decimal(28,10); turnover as decimal(38,12), rounded half-up. COGS is decimal(18,2)
and the sum of daily values is decimal(24,2). Money inputs requiring fractional
cent rounding, negative values or overflow are rejected. No annualization or
days-in-inventory metric is inferred. Pool ratios must not be summed or averaged
without a separately defined aggregate metric.

Shared stock across channels belongs to one SKU/location pool, with COGS summed
across those channels and one inventory denominator. Unsold declared pools remain
in the output. A single-product order has one allocation to one location in v1;
split-location allocations are not supported. Do not assign real fulfillment
locations without authoritative allocation evidence.

## Input bundle

Supply a directory containing exactly the following declared file inventory:

| File | Required CSV columns / meaning |
| --- | --- |
| scope.csv | sku, location_id, unit_cost; one unique pool, including unsold stock |
| products.csv | channel, product_id, sku; one mapping per source product |
| allocations.csv | channel, order_id, sku, location_id, quantity, unit_cost, cogs, cogs_date |
| snapshots.csv | sku, location_id, snapshot_date, on_hand, inventory_value |

Extra CSV columns are rejected in this contract. Keys are canonical nonempty
strings. Quantities are nonnegative integers up to 2^31-1. Dates use YYYY-MM-DD.
Daily stock represents business-day closing balances, including weekends and
days without sales. There is no implicit fill-forward.

allocations.csv must exactly cover every completed order in the period, once.
No omitted order, duplicate allocation, cancelled/refunded order, or allocation
from outside the period is allowed. Product SKU, quantity, date and cost are
reconciled to the pinned order and declared scope. cogs_date must equal its
order_date under this supported period basis. Scope and product mappings must
support each allocation. Unknown pool/date snapshots and duplicate daily rows
are rejected. Daily inventory_value and allocation cogs must reconcile exactly
to quantity times the scoped cost.

manifest.json requires these fields:

```json
{
  "schema_version": "inventory.v1",
  "cost_basis": "fixed-standard-cost-v1",
  "period_basis": "order_date_latest_state",
  "currency": "INR",
  "business_timezone": "Asia/Kolkata",
  "state_table_uuid": "EXACT_STATE_UUID",
  "state_snapshot_id": 123456789,
  "period_start": "2026-07-18",
  "period_end": "2026-07-25",
  "is_synthetic": false,
  "input_provenance": "Description of the supplying cost/inventory authority",
  "files": {
    "scope.csv": "SHA256",
    "products.csv": "SHA256",
    "allocations.csv": "SHA256",
    "snapshots.csv": "SHA256"
  }
}
```

The state UUID, snapshot ID and business timezone must match the exact commerce
publication snapshot. Any state commit, including a change outside the inventory
period, requires a newly reconciled bundle. is_synthetic is a required boolean;
external provenance is a producer declaration, not independent confirmation of
inventory accuracy. Actual source ownership, physical counts and supply/movement
ledger reconciliation remain operational prerequisites for real-world use.

## Coverage and failure behavior

| Condition | Result |
| --- | --- |
| All daily pool snapshots present, positive denominator | Complete ratio; no-sales pool has zero COGS/turnover |
| Missing any daily snapshot | Null average and ratio; incomplete_daily_coverage with observed/expected day counts |
| Complete coverage but zero inventory value | Zero average, null ratio; zero_inventory_value |
| Invalid mappings, cost basis, amounts, allocations, dates, duplicates or stale state | Reject publication; preserve previous CURRENT.json |

inventory_value_days records the observed sum; missing days are never imputed.

Incomplete/zero pools are intentionally published with null values and explicit
quality reasons, per the release contract. They are not silently treated as
zero turnover or omitted from the scope. Quality summaries expose affected pool
counts so downstream consumers can choose stricter acceptance policies.

## Generate synthetic fixtures and publish

Use the isolated requirements/file-integration.txt environment. The generator
reads a ready local state snapshot and creates a new directory; it refuses to
replace an existing one. All generated costs, stock and source-product/location
mappings are invented and marked is_synthetic=true in both manifest and output.
They use a hypothetical shared SYNTHETIC-DC, not real POS store inventory. A
separate unsold pool demonstrates coverage of stock without sales.

```sh
python scripts/generate_inventory_bundle.py --catalog-db LOCAL/catalog.db --warehouse LOCAL/w --output LOCAL/inventory-new --period-start 2026-07-18 --period-end 2026-07-25
python scripts/gold_release.py publish --catalog-db LOCAL/catalog.db --warehouse LOCAL/w --output-root LOCAL/gold-with-inventory --batch-date 2026-07-25 --receipt SHOPIFY_RECEIPT --receipt AMAZON_RECEIPT --receipt POS_RECEIPT --inventory-bundle LOCAL/inventory-new
python scripts/gold_release.py inspect --output-root LOCAL/gold-with-inventory
```

Supply the original completion receipts associated with that exact state and
matching source windows. For intentionally two-source compatibility, explicitly
declare --required-source shopify --required-source amazon.

The new inventory_turnover.parquet is the fifth coordinated release artifact.
The legacy gold.inventory_turnover SQL table still contains category unit sales;
it is not rewritten or relabelled by this path. Prior four-artifact releases
remain readable. The release contract includes the inventory manifest and file
checksums. Original bundle bytes are read once and retained under immutable
inventory-inputs/ in the release. Consumer reads verify those evidence hashes as
well as every output artifact. Editing or deleting the original bundle cannot
change an already published release; tampering with retained evidence is detected.

## Validation and scope

The builder verifies business coverage and cost arithmetic. A separate output
gate independently recomputes numerator, denominator, calendar coverage, status
and ratio before pointer advancement. Tests cover shared pools, unsold stock,
fractional-cent/negative inputs, allocation gaps, duplicates, unknown mappings,
zero/missing inventory, stale state, tampering and corrupted metrics.

The full local demonstration adds a seven-day inventory bundle and fifth artifact
to the existing 120,000-record, three-source state, using explicitly synthetic
inputs. Real ERP/WMS extraction, cost-method authority, movement reconciliation,
expense-date COGS, streaming transport, Airflow activation, Docker/REST proof,
backup/restore and cloud deployment remain pending.
