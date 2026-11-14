# ADR-009: Scoped fixed-standard-cost inventory turnover

Status: Accepted for the optional local Gold inventory artifact
Date: 2026-11-11

Category unit sales cannot stand in for inventory turnover. A new optional fifth
Gold artifact computes COGS divided by average daily inventory value, requiring
explicit source-product/SKU mapping, SKU/location scope, per-order cost allocation
and daily closing stock inputs. Shared stock pools are valued once across channels;
unsold scoped pools remain represented.

V1 uses fixed standard cost per pool, INR and period basis order_date_latest_state.
Every completed single-product order within the business-date reporting period
must have exactly one reconciled allocation. This is an operational latest-state
standard-cost metric, not audited expense-date/actual-cost accounting. Other cost
methods, stock movement reconciliation, partial refunds and split fulfillment
remain explicit future extensions.

Each calendar day requires an explicit pool snapshot. Missing coverage yields
null average/ratio plus incomplete_daily_coverage. Complete zero inventory yields
zero average and null ratio plus zero_inventory_value. Input inconsistency,
duplicate keys, unsupported contracts or stale state block publication. Ratio
calculation uses the unrounded denominator, with independent output reconciliation.

Input bundles declare their state UUID/snapshot, business timezone, exact reporting
period, cost/period basis, synthetic/external provenance and CSV checksums. Original
bytes are retained inside the immutable Gold release and consumer-verified. Fixture
generation invents stock, costs and locations and labels every output as synthetic.

Existing commerce releases remain four artifacts unless inventory is explicitly
supplied. The legacy SQL gold.inventory_turnover table remains a compatibility
name for category unit sales. Local publication does not activate legacy Airflow,
Kafka, REST/MinIO or cloud services. See the [inventory guide](../phase4/inventory-turnover.md).
