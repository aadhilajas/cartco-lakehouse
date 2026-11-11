# Sources and contracts

The initial implementation has three synthetic commerce sources. No real PII or
production credentials are used. Each generator can emit versioned CSV plus a
`<source>.csv.contract.json` delivery contract. The same delivered adapter accepts
prepared files; it is not a Shopify/Amazon API client.

| Source | Source schema | Raw event table |
|---|---|---|
| Shopify | shopify.orders.v2 | bronze.shopify_order_events |
| Amazon | amazon.orders.v2 | bronze.amazon_order_events |
| POS | pos.orders.v1 | bronze.pos_order_events |

Raw bytes/contracts are archived unchanged with hashes. Event tables retain raw
JSON, batch identity, source row number and declared ingestion date. Canonical
`silver.order_state_v2` uses (channel, order_id), source_updated_at, is_deleted,
explicit INR and decimal money. Older versions cannot resurrect tombstones;
equal-version conflicting payloads fail. Gold pins one retained state snapshot
and validated completion receipts across all required channels.

Contracts declare source/business timezone, schema version, extraction window
and snapshot/incremental delivery semantics. Default business timezone is
Asia/Kolkata. POS supports one product per receipt, guest/loyalty identities,
zero separate taxes/discounts and the documented status mapping. Unsupported
partial refunds or multiline receipts are rejected rather than approximated.

Inventory is a separate SKU/location scope with product mapping, order COGS
allocations and daily stock evidence. Turnover uses fixed standard cost, an
explicit reporting period and null/reason fields for missing or zero stock.
Synthetic inventory is labelled. It is not inferred from sales revenue.

Kafka transport, SFTP distributor and ERP adapters remain extensions. More than
three initial sources and sub-second streaming were outside the initial scope.
See phase2 source-time-contracts, phase3 POS and phase4 inventory guides for exact
field-level contracts and migration restrictions.
