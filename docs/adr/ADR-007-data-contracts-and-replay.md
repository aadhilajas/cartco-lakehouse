# ADR-007: Release data contracts and replay semantics

Status: Accepted as the target contract; schema/write migrations are pending.
Date: 2026-11-09

## Decision

- Release grain: one single-product order per row. Business key is
  (channel, order_id). Multi-line orders require a future explicit line key.
- Source updates must carry source_updated_at; latest source version wins.
  Conflicting payloads at an equal update timestamp are rejected/quarantined,
  rather than selected nondeterministically. Exact duplicate payloads collapse.
- Delta inputs contain upserts and explicit tombstones. Absence from a delta
  is never a delete. Full snapshots require an explicit, validated coverage
  manifest. Never infer snapshot completeness from minimum/maximum order dates.
- Each run supplies batch_id, source, business_date, extraction interval, source
  schema version and input checksums. Ingestion timestamps are UTC audit fields,
  not a substitute for the requested batch date. Source business dates use an
  explicit timezone (Asia/Kolkata for this synthetic INR release).
- Bronze retains original input files and parsed source fields. Invalid records
  retain source/batch attribution and reasons. Required key/schema failures fail
  the batch; tolerated optional-field issues require an explicit quality rule.
- Financial amounts use decimal precision 18, scale 2, ROUND_HALF_UP after
  quantity times unit price. Synthetic release currency is INR. Currency must
  be preserved and grouped/validated; no implicit cross-currency totals.
- Placed value includes all placed orders and is labeled separately. Retained
  completed sales includes completed orders only; pending/cancelled/fully
  refunded orders contribute zero. Refund amounts/counts are separate measures.
  This is latest-state reporting, not historical accounting recognition. Partial
  refunds and cash-flow ledgers require additional explicit source events.
- Cancellation/refund rates use their status counts divided by all placed orders
  in the same declared period/channel. Completed-order AOV uses completed sales
  divided by completed orders; zero denominators produce null, not infinity.
- Customer purchase/spend metrics use completed orders and channel-scoped IDs.
  Global identity and product equivalence require explicit reference mappings.
- Missing fulfillment measurements are null. Inventory turnover requires period
  COGS and average daily inventory value with the same SKU/location coverage and
  cost basis. Missing/zero balances yield null plus a quality indication.
- Expected-source completion manifests define freshness. An explicit validated
  empty batch is valid; a missing batch is not. Gold publication requires all
  required source batches and one accepted input-snapshot/release boundary.

## Implementation boundary

Phase 2 now propagates explicit load dates and guards the legacy range overwrite
against dropping or changing existing orders. Fixed-date source generation is
reproducible. Floats and order-placement-time deduplication remain; source update
versions, immutable manifests and schema migrations are still pending. The guard
rejects updates until a versioned merge is implemented; it is not an upsert.
Migration must preserve existing source data and add regression tests before
switching writes. ADR-003/006 describe the older implementation where they differ.
