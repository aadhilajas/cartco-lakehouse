# ADR-008: POS v1 grain and scoped receipt identity

Status: Accepted for the local versioned path
Date: 2026-11-11

POS is the third source, identified as `pos`, with source schema pos.orders.v1.
It preserves ADR-007's single-product order grain: one receipt with exactly one
product line, possibly multiple units. Multi-product receipts are rejected until
a line/receipt schema and corresponding counts/AOV are implemented together.

The native identity tuple is store_id, terminal_id, transaction_id. Producers
must supply transaction IDs unique over terminal history. A deterministic UUIDv5
over a JSON-encoded tuple is the canonical order_id. Updates and tombstones
carry the same identity, preventing collisions from reused numbers at other
stores or terminals. Bronze keeps native metadata; no Silver schema rewrite is
needed for the existing (channel, order_id) key.

An empty customer_id explicitly means walk-in. It maps to a per-receipt guest
identity; known loyalty IDs use a separate namespace. Guest profiles do not
measure unique people. Partial refunds, separate taxes and discounts are not
modeled and must be rejected rather than silently changing revenue semantics.
Money inputs are exact cents, with no ingestion rounding; AOV alone uses the
documented half-up cent rounding.

All three sources use explicit delivery contracts and retained raw batches.
The default local Gold release requires all three completion receipts. Explicit
two-source compatibility is allowed only when no undeclared POS state exists.

The initial implementation accepts source CSV deliveries. This completes the
local POS data path without claiming Kafka transport. Streaming delivery still
needs explicit broker/offset/delivery semantics, orchestration and live tests.
The [POS guide](../phase3/pos-source.md) defines fields, mappings, commands and
the reproducible acceptance demonstration.
