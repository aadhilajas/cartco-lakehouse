# ADR-014: Bounded synthetic protocol ingestion

Date: 12 November 2026
Status: accepted for local protocol execution

## Context

The assignment requires Shopify GraphQL, Amazon SP-API, Kafka POS and SFTP.
The existing CSV adapters remain valuable regression references but are not
evidence that those protocols ran. Real merchant onboarding is outside this
synthetic project and no merchant credentials are assumed.

## Decision

Use a real localhost HTTP server with GraphQL cursor and Orders/OrderItems
NextToken shapes, a real two-partition Kafka broker and a real SSH/SFTP fixture.
Keep original response/message/file bytes by SHA-256 before validation. Publish
an immutable normalized CSV delivery only after the existing source contracts
pass. Refuse redirects, nonadvancing pagination, unbounded responses, partial
files, host-key mismatch, unsupported multiline orders and fractional cents.

Kafka snapshots end offsets for a finite microbatch. It commits only after a
validated durable landing and its retained checkpoint receipt. A missing landing
behind an existing broker checkpoint is an error, not a reason to reset offsets.
Replay identity does not depend on partition interleaving. Explicit source
versions/tombstones are retained for downstream state reconciliation.

SFTP supplies stock and fixed standard cost by source-qualified SKU/location/date.
It is never relabelled as an order source. A ready marker names a sibling CSV
with exact size, checksum and row count; the client rechecks stats and marker.

## Consequences

The protocol boundaries are executable locally without seller accounts. Fixture
scope is explicit: one product line, INR, no separate shipping/discount/tax and
declared synthetic Amazon taxonomy. Production authentication and wider merchant
semantics need a new design. Kafka provides at-least-once extraction plus
idempotent state, not a distributed exactly-once claim.

## Alternatives

- Keep CSV readers only: insufficient protocol evidence.
- Onboard real stores/sellers: credentials, approvals and privacy exceed scope.
- Continuous stream processing/Flink: unnecessary for a finite assessed batch.
- Auto-accept SSH host keys or consume `.part` files: hides trust/completion errors.

## Evidence

Real local protocol acceptance retained under `work/assessed-acceptance`, including
two Kafka partitions, exact replay, resume receipts and missing-landing refusal.
Boundary tests are in `tests/unit/test_protocol_connectors.py`. See
`docs/protocol-connectors.md` for the primary protocol references.
