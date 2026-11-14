# Sources and business contracts

The current application exercises **four real protocol paths against synthetic
fixtures**. There are three order channels; inventory is a separate entity.
It does not authenticate to live merchants or collect real customer data.

| Source | Entity | Current connector | Behavior exercised |
|---|---|---|---|
| Shopify GraphQL | Single-line orders | `ingestion/protocols/shopify.py` | Cursor pagination and explicit updated-at windows |
| Amazon Orders/OrderItems REST | Single-line orders | `ingestion/protocols/amazon.py` | Paginated order/item requests and bounded retry behavior |
| Kafka POS | Versioned receipt events | `ingestion/protocols/kafka_pos.py` | Two partitions, finite end-offset window and durable landing before acknowledgement |
| SSH/SFTP | Daily inventory and standard costs | `ingestion/protocols/sftp.py` | Host-key verification, ready manifest and file checksum/size checks |

The demonstration business date is **12 November 2026**. Its accepted business
result is seven completed orders and INR 921.75. Unchanged deterministic inputs
can produce the same totals on later processing dates. Compute, export and
publication times are distinct from the fixture business date.

## Evidence and canonical state

Raw response/message/file bytes remain hash-addressed. Date-partitioned Bronze
rows retain source/batch identities and raw payloads; wire objects retain their
checksums and bytes. Failed source completion does not become a valid delivery.

Spark canonical state uses channel-qualified order keys, source version times,
retained tombstones, INR and `decimal(18,2)` money. Late older events cannot
resurrect deleted records. Equal-version conflicting payloads fail rather than
winning by arrival order. Completed-sales calculations exclude cancelled,
refunded, pending and deleted orders.

## Inventory and business limits

SFTP stock observations cover eight source-qualified SKU/location pools with
fixed standard costs. Turnover uses completed-order cost of goods and average
observed stock value within an explicit period. Missing observations or zero
stock are labelled; selling price is not substituted for inventory cost.

Order fixtures support one product line, explicit currency and no separate
shipping/tax/discount amounts. Multiline receipts, unsupported partial refunds
and real merchant onboarding need a new contract/design. Customer identities are
qualified by source; cross-channel identity matching is not fabricated.

## Contracts, ownership and public export

[commerce-v1.json](../contracts/commerce-v1.json) records business rules and their
executable validators. [Owner review](../contracts/OWNER_REVIEW.md) is still pending
and must be a genuine dated review. A passing test is not business approval.

Public exports contain approved aggregate results, catalog identities, quality
summaries and declared lineage mappings. They omit raw payloads, customer IDs,
authentication material and local filesystem paths. Publishing a snapshot does
not publish the full local storage system or private operator interfaces.

See [protocol details](protocol-connectors.md), [catalog](catalog.md),
[current runbook](assessed-runbook.md) and
[earlier CSV-adapter documentation](history/baseline-data.md).
