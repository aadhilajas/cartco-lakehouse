# Bounded protocol connectors

These connectors exercise local synthetic HTTP, SSH/SFTP and Kafka protocols.
They do not authenticate to a merchant, impersonate a seller, or fetch customer
data. Order fixtures deliberately have one product line, explicit INR, no separate
tax/shipping/discount and full-state source versions. Wider merchant contracts
require a separate design. Synthetic Amazon category is a declared fixture field,
not a claim that Orders v0 supplies product taxonomy.

- Shopify GraphQL: cursor pagination and updated-at windows; API/GraphQL failures
  never publish a complete delivery. Refuse multiline and unsupported financial
  states rather than silently drop line items or partial refunds.
- Amazon Orders v0 plus paginated OrderItems: NextToken, explicit update window,
  bounded retries for throttling/transient HTTP errors and exact unit cents.
- Kafka POS: finite snapshot of partition end offsets; manual offset commits only
  after validated immutable delivery and durable landing acknowledgement. Failed
  batches retain raw bytes/rejection evidence and do not advance offsets. One
  scheduled owner per consumer group/topic is required. This is at-least-once
  extraction with downstream idempotency, not distributed exactly-once execution.
- SFTP: verify the SSH host key, consume only a `.ready.json` manifest uploaded
  after the CSV, verify size/SHA-256/count and stable stats/marker. Its entity is
  SKU/location daily stock with fixed standard cost; it is not an order source.

Raw response/message bytes are SHA-256-addressed before normalization. Completed
CSV deliveries carry validated contracts and immutable manifests. Exact delivery
replay returns the same identity; absence from a full extraction is never an
implicit deletion. Explicit versioned tombstones remain supported in the POS path.
HTTP headers/tokens are not copied into evidence. Redirects are refused.

Local executable checks: `tests/unit/test_protocol_connectors.py` starts actual
localhost HTTP and SSH/SFTP servers. Kafka commit-order tests use a double and
are not broker acceptance; the separate live runner records the real broker proof.

## Primary protocol references

- [Shopify cursor pagination](https://shopify.dev/docs/api/usage/pagination-graphql)
- [Shopify orders query](https://shopify.dev/docs/api/admin-graphql/2026-10/queries/orders)
- [Amazon Orders v0](https://developer-docs.amazon/sp-api/reference/orders-v0)
- [Kafka Python clients](https://kafka-python.readthedocs.io/en/master/high_level.html)
- [Paramiko host-key policies](https://docs.paramiko.org/en/stable/api/client.html)

Transport package versions and artifact hashes are in `requirements/connectors.txt`.
Keep Spark compute and Airflow dependency environments separate.
