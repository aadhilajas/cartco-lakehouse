# Project walkthrough

Use the actual running assessed stack and a retained `SUCCESS.txt`. This script
is a demo plan, not a claim that a narrated recording already exists.

## 1. Show the result

Open [the public dashboard](https://cartco-lakehouse.vercel.app/) or the local
dashboard at 127.0.0.1:18100. Explain that it is a frozen synthetic
fixture with actual pipeline outputs. Show completed sales, order count and
average order value. Filter Shopify to the reconciled INR 301.50 sample, then
reset. Explain why refunded/cancelled orders and tombstones do not count as
completed sales. Point out that inventory scope is separately labelled.

## 2. Show protocol evidence

Open an accepted delivery manifest and its raw object checksum. Explain the
GraphQL cursor and Orders/OrderItems token paths. For POS, show two partitions,
durable landing before offset commit, exact replay and refusal of a checkpoint
whose local landing is missing. Show the SFTP ready manifest, size/hash and
host-key verification. Its stock/cost entity is not a fourth order channel.

## 3. Show real Spark and catalog state

Open the actual scheduled Spark task log and catalog records. Describe the
source-version window and conflict rejection, retained tombstones, exact decimal
money, channel-qualified customers and new date-partitioned Bronze tables.
Show one real explain plan. State `local[2]` accurately; do not call the host a
distributed cluster. Show actual table UUID/snapshot identities and metadata.

## 4. Show failure safety

Open the retained exact-reconciliation failure and genuine failed GE report.
Compare the good CURRENT pointer before/after. Explain immutable Gold candidates
and coherent release publication. Show bad snapshot refresh retaining good UI
data. Do not inject failure into the original baseline or delete its volumes.

## 5. Follow lineage and operations

Open the Lineage tab and then Marquez's real backend/UI. Distinguish 44 verified
schema fields from 34 nonconstant derivations; constants do not need fabricated
input edges. Open Grafana's actual DAG run states/durations and its read-only
health datasource. A missing duration is unavailable, not zero or an invented SLA.

## 6. Explain reproducibility and limits

Show the shared assessed specification and isolated Terraform plan. Explain the
separate Spark environment and protected volumes. Show the actual restart or
recovery evidence only when it exists. Close with honest boundaries: synthetic
one-line source contracts, a small fixture, no production merchant onboarding,
and the distinction between the public saved snapshot, temporary GitHub processing
and retained local volumes. Show the producing run from the website's published
provenance. Persistent cloud services and ARM qualification remain separate;
remote publication success does not establish them.
