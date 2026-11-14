# Historical reference: baseline lineage

> Preserved from the documentation before the 14 November 2026 navigation cleanup.
> Pending statements and architecture details below describe the earlier application,
> not the current project status. Start with the [documentation index](../README.md).

# Current lineage and observability

The coordinated versioned runtime emits paired START/COMPLETE/FAIL events via
OpenLineage 1.11.3 to local Marquez. Each invocation has its own UUID; business
batch date and orchestration run identity are separate facets. A local intended
journal and checksum-linked client-delivery status retain what was attempted.
Instrumentation remains fail-open and never changes business results/exceptions.

Source events declare CSV field schemas and input row counts, raw JSON field
preservation, source normalization and versioned merge dependencies. Silver
fields include prior-state dependencies to represent retained rows and tombstones.
Gold outputs declare schema, actual artifact row counts and columnLineage facets.
Maps include grouping keys, completed-sales/status controls, exact quantity/price
arithmetic and explicit cost/stock dependencies for turnover. Constants such as
INR and unavailable fulfillment time are labelled; no fictitious input edge is
invented for a constant null.

The five-artifact live event exposes 44 output fields. Three source-stage events
add 57 checks across raw/Silver output fields, for 101 verified field checks. Direct dependency edges
for every derived output were checked against Marquez's column-lineage API;
input-free constants were checked against their event schema/declaration because
the backend need not create a lineage node for a constant. Unit tests also cover
POS native keys, source product/status normalization and prior state. This is
field dependency tracing, not exhaustive per-row or historical-version provenance.
Legacy DAG lineage remains table-level; no blanket claim is made for those paths.

Inventory input schemas must be declared as well as output schemas: without
input field metadata, Marquez can accept an event while omitting the requested
stock/cost edges. Live backend verification caught this and the input schemas
were added before acceptance. Consumer checks distinguish backend indexing from
a successful client call.

```text
python scripts/check_column_lineage.py --event <completed-event.json> --output <column-report.json>
python scripts/check_marquez_runtime.py --expected <versioned-runtime.json> --orchestration-run-id <run-id> --output <runtime-report.json>
python scripts/check_runtime_observability.py --help
```

Retry/failure callbacks write local alert records. The controlled task-kill drill
produced one real retry_scheduled alert and no terminal alert. The incident stays
visible as clean_history=false. No email, Slack, webhook, acknowledgement or
external paging consumer is configured or invoked. Retention/backup covers the
local runtime files, with their documented integrity/authenticity boundaries.
