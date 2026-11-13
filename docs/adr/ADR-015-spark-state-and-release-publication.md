# ADR-015: Spark state and coherent release publication

Date: 12 November 2026
Status: native and scheduled Compose compute/catalog/quality path accepted; recovery and Terraform tracked separately

## Context

PySpark and a listed catalog are assessed requirements. Existing exact-decimal,
source-version and Gold release contracts must survive the change. Independent
Iceberg table commits do not form a transaction across every Gold mart.

## Decision

Use Spark DataFrames, casts, windows, aggregates and joins against Nessie/Iceberg
on MinIO. Introduce new ingestion-date-partitioned Bronze tables and a separate
assessed namespace. Preserve the original catalog/tables. Keep UTC clock values
in timestamp-without-timezone canonical columns, preventing local Windows
collection from shifting them by the host timezone.

Merge by channel/order/source version. Equal conflicting versions fail; exact
replay retains the existing audit timestamp; tombstones prevent resurrection.
Retain a known state UUID fence. Use exact decimal money, deterministic category
ties, channel-scoped customers and real cost/stock inventory turnover.

Write immutable Gold table names per release. Pin their UUIDs, metadata and
snapshots. Independently reconcile Spark rows with the existing Python/PyArrow
implementation, then apply seven Great Expectations suites. Advance one atomic
publication pointer only after success. Emit/verify actual Marquez column
dependencies and atomically export a privacy-safe dashboard snapshot afterward.

## Consequences

Spark performs the distributed normalization, windows, joins and aggregates.
Final inventory-pool division uses a small decimal UDF to round the rational
result once, avoiding intermediate scale reduction changing the 12-place ratio.
The new thirds regression needs a fresh runtime check after the execution
launcher is restored.

Readers see one coherent release instead of independently changing marts.
Unpublished candidate tables can remain after failure for diagnosis; they are
not the public release. A failed lineage/export stage does not replace the last
valid public snapshot. Source versions, data computation and export timestamps
remain separate. No cross-table ACID transaction is claimed.

## Alternatives

- Hide Python reference calculations behind a Spark wrapper: not actual Spark.
- Replace each Gold table in place and read "latest": risks mixed releases.
- Convert money to doubles: loses the exact-cent contract.
- Delete tombstones or initialize missing known state: allows silent data loss.

## Evidence

Three real Spark/reference tests passed. The actual catalogued candidate passed
five Gold output comparisons and seven GE suites. A real GE null-version failure,
a financial mismatch and a bad export preserved good publication. Backend Marquez
verification covered 44 schema fields and 34 nonconstant derivations. Expanded
scheduled-container and recovery evidence remains separate from native results.

Expanded Compose acceptance subsequently passed all four DAGs, exact state after
recreation/replay and genuine file compaction (five to two, same row hash).
