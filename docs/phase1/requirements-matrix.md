> Implementation-stage reference. Pending statements below belong to the
> original stage. See the [documentation index](../README.md) and
> [current verification report](../test_report.md) for later acceptance, remote CI
> and the working public snapshot deployment.

> Superseded scope interpretation: review the actual B1 requirements in docs/final/assignment-gap-matrix.md and assignment-aligned-plan.md. Spark, IaC, Grafana and a listed catalog are mandatory gaps; reproducible Compose is a permitted backend option. Historical evidence below remains scoped to the tested baseline.

# Original-plan requirements and current evidence

Updated 2026-11-11. Historical baseline: a4b3406. Current scope follows ADR-012.

| Requirement | Current evidence | Remaining boundary |
|---|---|---|
| Three synthetic sources | Shopify, Amazon and POS, delivered contracts and versioned ingestion | Live APIs/Kafka/additional sources are extensions |
| Source-faithful Bronze | Immutable bytes/manifests/receipts and raw event tables | Offsite protection needs deployment configuration |
| Canonical Silver | Decimal INR, source-version merges, tombstones and migrations | Original compute selection changed to PyArrow/PyIceberg by ADR |
| Business Gold | Four reconciled commerce artifacts and optional standard-cost inventory ratio | Expanded business contracts are future scope |
| Airflow | Four normal DAGs; actual scheduler replay and isolated fresh/retry run | Arbitrary scheduler loss not demonstrated |
| Silver/Gold GE | Seven live suites; mandatory gate on new releases with persisted results | Older retained releases validated retrospectively |
| Lineage | Source/Gold column facets; five-artifact derived edges checked in Marquez | Legacy DAGs remain table-level; no row-level claim |
| Alerts | Real callback plus local retry/failure outbox | External notification destination/authorization not supplied |
| Reproducibility and CI | Locked deps, digest-pinned images, successful local builds/tests | Final remote CI awaits authorized publication |
| Durability/recovery | Catalog recreation, five-volume restore, restored application boot and controlled task kill | Protected/offsite backup and other failure cases remain |
| Local deployment | Working Compose stack and current acceptance evidence | Original cloud milestone not executed |
| Final written delivery | Updated architecture/test report, walkthrough, thinking artifact and resume bullets | Newly recorded/published final video not claimed |

No production or remote deployment result is inferred from a local test. See the
current README, docs/test_report.md and deliverables/week4 evidence summary.
