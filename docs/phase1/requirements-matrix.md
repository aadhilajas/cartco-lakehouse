# Requirements and evidence matrix

Baseline commit: a4b3406. Phase 1 establishes reproducible execution, not new
business behavior. Status means implemented/evidenced here, not planned success.

| Requirement | Existing evidence | Remaining work / acceptance |
|---|---|---|
| Three synthetic sources | Shopify/Amazon generators and pipelines | Third source end to end; POS batch proposed |
| Bronze source fidelity | Explicit source schemas, daily loads | Immutable input manifests, extension fields, replay contracts |
| Canonical Silver | Shared silver.orders and status mappings | Safe updates, batch dates, stable update ordering |
| Business Gold | Four transform modules and unit tests | Correct status/money rules and inventory inputs |
| Airflow orchestration | Three DAG definitions | Real DAG import check added; scheduling/readiness later |
| Quality on Silver/Gold | Bronze GE gates only | Output gates and persisted results |
| Lineage | Source and Gold event emission | Live all-source proof, facets and failure-path coverage |
| Reproducible environment | Clean hash-checked install, Python 3.11, fixed-source MinIO build | Docker build and live verification pending |
| CI | Unit job and full-stack baseline job | Remote run pending; both-source seeding, status scope and fresh-snapshot checks prepared |
| Durability and recovery | Compose storage volumes | Catalog recreation and restore tests |
| Deployment | Local Compose configuration | Cloud deployment not executed |
| Final delivery | Historical screenshots/demos and six ADRs | Current architecture/test report and final-week evidence |

Phase 1 gate: clean Python 3.11 install + dependency check + unit discovery/run,
actual DAG import/graph check in the built image, reviewed contracts, reproducible
image references and documented bootstrap. Every unexecuted check stays pending.
