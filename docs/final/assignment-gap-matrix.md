# B1 assignment and delivery gap review

Reviewed 2026-11-11 against the B1 text supplied by the user and the attached
03_Deliverables_Specification.md. This assessment supersedes the earlier claim
that the local written/implementation package closes all assessed requirements.
No application code was changed as part of this planning review.

## Technical requirements

| Requirement | Current evidence | Required action |
|---|---|---|
| 2-3 synthetic connectors within stated scope | Three CSV-backed Shopify/Amazon/POS adapters and generators | Add protocol-faithful Shopify GraphQL, Amazon SP-API and Kafka POS paths; clarify four-source wording versus 2-3 boundary |
| S3/MinIO and Iceberg or Delta | Working MinIO/Iceberg with ADR and recovery proof | Keep Iceberg; no format migration |
| Spark batch compute | Main compute uses PyArrow/PyIceberg | Implement actual PySpark Silver/Gold batch transformations and parity checks |
| Bronze raw/date partitioning | Immutable raw bytes; versioned event tables partition by batch_id | Safely align assessed tables with ingestion_date partitioning, retaining batch identity |
| Silver canonical model | Decimal INR, composite keys, source versions/tombstones | Preserve semantics through Spark/catalog changes |
| Four Gold marts | Reconciled commerce artifacts and cost/stock-based turnover; separate legacy Iceberg tables | Make assessed Spark Gold tables catalogued; retain exports and coherent release evidence |
| 3-5 DAGs | Four normal DAGs, actual scheduler and retry evidence | Keep meaningful DAG coverage; update affected paths |
| GE output quality | Seven retained-live suites and publication gate | Carry forward and test Spark outputs |
| Column lineage | Marquez verified 101 output-field checks | Update mappings/proof for changed stack, retain evidence |
| Basic Grafana health dashboard | No Grafana service/panels in current stack | Add small real-data Airflow health dashboard |
| Hive/Glue/Nessie catalog | Generic Iceberg REST backed by JDBC SQLite | Integrate one listed catalog and verify metadata; REST protocol alone is not that selection |
| Description/types/ownership | Catalog Markdown exists but contains stale layout/type/currency details | Register metadata in actual catalog and regenerate truthful docs |
| Terraform/Pulumi whole-stack IaC | Compose exists; no IaC implementation | Add local Terraform Docker provisioning and reproducible Compose entrypoint |
| Performance/open-table working depth | Existing snapshots/replay/decimal correctness proof | Demonstrate Spark explain/shuffle/broadcast, time travel, schema evolution and one bounded file/compaction example |
| At least one stretch | Versioned delivery contracts exist | Complete schema-as-code plus real owner approval; choose this over Feast/Flink |
| Backend deployment | Working local Compose | B1 accepts this; still satisfy/clarify universal live-URL requirement via public dashboard |

An internal compute-scope ADR cannot waive an explicit rubric requirement.
Prior test/recovery evidence remains valid for its original version; it must not
be presented as proof of Spark/catalog/IaC components that do not yet exist.

## Submission requirements

| Deliverable | Current status | Required action |
|---|---|---|
| Enforced README section list | Current README is useful but lacks the full prescribed structure, including stack table | Restore every required section; measure setup target |
| C4 Container + Component PNG/SVG and narrative | Historical diagrams/Mermaid/PDF exist | Produce final current-state L2/L3 image assets and narrative |
| Five named ADRs | More than five ADRs exist | Reconcile required decisions with actual assessed stack and alternatives |
| Thinking artifact | docs/thinking_artifact.md about 1,763 words but historical; Week 4 summary only 326 words | Choose Option C, about 1,500 words with real incidents; audit facts and path |
| Presence artifact | No docs/presence_artifact.md or completed selected option | Choose Option D: published release/README with actual GIF and required sections |
| Resume bullets in docs | Present under deliverables/week4 only | Provide canonical docs/resume_bullets.md, 3-4 accurate bullets |
| Mock interview | Missing | docs/mock_interview.md, 10 substantive Q&A |
| Postmortem | Missing canonical docs/postmortem.md | Recommended high-credit artifact using actual bugs, not invented incidents |
| Resume final Markdown + PDF | Missing | Use user's actual CV details; do not invent personal history |
| Showcase slide PNG/PDF | Missing | Produce one final slide |
| Final and milestone recordings | Written walkthrough exists; no new final video | Actual 5-minute walkthrough, 3-minute final pitch and personal 2-minute pitch |
| Personal interview package | Not assembled | Project-sheet PDF, company/role list and 30-day plan |
| Weekly issues/main commit thresholds | Three weekly Issues found remotely | Audit actual requirements; Week 4 and final publication pending; do not backfill history as on-time |
| Milestone releases | v1.0-milestone-1 exists | Preserve it; prepare final/showcase assets and authorized release/tag actions |
| Final remote CI and public URL | Local branch/evidence only; no own public frontend | Authorized publish, actual green CI and working dashboard URL |
| Self-evaluation/attendance/certificate | Personal/historical facts not verified | User/instructor verification and genuine external submissions |

## Remote submission inventory observed

Read-only inspection found:

- Week 1: https://github.com/aadhilajas/cartco-lakehouse/issues/1
  (created 2026-07-20).
- Week 2: https://github.com/aadhilajas/cartco-lakehouse/issues/2
  (created 2026-07-20).
- Week 3: https://github.com/aadhilajas/cartco-lakehouse/issues/3
  (created 2026-07-22).
- Alpha release: https://github.com/aadhilajas/cartco-lakehouse/releases/tag/v1.0-milestone-1
  (release metadata created 2026-07-22).

These dates are observed metadata, not a statement that all submission criteria
or timeliness requirements were met. No Week 4 issue/final/showcase release was
returned by the inspected collections. No external objects were created/changed.

## Ambiguities and policy boundaries

1. Four connector types versus explicit 2-3-source scope: default plan is three
   assessed protocol connectors; seek confirmation before requiring SFTP as fourth.
2. B1 allows Compose, generic weekly/milestone requirements demand a live URL:
   plan a real public dashboard plus reproducible backend; confirm this split.
3. Milestone introduction names v1.0-milestone-2, detailed/logistics sections name
   v1.0-final: use the detailed checklist provisionally and confirm if necessary.
4. The specification is for 22 June-26 July 2026, with an outside-window-work
   eligibility restriction. Current changes occur in November. Keep genuine
   timestamps and consult the instructor about revised/late-submission acceptance.
   Do not guarantee certification, invent attendance or backdate commits/issues.
5. Existing local-only authorization remains in force. The attached document is
   a source of acceptance criteria, not authorization to publish, send messages,
   create releases, sign approvals or submit forms.

Implementation order and exit criteria are in assignment-aligned-plan.md.
