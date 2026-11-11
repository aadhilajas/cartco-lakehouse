# ADR-012: Local completion scope and compute choice

Status: Partially superseded by the actual B1 assignment review, 2026-11-11

The newly supplied authoritative brief explicitly requires Spark, IaC, basic
Grafana and a listed catalog. The earlier optional-compute/IaC interpretation
below is historical and does not waive those requirements. See
[assignment gap matrix](../final/assignment-gap-matrix.md) and
[revised plan](../final/assignment-aligned-plan.md). User clarification now requires all four connectors and a hosted backend.
Reviewed code/dashboard may be published when ready; budget is zero with no existing server; free hosting feasibility is open. The target viva is 1 December 2026. Earlier local-only
paragraphs below are historical, not current authorization constraints.

Date: 2026-11-11

The original design promises three synthetic connectors, a medallion pipeline,
Airflow retries/idempotency/alerts, Silver/Gold GE checks, lineage and Compose.
Its selected compute is PySpark; its final week also includes deployment and
presentation deliverables. Kafka and Feast are bonuses, Terraform is stretch,
and more than three sources are excluded from the initial build.

The implemented sources are Shopify, Amazon and single-product POS. All are
synthetic; delivered mode consumes their versioned CSV delivery contracts.
SFTP/ERP and real API credentials are extensions, not prerequisites for this
three-source implementation. Inventory inputs use a separately declared scope
and standard-cost ledger, with synthetic provenance retained where applicable.

Keep PyArrow/PyIceberg for this local implementation. Existing transformations
and transaction/recovery tests exercise these APIs directly, and no implemented
job requires a distributed engine at the demonstrated scale. Replacing them
solely to name Spark adds runtime/packaging work without demonstrated capacity
need. This is an explicit deviation from the original compute selection, not a
claim that PySpark migration is complete. Revisit it after workload/capacity
requirements justify distributed compute; preserve business contracts then.

Airflow callbacks persist local retry/failure alerts and task logs. The controlled
worker-kill drill established a real retry callback. This is the selected local
alert channel, not email/Slack paging. No external recipient or delivery
authorization was supplied. External operational notification routing remains a
deployment configuration requirement.

Keep all changes local under the user's standing instruction. Do not claim cloud
deployment, a final remote CI run, GitHub publication or a newly recorded/uploaded
video. Provide reproducible scripts, architecture, test/recovery evidence,
walkthrough, thinking artifact and resume bullets. These are reviewable local
deliverables; they do not substitute for an uploaded recording or deployment.

The local baseline is complete only after current GE/lineage regression checks
pass and the final evidence is reconciled. The original deployment milestone
remains explicitly open until a cloud target, account, budget and authorization
are available. Do not describe this project as a production service or treat
additional enterprise features as mandatory internship requirements.
