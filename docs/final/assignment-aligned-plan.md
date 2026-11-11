# CartCo revised assignment-aligned completion plan

Reviewed: 2026-11-11
Authority: user-supplied B1 assignment and 03_Deliverables_Specification.md.
This supersedes the earlier dashboard-first plan and optional-compute/IaC assumptions.
It is a plan review, not a claim that the missing components are implemented.

## Goal and scope

Finish a defensible B1 lakehouse and the required submission artifacts. Preserve
our working Iceberg/MinIO, decimal/version contracts, GE/lineage and recovery
baseline as the regression reference. Close assigned gaps before adding a public
showcase. No Kubernetes, multi-region platform, real merchant onboarding or
sub-second streaming is needed.

Two source documents contain ambiguities:
- B1's problem statement lists four connector types, but Scope Boundaries allows
  2-3 synthetic connectors. Plan for three protocol-faithful synthetic connectors;
  mark the fourth explicitly deferred unless the evaluator requires all four.
- B1 allows live deployment OR reproducible Compose, while the general Week 4 /
  milestone spec requires a live URL. Plan a real public snapshot dashboard plus
  a fully reproducible local backend. Confirm evaluator acceptance of that split;
  a Vercel UI is not a cloud-hosted pipeline. Paid cloud is not assumed necessary.

Recommended assessed connector set: Shopify GraphQL mock API, Amazon SP-API mock
API and a basic Kafka POS connector. This reuses the existing POS canonical model.
Synthetic data is allowed; CSV generators alone do not demonstrate those protocol
behaviors. Add SFTP as a bounded fourth loader only if the four-source wording is
confirmed mandatory. Existing stock/cost fixtures remain declared input evidence.
No Flink/KSQL or continuous streaming service is needed; finite Kafka microbatches
with correct offset handling cover the connector. Use the data-contract stretch.

## Phase 1 — Infrastructure and compatibility

1. Checkpoint the current tested baseline and use isolated new project/volume names.
   Never replace the original catalog or delete its volumes to introduce services.
2. Prove a pinned Spark/Python/Java/Iceberg dependency combination in a small Linux
   container smoke test. Spark must execute real DataFrame/SQL batch work, not
   merely be installed. local[2] is sufficient for the demonstration; no cluster.
3. Select one listed catalog. Prefer a Nessie compatibility spike while retaining
   Iceberg; use Hive if it is the better verified fit. Current REST/JDBC catalog
   alone is not one of the named options. Do not operate multiple catalogs for show.
4. Add Terraform for the local Docker stack: images, network, persistent volumes,
   MinIO, Airflow/PySpark, chosen catalog, Marquez and required metadata services.
   Add the small Grafana/Kafka/mock-source services as later phases need them.
5. Keep a reproducible Compose entrypoint. Terraform and Compose must not manage
   the same resources concurrently; isolate their names/state and share pinned
   build/config definitions. Do not run terraform apply in this planning turn.

Exit: Spark query + Iceberg write/read through a listed catalog work on isolated
storage; local Terraform/Compose entrypoints are defined and tested. Existing
baseline is preserved. No AWS/Kubernetes or format migration is introduced.

## Phase 2 — Assessed ingestion and Bronze alignment

1. Add a synthetic Shopify GraphQL server/client path with cursor pagination,
   bounded timeout/retry handling and raw response preservation.
2. Add a synthetic Amazon SP-API server/client path with next-token pagination,
   source versions, throttling/retry behavior and explicit incremental windows.
   No real merchant tokens or business verification are required for the fixture.
3. Add one local Kafka broker and POS producer/consumer. Consume a bounded batch;
   checkpoint/commit offsets only after durable accepted landing, retain rejection
   evidence and prove replay does not duplicate records. No sub-second SLA.
4. Partition the assessed Bronze tables by ingestion date, retaining batch IDs
   for immutable evidence/idempotency. Current versioned tables partition by
   batch_id, so introduce an explicit safe evolution/new-table migration rather
   than silently claiming existing layout satisfies the requirement.
5. Preserve current immutable archives/contracts and demonstrate full versus
   incremental updates, late arrivals, duplicate events and tombstones.
6. If all four named connectors are required, add a small SFTP CSV loader with
   incomplete-file protection and checksum/replay behavior. Keep this decision
   visible; never relabel a local CSV reader as a tested SFTP connection.

Exit: the selected 2-3 assessed connectors execute their real local protocol paths
and land queryable date-partitioned raw data. The source-count ambiguity is
resolved or explicitly recorded for evaluator confirmation.

## Phase 3 — Actual Spark medallion and catalog proof

1. Implement Silver normalization/dedup/version handling using Spark DataFrames
   and windows, preserving composite keys, exact Decimal types, currency, timezone
   rules and tombstones. Equal-version conflicting events must fail deterministically.
2. Build the four assigned Gold marts using Spark aggregates/joins. Keep our metric
   definitions and exact financial/inventory reconciliation. Preserve Python
   reference functions as regression oracles, not as the hidden main compute path.
3. Store assessed Silver/Gold results as catalogued Iceberg tables on MinIO.
   Existing coordinated Parquet releases remain useful exports/evidence, but
   cannot alone be presented as the catalogued Gold layer. Preserve a coherent
   release manifest/snapshot boundary for consumers across independently written marts.
4. Register actual table descriptions, typed columns and ownership in the selected
   catalog and generate docs from live metadata. Fix stale USD/type/local-file-link
   statements in docs/catalog.md. A Markdown catalog alone is not catalog integration.
5. Prove time travel and safe additive schema evolution on a disposable table;
   reject incompatible evolution. Capture an explain plan, partition pruning,
   shuffle/broadcast-join example and file-size/compaction evidence with unchanged
   business rows. Do not add bucketing/Z-ordering where it is not appropriate.

Exit: actual Spark Bronze/Silver/Gold queries reconcile to the reference outputs;
listed-catalog metadata and open-table behaviors have executable proof. Changes
rerun affected tests; old-engine recovery proof is not automatically reused as
proof of the changed stack.

## Phase 4 — Orchestration, observability and selected stretch

1. Retain 3-5 meaningful assessed DAGs with actual dependencies and source readiness,
   retries, idempotency, timeouts/freshness expectations and a sensor where relevant.
   The current repository already has four normal DAGs; avoid artificial DAGs
   created solely to increase the count. Update their implementation as needed.
2. Carry current Silver/Gold GE suites and fail/warn policies into the Spark path.
   Preserve persisted reports and test an actual failing-output gate.
3. Retain real OpenLineage column derivations and Marquez backend checks; update
   mappings/snapshot identities for the changed compute/catalog. Installing an
   emitter or attaching schemas alone is not column-derivation proof.
4. Add basic Grafana health panels using a read-only Airflow metadata source:
   recent DAG states, durations, failures and latest success. Use real records;
   no new Prometheus/observability platform is required just to satisfy the brief.
5. Select exactly one stretch: schema-as-code data contract with declared owner
   and an actual owner approval/sign-off record. Existing delivery contracts give
   a strong base, but missing ownership approval must not be invented. No Feast
   or Flink implementation is needed for this choice.

Exit: assessed DAGs, DQ failure, real lineage and Grafana panels work on the changed
stack; the contract stretch has genuine owner approval. One final relevant
integration/recovery check covers changed components, not endless new fault campaigns.

## Phase 5 — Snapshot serving and public dashboard

1. Export one versioned, privacy-safe JSON snapshot from a coherent release and
   real run/DQ/lineage evidence. Keep Decimal arithmetic in Python; preserve zeros,
   missing/stale states, synthetic labels and distinct batch/export/run timestamps.
2. Build a small original React/Vite frontend with Business, Engineering and Lineage
   tabs. Use existing metrics, sensible filters, INR and clear definitions.
   Do not display invented reserved-stock/top-product/duration/success-rate values.
3. Add atomic validated snapshot export after successful publication. Refresh
   reloads published data; pipeline execution remains in Airflow/manual CI.
   Do not expose an anonymous public workflow-trigger endpoint.
4. Test exporter/contract/filtering/error behavior and build the frontend. Perform
   one pipeline-to-export-to-UI acceptance; show a bad export retaining valid data.
5. Prepare Vercel deployment for the dashboard. Keep backend endpoints and secrets
   private. Publish only after user authorization; local-only remains in force.

Exit: a polished working dashboard backed by actual results, not hard-coded
engineering/lineage panels. This supports product sense and the generic live URL
requirement without unnecessary cloud infrastructure, subject to evaluator acceptance.

## Phase 6 — Complete the actual written/media deliverables

1. Restore every mandatory README section: title/tagline, Demo, business problem,
   architecture, stack table, prerequisites/install/run/test, data, ADRs,
   limitations, roadmap, license/acknowledgements. Add why-built and hiring sections
   plus a genuine demo GIF for Presence Option D. Test the declared <15-minute
   clone-to-run target; report cold/cached conditions rather than guessing.
2. Produce C4 Level 2 Container and Level 3 Component PNG/SVG diagrams plus the
   one-page narrative. Current historical Mermaid/PDF diagrams are not the final
   required image set. Diagrams must match the actual assessed stack.
3. Finalize at least the five named ADRs in standard format with alternatives.
   Revise compute/catalog/ingestion/partition decisions to actual implementation.
   Supersede the earlier ADR-012 assumptions about optional Spark/Terraform/Grafana.
4. Choose Thinking Option C: an approximately 1,500-word postmortem/lessons artifact
   using genuine dependency, snapshot-history and recovery incidents. The short
   Week 4 summary is only 326 words. An older docs/thinking_artifact.md is 1,763
   words but needs accuracy review; length alone does not make it final.
5. Choose Presence Option D: public release with a strong README, real GIF,
   architecture, why-built and hiring sections. This avoids requiring an unrelated
   new blog platform or multiple extra videos. Add docs/presence_artifact.md with
   the actual published evidence after authorization.
6. Add docs/mock_interview.md with 10 substantive Q&A; docs/resume_bullets.md with
   3-4 accurate bullets; optional high-credit docs/postmortem.md. Avoid duplicate,
   inconsistent copies across docs and deliverables.
7. Prepare resume_final.md and PDF using the user's genuine CV details, a one-slide
   showcase PNG/PDF, a one-page project sheet PDF, 20 company/role targets and a
   30-day development plan. Do not invent education, employment or achievements.
8. Record the actual 5-minute walkthrough, 3-minute final pitch and 2-minute personal
   interview pitch as required. Reuse a recording session where practical, but a
   written walkthrough is not a recording. No fabricated/pending-as-live URLs.
9. Prepare self-evaluation answers and a submission checklist; user supplies genuine
   self-ratings/attendance and submits the external form. No form is submitted or
   message sent on the user's behalf without explicit authorization.

Exit: required paths, lengths, image/PDF formats and real recordings exist; each
claim has evidence. All personal details and signatures are genuine.

## Phase 7 — Authorized publication and submission closeout

1. Review local changes, then obtain authorization to publish; resolve GitHub
   contents/workflow permissions if still needed. Do not push during plan review.
2. Run final remote CI and attach actual artifacts/results. Verify 3 unit + 1
   integration minimum and final end-to-end acceptance on the assessed stack.
3. Audit main-branch commit thresholds and actual weekly Issues. Prepare the missing
   Week 4 submission accurately; do not manufacture historical weekly cadence.
4. Preserve the existing v1.0-milestone-1 release. Prepare the detailed-spec tags
   v1.0-final and v1.0-showcase with assets. The spec's introduction also mentions
   v1.0-milestone-2; confirm naming if needed rather than silently overwriting tags.
5. Publish dashboard and genuine demo/presence links; check URLs, README setup,
   screenshots, diagram assets, forms and release checklists from a reviewer view.
6. Reconcile dates/eligibility with the instructor. The reference specifies work
   from 22 June to 26 July 2026 and excludes outside-window work for certification;
   current improvements are in November. Preserve real dates and seek acceptance
   of a revised/late portfolio submission. Never backdate work or guarantee eligibility.

Exit: technical rubric and required submission artifacts are evidenced, public
links work, and remaining instructor decisions are explicit. Stop adding features.

## What changes from the previous plan

- Spark is now a required implementation gap, not merely a documented preference.
- Terraform/Pulumi IaC and basic Grafana move into the core plan.
- A listed catalog and actual metadata registration are required; generic REST/JDBC
  plus documentation is not assumed sufficient.
- Protocol connector behavior and ingestion-date Bronze partitioning need alignment.
- The assessed Gold layer must be queryable/catalogued; serving exports are separate.
- Backend cloud deployment is not assumed mandatory because B1 explicitly accepts
  reproducible Compose. The generic public URL is addressed separately.
- A stretch must actually be completed; choose an owner-approved data contract.
- Written/media deliverables are substantially expanded and checked against exact
  paths/lengths/formats. Dashboard work follows the missing technical requirements.

## Stop list

No multi-region/HA platform, Kubernetes, distributed cluster, real production
merchant credentials, 100-source system, sub-second SLA, Feast/Flink expansion,
public unauthenticated task execution, or new tests/failure campaigns without
requirement/change/defect justification. Local Spark, one catalog, one broker,
one Grafana and the chosen stretch are enough. All past proof remains scoped to
the version that produced it.
