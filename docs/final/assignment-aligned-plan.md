> Historical planning/handoff document. These dated decisions and pending items
> record an earlier project stage. For current implementation and the selected
> public demo hosting model, use the [documentation index](../README.md) and
> [status ledger](../local-completion-checklist.md).

# CartCo revised assignment-aligned completion plan

Original review: 2026-11-11; scope/status reconciliation: 2026-11-13
Authority: user-supplied B1 assignment and 03_Deliverables_Specification.md.
This supersedes the earlier dashboard-first plan and optional-compute/IaC assumptions.
Implementation update, 2026-11-12: the isolated Phase 1 foundation, Terraform
configuration and compatibility harness passed local Compose and Terraform
apply/recreation acceptance. Hosting/account availability and ARM execution remain open. See [Phase 1 evidence](../phase1-infrastructure.md). The project advanced beyond this checkpoint on 13 November; the current
implementation and acceptance ledger is [local completion](../local-completion-checklist.md).
Native protocol/Spark/quality/lineage/browser checks, scheduled Compose, cold
restore and expanded live Terraform acceptance are now verified, including a
final no-change plan. GitHub integration, remote CI and hosted operation remain
manual/external gates.

Latest user direction: complete project work locally, then guide manual GitHub
integration and hosting. Do not publish automatically. Personal resume/interview
materials are excluded from this project scope.

## Goal and scope

Finish a defensible B1 lakehouse and the required submission artifacts. Preserve
our working Iceberg/MinIO, decimal/version contracts, GE/lineage and recovery
baseline as the regression reference. Close assigned gaps before adding a public
showcase. No Kubernetes, multi-region platform, real merchant onboarding or
sub-second streaming is needed.

## Confirmed requirements and deadline

User clarification, 2026-11-11:
- All four connectors are required: Shopify GraphQL, Amazon SP-API, Kafka POS and
  SFTP CSV. Synthetic local protocol fixtures are acceptable to the implementation
  plan; actual merchant credentials are not assumed available.
- A hosted backend is required in addition to the public dashboard. Compose alone
  is not the selected completion target for this viva.
- Viva: 1 December 2026. The user did not finish in the original submission window;
  this is a truthful completion/preparation effort for the viva, not a backdated
  on-time submission.
- The latest user instruction defers publication to a manual walkthrough after
  local completion. Nothing has been published. Budget is zero and no existing server/account resource is available. No paid
  resources, paid upgrades or billable overages are authorized.

Use all four bounded synthetic connectors. Kafka is a finite microbatch connector;
no Flink/KSQL/sub-second streaming platform is needed. SFTP supplies its declared
inventory/distributor entity, not fictitious order rows. Preserve existing stock,
cost and POS model semantics. Choose the owner-approved data-contract stretch.

## Zero-budget hosting gate

Confirmed: no paid spending and no existing server (11 November 2026).
See [hosting feasibility](hosting-feasibility.md). A free hosted backend remains
required; it is not waived or replaced by a static snapshot website. Phase 1 must
check resource limits, ARM image compatibility and measured peak memory before
committing to a provider. Oracle Always Free is a candidate, not a provisioned or
validated target. No paid upgrade, expiring trial-only architecture or overage is
part of the plan. Account creation/identity verification requires the user; no
card details or credentials should be sent in chat. If no suitable free capacity
is available, report the hosting requirement as unmet and seek institutional
resources or evaluator guidance rather than claim deployment is complete.

## Working target schedule (subject to hosting readiness)

| Target | Work |
|---|---|
| 11 November | Requirements settled; zero-budget hosting feasibility |
| 12-14 November | Phase 1 compatibility, catalog/IaC foundation and hosting feasibility |
| 15-18 November | Phase 2: all four connector protocol paths |
| 19-22 November | Phase 3: Spark medallion, catalog metadata and correctness proof |
| 23-24 November | Phase 4: assessed DAGs, Grafana, DQ/lineage and contract sign-off |
| 25-26 November | Phase 5: snapshot/dashboard integration and hosted-stack acceptance |
| 27-28 November | Phase 6 artifacts, final CI/publication and Phase 7 closeout |
| 29 November | Feature freeze and final public-link/reproduction checks |
| 30 November | Viva rehearsal and fixes only |
| 1 December | Viva |

These are planning targets, not claims of completed work or guaranteed durations.
Hosting feasibility is checked early so resource limits cannot surprise us at the
end. No new optional features are added after feature freeze.

## Phase 1 — Infrastructure and compatibility

1. Checkpoint the current tested baseline and use isolated new project/volume names.
   Never replace the original catalog or delete its volumes to introduce services.
2. Prove a pinned Spark/Python/Java/Iceberg dependency combination in a small Linux
   container smoke test. Spark must execute real DataFrame/SQL batch work, not
   merely be installed. local[2] is sufficient for the demonstration; no cluster.
3. Select one listed catalog. Prefer a Nessie compatibility spike while retaining
   Iceberg; use Hive if it is the better verified fit. Current REST/JDBC catalog
   alone is not one of the named options. Do not operate multiple catalogs for show.
4. Add Terraform for the Docker stack, usable locally and on the chosen host: images, network, persistent volumes,
   MinIO, Airflow/PySpark, chosen catalog, Marquez and required metadata services.
   Add the small Grafana/Kafka/mock-source services as later phases need them.
5. Keep a reproducible Compose entrypoint and select the smallest compatible hosted
   backend target after zero-budget feasibility and account availability checks. Terraform and Compose must not manage
   the same resources concurrently; isolate their names/state and share pinned
   build/config definitions. Review a real Terraform plan before an isolated apply;
   preserve the existing baseline and its volumes.

Exit: Spark query + Iceberg write/read through a listed catalog work on isolated
storage; Terraform/Compose entrypoints and hosting feasibility are verified.
Existing baseline is preserved. No Kubernetes or format migration is introduced.

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
6. Add the required SFTP CSV loader with incomplete-file protection and checksum/
   replay behavior, landing its declared inventory/distributor schema. Never
   relabel a local CSV reader as a tested SFTP connection.

Exit: all four required connectors execute their local protocol paths and land
queryable date-partitioned raw data. API/file/event boundaries and source entities
are explicit, with replay and failure evidence.

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
5. Prepare dashboard deployment and the separately required hosted backend. Code
   and dashboard publication are authorized after review. Keep data services,
   credentials and admin interfaces protected; do not expose internal services
   wholesale. Provision only verified free resources after account access is available.

Exit: a polished dashboard backed by actual results and a working hosted backend,
with health, pipeline execution, persistence and access boundaries verified. A
Vercel page alone does not satisfy this exit condition.

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

1. Review the actual changes, then use the user's authorization to publish ready
   code/dashboard. Resolve GitHub contents/workflow permissions if still needed.
   This planning update does not publish an unfinished branch.
2. Run final remote CI and attach actual artifacts/results. Verify 3 unit + 1
   integration minimum and final end-to-end acceptance on the assessed stack.
3. Audit main-branch commit thresholds and actual weekly Issues. Prepare the missing
   Week 4 submission accurately; do not manufacture historical weekly cadence.
4. Preserve the existing v1.0-milestone-1 release. Prepare the detailed-spec tags
   v1.0-final and v1.0-showcase with assets. The spec's introduction also mentions
   v1.0-milestone-2; confirm naming if needed rather than silently overwriting tags.
5. Publish dashboard and genuine demo/presence links; check URLs, README setup,
   screenshots, diagram assets, forms and release checklists from a reviewer view.
6. Preserve the confirmed 1 December viva target and reconcile dates/eligibility
   with the instructor. The reference specifies work
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
- User clarification now requires a hosted backend and all four connectors,
  superseding the document ambiguity and the earlier provisional three-source plan.
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
