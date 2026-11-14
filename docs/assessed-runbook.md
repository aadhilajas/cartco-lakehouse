# Assessed project: local runbook

This is the synthetic protocol/Spark implementation. It uses an isolated
`cartco-assessed` project and its own nine named volumes. The original Compose
application and the earlier Phase 1 Compose/Terraform stores remain preserved.
The assessed specification is `infra/assessed/stack.json`; do not replace the
original `docker-compose.yml` or reuse its volumes.

The current fixture is explicitly dated **12 November 2026**. It is not a claim
of live merchant connectivity or current-day business data. Shopify and Amazon
exercise local API shapes; Kafka and SFTP use real local protocols.

## Prerequisites

- A running Linux Docker engine, Compose v2+, Git and host Python 3.11+.
- At least 8 GiB free for the new build; retain all existing data/volumes.
- Network access for the pinned images, Python/npm packages and Maven artifacts.
- Sufficient host memory for the capped services and one Spark task at a time.
  Container caps are not measured usage or a cloud sizing guarantee.

The Windows wrapper uses the existing workspace Python if available, otherwise
`python` on PATH. A fresh clone does not include that workspace environment.

## One acceptance entrypoint

From the repository in normal PowerShell:

```powershell
& .\scripts\Start-CartcoAssessedStack.ps1
```

On Linux, or with a chosen host Python:

```bash
python scripts/run_assessed_stack.py
```

The runner saves full logs, inventories original baseline container identities,
builds the parent and assessed images, then stops only earlier new Phase 1
containers to free overlapping ports. It retains their volumes. It starts the
assessed services, checks real DAG imports/cycles/serialization and isolated
Spark dependencies, and triggers these actual scheduled DAGs in order:

1. `assessed_orders_ingestion`: Shopify GraphQL, Amazon Orders/OrderItems and POS
   Kafka durable landing; completion manifest after all three tasks succeed.
2. `assessed_inventory_ingestion`: host-key-verified SFTP inventory delivery.
3. `assessed_commerce_release`: readiness sensor, manifest checks, Spark medallion,
   independent exact reconciliation plus GE, publication, Marquez verification
   and atomic public snapshot export.
4. `assessed_system_health`: read-only dependency and published-manifest audit.

It restores each DAG's previous pause state. The demonstration is manually
triggered; it does not pretend a frozen fixture is a fresh production schedule.
An isolated Spark virtual environment inside the bounded task container keeps
Airflow's dependency environment separate. No Docker socket is mounted into it.

Evidence is under `test-results/assessed/<timestamp>/`. Only a completed run
writes `SUCCESS.txt`. Failed logs and data remain for diagnosis. The wrapper's
`-SkipBuild` is only for retrying unchanged, already-built sources.

## Local interfaces

| Interface | Loopback URL | Fixture credentials |
|---|---|---|
| Read-only React dashboard/API | http://127.0.0.1:18100 | Public synthetic snapshot only |
| Airflow | http://127.0.0.1:18080 | airflow / airflow |
| Grafana | http://127.0.0.1:13030 | cartco / cartco-grafana-local-only |
| Marquez | http://127.0.0.1:13000 | Local UI |
| Marquez API | http://127.0.0.1:15000 | Private local API |
| MinIO console | http://127.0.0.1:19001 | cartco-local / cartco-local-secret |
| MinIO S3 | http://127.0.0.1:19000 | Same local fixture credentials |
| Nessie API | http://127.0.0.1:19120/api/v2 | Private local API |

Kafka, SSH/SFTP and PostgreSQL are not published on host ports. These credentials
are synthetic local defaults, not secure public deployment settings.

Grafana reads a dedicated PostgreSQL view containing only DAG IDs, run IDs,
states and timing columns. Its role cannot read raw DAG configuration or write
the view. Its five panels show actual Airflow records; a zero/empty history is
not replaced with invented success rates or durations.

## Correctness and publication

New Bronze tables are ingestion-date partitioned. Wire response/message/file
bytes remain SHA-256-addressed and queryable separately from source-shaped rows.
Silver uses source versions, retained tombstones, exact decimal INR and stable
channel/order keys. Equal-version conflicts fail; old events cannot resurrect
deletions. A state identity fence refuses loss/replacement of a known catalog
state instead of silently initializing an empty state.

Gold is computed with Spark aggregates, windows and joins. The four core marts
are daily revenue, channel performance, customer 360 and actual inventory
turnover; the existing category unit-sales output is retained as a fifth table.
Each release has immutable Gold table names and pinned snapshot identities.
The existing independent Python/PyArrow calculations verify candidate results.
Seven GE suites then gate one atomic `CURRENT.json` pointer. Failed quality or
bad export leaves the last valid published result intact.

Inventory turnover uses declared fixed standard cost and daily stock, not
revenue divided by sales units. Missing daily observations and zero inventory
produce explicit unavailable ratios. Customer identities remain channel scoped.
Public exports omit raw payloads, customer IDs, credentials and local file paths;
64-bit snapshot numbers are exact strings. Refresh only reloads data.

## Reuse and preserve

```bash
docker compose -p cartco-assessed -f infra/assessed/stack.json stop
docker compose -p cartco-assessed -f infra/assessed/stack.json up -d
```

Do not use `down -v`, prune, delete metadata, reset offsets or clear the state
fence to repair a failed run. Kafka checkpoints require their retained local
landing receipts. Catalog, object store, Kafka and runtime/publication state
must be preserved together. The original SQLite/file recovery tool does not
automatically qualify as a backup/restore tool for this new stack.

## Terraform alternative

The module reads the same assessed specification with `stack_spec`. Keep its
state separate from the accepted foundation state. With Terraform 1.13.5:

```bash
terraform -chdir=infra/terraform init -backend=false
terraform -chdir=infra/terraform workspace select -or-create assessed
terraform -chdir=infra/terraform plan -var='stack_spec=../assessed/stack.json' -var='project_name=cartco-infra-p1-tf-assessed' -out=assessed.tfplan
```

Review the actual plan before applying. On Windows add
`-var='docker_host=npipe:////./pipe/dockerDesktopLinuxEngine'` and
`-var='buildx_builder=default'`. First stop the competing new Compose stack while
retaining its volumes. Do not let Terraform and Compose manage one resource set.
Do not apply a changed specification to the earlier foundation's default state.
Volume `prevent_destroy` protection is deliberate. Three mock plans validate
wiring; they do not count as a live assessed Terraform apply.

## Validation boundary

Native protocol, Spark/catalog, GE, backend lineage and dashboard browser checks
have passed. Expanded scheduled-container/Grafana Compose acceptance passed in
`test-results/assessed/20261113-203545`, with four successful DAGs, real Grafana
queries, limited-role refusal checks, exact recreation/replay and compaction.
See `local-completion-checklist.md` for the separate recovery, Terraform and
external hosting gates.

## Controlled recreation and coordinated recovery

The host acceptance runner now checks all published catalog UUIDs, metadata
locations, schemas and snapshots plus exact Silver/Gold rows before and after
recreating only assessed catalog/storage containers. It then schedules the same
immutable deliveries again and requires unchanged business outputs and retained
Bronze/Silver identities. These checks passed in the retained Compose host result above.

After that run succeeds, a separate cold recovery proof can be run from the repo:

```powershell
& '..\work\phase1-locked-venv\Scripts\python.exe' -X utf8 scripts/prove_assessed_recovery.py --output test-results/assessed-recovery/proof-1
```

Docker must be on PATH; otherwise add `--docker` with its executable path. Choose
a new empty output directory each time. This stops only `cartco-assessed`, archives
all eight active coordinated volumes (the optional smoke-only volume is excluded), verifies archive hashes, restores into a fresh
separate recovery project, and verifies the same publication, catalog identities
and exact data. It stops the recovery copy and restarts the original stack. Both
copies and archives remain available. It never deletes or overwrites existing
volumes. Plan disk space for the additional copy. Require a retained `SUCCESS.txt`: the coordinated Compose recovery proof passed
in `test-results/assessed-recovery/20261113-205004`. This proves the tested
local cold-copy method; it does not prove offsite/cloud disaster recovery.

To execute the same scheduled/data/recreation acceptance through the real
Terraform provider, use the same PowerShell wrapper with the pinned executable:

```powershell
& '.\scripts\Start-CartcoAssessedStack.ps1' -TerraformPath ((Resolve-Path '..\work\infra-phase1\terraform\terraform.exe').Path)
```

This selects the separate `assessed` workspace, saves and applies a real plan,
uses its own container/volume prefix, and restores the previously selected
workspace afterward. It stops competing new Compose/Phase 1 services to free
their ports. No original baseline resource is managed or deleted. Its separate live result passed in `test-results/assessed/20261113-210244`,
including exact replay, compaction and a final no-change plan.

The Windows checkout also includes no-flag wrappers for these two follow-up
checks. Run the recovery proof while the Compose stack is active, before
switching to Terraform:

```powershell
& '.\scripts\Start-CartcoAssessedRecovery.ps1'
& '.\scripts\Start-CartcoAssessedTerraformValidation.ps1'
```

Each wrapper saves a separate dated result directory and retains volumes.

The final successful Terraform run resumed after the completed recreation using
`Resume-CartcoAssessedTerraformValidation.ps1`. This preserves earlier plans/logs,
checks successful prerequisites and unchanged compute identity, reconciles cached
images, then finishes verification/replay/compaction/convergence. It clears prior
cookies before obtaining a fresh CSRF login form and restores the previously
selected Terraform workspace. It does not repeat ingestion or cold recovery.
