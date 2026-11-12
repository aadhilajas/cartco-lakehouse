# Phase 1 infrastructure and compatibility

Updated: 2026-11-12. Status: implementation prepared, live acceptance pending.

This foundation closes infrastructure gaps identified in the assignment review.
It preserves the original `docker-compose.yml`, its containers, catalog, data and
volumes. It is not a claim that the new commerce models/connectors are complete.

## What is implemented

- [Shared stack specification](../infra/phase1/stack.json): MinIO, persistent
  Nessie/PostgreSQL, Airflow/PostgreSQL, Marquez/PostgreSQL/UI, plus an opt-in Spark
  acceptance job. All published ports bind loopback. No Docker socket is mounted
  into Airflow. Memory caps bound the containers; they are not measured usage.
- [Spark image](../infra/phase1/Dockerfile.spark): Python 3.11.14, Java 17,
  Spark 3.5.6/Scala 2.12, checksum-locked Iceberg 1.12.0 jars. Image digests and
  Python package hashes are pinned. Debian security packages are installed from
  the distribution repositories at build time; the Java patch level is recorded
  by acceptance, not falsely described as bit-for-bit frozen.
- [Terraform](../infra/terraform/main.tf): the same images/services, isolated
  network and six protected persistent volumes, with content rebuild triggers.
- [Acceptance](../scripts/phase1_spark_smoke.py): actual Spark SQL write/read,
  decimal aggregation, MERGE, ingestion-date partitioning, schema evolution and
  time travel; a second read-only run compares persisted identity and data.
- [Windows/PowerShell host runner](../scripts/Start-CartcoInfrastructure.ps1):
  builds, health checks, container recreation, retained evidence and baseline
  identity comparison. It also runs under PowerShell on Linux in the prepared CI.

## Compose acceptance

Prerequisites: Docker Desktop with the Linux engine running, at least 10 GB free
on the repository drive (15 GB preferred for build caches), sufficient engine
memory and network access to registries, Maven, PyPI and pinned MinIO sources.
The first source build can be slow; no cold-start timing is claimed yet.

From normal PowerShell:

```powershell
& 'C:\Users\abiaf\Documents\Codex\2026-11-09\i-x20\cartco-lakehouse\scripts\Start-CartcoInfrastructure.ps1'
```

Evidence is written to `test-results/infrastructure/<timestamp>/`. A `SUCCESS.txt`
is written only after the full run passes. A previous success in a different
directory does not validate a new build. Failures retain logs and volumes.
The script never runs down -v, prune, baseline stop, baseline recreate or volume
deletion. An existing baseline is inventoried by container identity before and
after; comprehensive old-data hashes remain the earlier baseline evidence.

For a verified repeat without source changes, `-SkipBuild` reuses the images.
After a code or dependency change, rebuild. Every creation pass uses a fresh
probe table; the restart pass reads the corresponding evidence.

New local endpoints: Airflow 18080, Nessie 19120, MinIO S3 19000/console 19001,
Marquez API 15000 and web 13000. Fixture credentials in the specification are
for this loopback-only stack. Replace them and add authenticated HTTPS before
any public deployment. Airflow's local administrator is airflow/airflow.

To stop only the new Compose stack and retain its containers/volumes:

```powershell
docker compose -p cartco-infra-p1 -f infra/phase1/stack.json stop
```

## Terraform alternative

Use Terraform **1.13.5** and the committed provider lock. Do not run it while the
new Compose stack is active: their host ports overlap intentionally, although
their resource names/volumes are separate. The original baseline uses different
ports. Terraform state/provider directories and variable files are ignored.
State still contains fixture configuration and must be kept private.

```powershell
terraform -chdir=infra/terraform init -backend=false
terraform -chdir=infra/terraform fmt -check -recursive
terraform -chdir=infra/terraform validate
terraform -chdir=infra/terraform test
terraform -chdir=infra/terraform plan -var='docker_host=npipe:////./pipe/dockerDesktopLinuxEngine' -var='buildx_builder=desktop-linux'
terraform -chdir=infra/terraform apply -var='docker_host=npipe:////./pipe/dockerDesktopLinuxEngine' -var='buildx_builder=desktop-linux'
```

On Linux, omit the Windows endpoint/builder arguments for the local Docker
socket and default Buildx builder. The provider explicitly uses Buildx rather
than relying on a legacy Docker image builder. The named builder must target the
same Docker engine as the provider endpoint. For a
remote host, configure the Docker provider endpoint/authentication appropriately;
do not expose an unauthenticated Docker TCP API. Checkout/build context must be
available on the Terraform machine. No cloud VM provisioning or paid resource
is included. Review the real plan before apply. Mock-provider tests create no
containers and do not count as an apply acceptance run.

Services may start before peers under Terraform. The initializers retry and the
Airflow runtime waits for migrations; the Compose runner additionally uses
explicit dependency health conditions. Verify service health after apply. The
default Terraform deployment does not execute the optional Spark job. To request
that one-shot probe, set `enable_acceptance=true`; keep its evidence volume.
Do not use the Compose runner against Terraform-owned resources. A real Terraform
apply, successful probe and Terraform-controlled recreation proof are still
required before declaring that deployment path accepted. The prepared
`terraform-live` CI job provisions this path, then runs:

```bash
python scripts/phase1_terraform_acceptance.py
```

The host validator needs Python 3.11+ (stdlib only). On Windows, pass the same
`--docker-host npipe:////./pipe/dockerDesktopLinuxEngine` and
`--buildx-builder desktop-linux` used for apply, plus explicit executable paths
if Docker/Terraform are not on PATH. It refuses baseline project names, uses
Terraform to recreate only the new storage/catalog containers and preserves their
named volumes. The workflow has not run remotely yet.

## Validation ledger

- Unit suite: 287 passed, 1 Windows-only skip (288 collected).
- Terraform fmt and validate passed; two mock-provider plan tests passed.
- Docker Compose config validation passed.
- Live Nessie API v2 and Airflow scheduler/database health verified. The paused
  diagnostic DAG was triggered once and succeeded; its original pause was
  restored. Evidence: outputs/cartco-phase1-diagnostic-dag.json outside the repo.
- PowerShell host runner parsed successfully.
- Image registry digests/platform manifests audited in
  [image-platforms.json](../infra/phase1/image-platforms.json).
- Container build, real Spark queries, JDBC2 persistence/recreation, peak memory,
  Terraform apply and ARM execution are pending until actual evidence is retained.

The agent cannot access the host Docker engine from this sandbox. On 12 November,
about 19 GB free disk was confirmed after the user freed space. Docker Desktop
was not running; a host-side launch/run was requested. Earlier incomplete native
dependency downloads are not treated as installed runtimes or acceptance proof.

## Hosting gate

See [hosting feasibility](final/hosting-feasibility.md). The current pinned
Marquez images advertise amd64 only. ARM-capable base manifests for other services
do not solve that gap. No free VM/account/capacity, ARM build or hosted URL is
verified. This remains an explicit Phase 1 exit condition rather than a promise.

Decision and alternatives: [ADR-013](adr/ADR-013-spark-nessie-infrastructure.md).

## Resource sampling

New host-runner invocations sample Docker memory during Spark execution into
resource-samples.jsonl. Summarize with:

```bash
python scripts/summarize_infrastructure_resources.py PATH/resource-samples.jsonl --output PATH/resource-summary.json
```

The report uses simultaneous samples, not a sum of unrelated per-container
maximums. It excludes OS memory and unsampled peaks. The first host run started
before the sampler was added; its post-job stats alone cannot prove peak usage.
A cached repeat with the updated runner can gather this evidence.

## First live run and configuration correction

Run 20261112-184728 built MinIO, Spark and Airflow images successfully, with
pip check and both Iceberg JAR checksums passing. Nessie JDBC2 and all three
PostgreSQL stores started; Airflow's diagnostic task succeeded. The run correctly
failed before Spark execution because Marquez could not authenticate to its DB.
The upstream development YAML hard-codes marquez/marquez and ignores the supplied
POSTGRES_PASSWORD. No data reset is needed.

The fix adds an explicit environment-backed Marquez configuration in a small
derived image. Its database credentials match the already-initialized store;
unused OpenSearch integration is disabled. To rebuild only that layer and rerun:

```powershell
& 'C:\Users\abiaf\Documents\Codex\2026-11-09\i-x20\cartco-lakehouse\scripts\Start-CartcoInfrastructure.ps1' -SkipBuild -RepairMarquez
```

Cached acceptance requires 2 GB free; full image builds require 10 GB. This retry
also uses the updated memory sampler. The failed run is retained as diagnostic
evidence and is not counted as Spark or complete-stack acceptance.
