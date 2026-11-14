# Infrastructure map

```text
infra/
  local/          Current protocol/Spark/Nessie application
  compatibility/ Smaller Spark/catalog acceptance foundation
  shared/         Airflow base, MinIO, Spark locks/JARs and Marquez build inputs
  baseline/       Guide to the preserved root Compose entrypoints
  terraform/      Stable Docker provider module and state/resource addresses
  hosting/        Alternative persistent-server HTTPS example
  phase1/         Legacy Python fetch-helper import only; not a stack directory
```

| Goal | Configuration | Operator entrypoint |
|---|---|---|
| Current application | [local/stack.json](local/stack.json) | `python scripts/acceptance/run_assessed_stack.py` from repository root |
| Compatibility foundation | [compatibility/stack.json](compatibility/stack.json) | `scripts/windows/Start-CartcoInfrastructure.ps1` |
| Earlier application | Root [docker-compose.yml](../docker-compose.yml) | Existing root Compose commands; [baseline guide](baseline/README.md) |
| Local Docker IaC | [terraform/](terraform/) | `scripts/windows/Start-CartcoAssessedTerraformValidation.ps1` for current application |
| Public demo | [dashboard/vercel.json](../dashboard/vercel.json) | Manually requested GitHub Actions publication, then Vercel |

Plain `docker compose up` at repository root still selects the **baseline**.
It does not silently start or migrate the current Spark application.

## Compatibility and ownership

File paths changed; resource identities did not. Current Compose remains
`cartco-assessed`, foundation Compose remains `cartco-infra-p1`, and Terraform's
project/workspace/resource names are preserved. DAG IDs and volume names are not
renamed. Do not attach an earlier stack's data volumes to a different application.

Terraform stays in `infra/terraform`; no state move or `terraform state mv` is
needed for this layout change. Its default specification is now
`../compatibility/stack.json`. Select `../local/stack.json` for the current stack.
Old variable values `../phase1/stack.json` and `../assessed/stack.json` remain
supported aliases inside Terraform, resolving to the canonical files. Old direct
Compose `-f infra/phase1/stack.json` or `-f infra/assessed/stack.json` commands must
use the new paths; this alias is for Terraform selection, not duplicate JSON files.

Builds use [shared inputs](shared/README.md). Shared changes trigger the current
and foundation CI paths. Keep dependency locks, image pins and JAR checksums;
source-trigger hashes cover both compatibility and shared build inputs.

[Current runbook](../docs/assessed-runbook.md) ·
[Hosting boundaries](../docs/vercel-demo-guide.md)

## Apply concurrency

Repository launchers and CI use `terraform apply -parallelism=1`. Keep that flag
for direct applies too: concurrent container creation against a new shared Airflow
log volume produced a Docker `scheduler/latest` symlink-already-exists error.
Serialization limits Terraform operations; running containers and their finite
initializers still initialize asynchronously and require readiness checks.
Existing volume contents, ownership and names remain intact.

See [Terraform apply options](https://developer.hashicorp.com/terraform/cli/commands/apply)
and [Docker volume population](https://docs.docker.com/engine/storage/volumes/).
