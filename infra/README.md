# Infrastructure map

Three separate configurations coexist. Choose the current application explicitly;
plain `docker compose up` at repository root selects the preserved baseline.

| Configuration | Purpose | Entrypoint |
|---|---|---|
| `assessed/stack.json` | Current protocol/Spark/Nessie application | `python scripts/run_assessed_stack.py` from repository root |
| `phase1/stack.json` | Smaller Spark/catalog compatibility foundation | `scripts/Start-CartcoInfrastructure.ps1` |
| Root `docker-compose.yml` | Earlier REST-catalog/PyArrow application | Root Compose commands; baseline CI |
| `terraform/` | Docker resources on an existing host | Current application's Terraform wrapper or explicit reviewed variables |
| `hosting/Caddyfile.example` | Alternative persistent-server HTTPS example | Not used by the Vercel demonstration |

## Ownership and dependencies

- Current Compose uses project `cartco-assessed`; current Terraform uses an
  isolated workspace/project ending in `assessed`. They must not own the same
  resources concurrently. The [runbook](../docs/assessed-runbook.md) gives details.
- Terraform defaults select the **foundation**, not the current application.
  Use `Start-CartcoAssessedTerraformValidation.ps1` for the current path.
- `phase1/` is not disposable legacy material: current Docker builds use its
  Spark locks, JAR manifest, fetch helper and Marquez configuration layer.
- Root Dockerfiles are shared build inputs. Moving them requires updating
  specifications, build contexts and Terraform source triggers together.
- Keep Docker project/volume names, Terraform addresses and Airflow DAG IDs
  stable. A folder rename does not authorize a data migration or new resource set.

The public site uses [dashboard/vercel.json](../dashboard/vercel.json), not Caddy
or a persistent cloud VM. GitHub Actions starts a temporary stack per publication.
Local cold restore and storage recreation do not establish persistent cloud
storage. See [deployment guide](../docs/vercel-demo-guide.md).
