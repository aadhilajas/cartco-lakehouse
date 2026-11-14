# Script entrypoints

Run these commands from the repository root. Scripts remain at their existing
paths in this documentation pass; moving them would affect Docker COPY paths,
repository-root detection, tests and workflow commands.

| Goal | Entrypoint | Scope |
|---|---|---|
| Current full acceptance | `python scripts/run_assessed_stack.py` | Build, scheduled DAGs, quality/lineage, Grafana, recreation, replay and table checks |
| Windows equivalent | `scripts/Start-CartcoAssessedStack.ps1` | Choose `-PythonPath` explicitly if Python is not on PATH |
| Current Terraform acceptance | `scripts/Start-CartcoAssessedTerraformValidation.ps1` | Separate Terraform state/workspace; local Docker host |
| Resume supported Terraform checkpoint | `scripts/Resume-CartcoAssessedTerraformValidation.ps1` | Requires the retained accepted checkpoint; not a generic reset |
| Cold backup/restore proof | `scripts/Start-CartcoAssessedRecovery.ps1` | Existing accepted Compose stack; restored copies use separate volumes |
| Verify current catalog | `scripts/verify_assessed_catalog.py` | Runs inside the current Spark task environment |
| Source protocol acceptance | `scripts/accept_protocols.py`, `scripts/accept_native_protocols.py` | Protocol fixtures and retained manifests; inspect CLI options first |
| Local dashboard export/server | `scripts/export_dashboard.py`, `scripts/serve_dashboard.py` | Validated release input and read-only serving |
| Older compatibility foundation | `scripts/Start-CartcoInfrastructure.ps1` | Phase 1 infrastructure, not the complete current application |
| Earlier development commands | `scripts/dev.py` | Baseline Compose/generators; not the current Spark launcher |
| Earlier file-catalog recovery | `scripts/local_recovery.py` | SQLite/file integration environment; not the full current volume backup |

Inspect a Python command with `--help` before use. Most checks are designed for
container-specific dependencies and paths; their presence here does not imply
they run in a host unit-test environment.

## Public publication

Use **Publish verified demo** in GitHub Actions on `main`. It calls the current
acceptance runner, then `dashboard/prepare-publication.mjs`. Browser checks must
pass before the two public files are committed. See the
[deployment guide](../docs/vercel-demo-guide.md).

The Windows wrappers may discover a neighbouring development-workspace Python.
A fresh clone has no such environment: install Python 3.11 and use the explicit
`-PythonPath` option where offered, or invoke the documented Python runner.
Never delete volumes or reset Kafka offsets as a routine retry step.
