# Script entrypoints

Use the grouped paths below for new commands. The small files directly under
`scripts/` preserve existing CLI/import paths; they forward to the same grouped
implementation rather than maintaining a second copy of the logic. The complete
mapping is in [layout.json](layout.json).

```text
scripts/
  acceptance/   Protocol, current stack, table-behavior and foundation acceptance
  deployment/   Deployment adapter and database/role initializers
  diagnostics/  DAG, catalog, quality, lineage and runtime inspections
  recovery/     Cold restore, volume inventories and baseline worker drills
  fixtures/     Synthetic fixture preparation
  development/  Earlier local generators/state/release development commands
  runtime/      Spark/export/publication/server task entrypoints
  windows/      PowerShell launchers and executable discovery
  ci/           Baseline CI shell orchestration
```

## Recommended commands

Run from the repository root unless a command takes explicit paths.

| Goal | Canonical entrypoint | Scope |
|---|---|---|
| Current full acceptance | `python scripts/acceptance/run_assessed_stack.py` | Real scheduled DAGs, quality/lineage, Grafana, recreation, replay and table checks |
| Windows equivalent | `scripts/windows/Start-CartcoAssessedStack.ps1` | Current Compose application |
| Current Terraform acceptance | `scripts/windows/Start-CartcoAssessedTerraformValidation.ps1` | Separate local Docker state/workspace |
| Resume retained Terraform checkpoint | `scripts/windows/Resume-CartcoAssessedTerraformValidation.ps1` | Requires the completed recreation checkpoint; not a reset |
| Cold restore proof | `scripts/windows/Start-CartcoAssessedRecovery.ps1` | Previously accepted Compose application; restored copies use separate volumes |
| Protocol checks | `scripts/acceptance/accept_protocols.py`, `accept_native_protocols.py` in the same directory | Actual bounded synthetic protocols |
| Catalog verification | `scripts/diagnostics/verify_assessed_catalog.py` | Current Spark task environment |
| Read-only serving | `scripts/runtime/serve_dashboard.py` | Validated snapshot and built website |
| Smaller foundation | `scripts/windows/Start-CartcoInfrastructure.ps1` | Phase 1 compatibility, not the full application |
| Earlier development | `scripts/development/dev.py` | Preserved baseline commands |
| Earlier file-catalog recovery | `scripts/recovery/local_recovery.py` | SQLite/file integration environment, not full volume recovery |

Inspect Python commands with `--help`; some diagnostics intentionally execute
container-specific checks at startup instead of providing a general host CLI.
Do not run a container check in an unrelated host dependency environment.

## Windows tool selection

Launchers accept explicit `-PythonPath` and/or `-TerraformPath` where applicable;
Docker-consuming launchers accept `-DockerPath`. Python selection checks for
Python 3.11 in this order: explicit path, repository `.venv`, active virtual
environment, PATH, then the optional older workspace tool as a last resort.
Terraform prefers an explicit path or PATH before its older workspace fallback.
A normal clone does not require a neighbouring Codex workspace.

Example with an explicitly chosen environment:

```powershell
& .\scripts\windows\Start-CartcoAssessedStack.ps1 -PythonPath 'C:\Python311\python.exe'
```

Replace the example Python path with a real installed interpreter. Windows Store
app aliases are not treated as working interpreters. A supplied invalid path
fails rather than silently selecting another executable. The launchers do not
install software, delete volumes or reset Kafka offsets.

## Compatibility and imports

Old commands such as `python scripts/run_assessed_stack.py` still work. Standard
imports of the old Python module alias the canonical module, so function patches
and runtime globals refer to one implementation. New code should import grouped
modules explicitly. PowerShell aliases retain typed parameters and forward them
with named splatting. The CI shell alias delegates to `ci/ci_baseline.sh`.

Individually copied tools (the dashboard server, foundation Spark probe,
initializer and volume inventory) remain self-contained. Docker COPY paths and
the foundation source-hash comparison select the actual implementations, not
forwarders. Current Python source identity changes when an orchestration caller
path changes; rebuild and requalify images before claiming runtime acceptance.

## Public publication

Use **Publish verified demo** in GitHub Actions on main. It calls the grouped
acceptance runner, then `dashboard/scripts/prepare-publication.mjs`. Browser
checks must pass before public results are committed. See the
[hosting guide](../docs/vercel-demo-guide.md).

Root compatibility aliases are transitional supported entrypoints, not files to
remove as cosmetic cleanup. A later removal needs a deliberate compatibility
decision after all external/operator callers have migrated.
