# Documentation index

Start with the [project overview](../README.md),
[live dashboard](https://cartco-lakehouse.vercel.app/) and
[current status](local-completion-checklist.md).

## Current application

| Topic | Guide |
|---|---|
| Local setup, services and Terraform | [Current runbook](assessed-runbook.md) |
| Architecture | [Narrative](architecture/assessed-narrative.md), [containers](architecture/assessed-containers.svg), [components](architecture/assessed-components.svg) |
| Sources and business semantics | [Data](data.md), [protocol connectors](protocol-connectors.md), [contract review](../contracts/OWNER_REVIEW.md) |
| Tables | [Catalog](catalog.md) |
| Lineage and monitoring | [Lineage](lineage.md) |
| Tests and successful runs | [Verification report](test_report.md), [checkpoint index](validation/README.md) |
| Public hosting and refresh | [Vercel demonstration](vercel-demo-guide.md), [deployment decision](adr/ADR-016-public-snapshot-demo.md) |
| Demo preparation | [Walkthrough](project-walkthrough.md), [presence artifact](presence_artifact.md) |
| Incidents and implementation lessons | [Postmortem](project-postmortem.md), [thinking artifact](thinking_artifact.md) |

## Code and operator navigation

- [Infrastructure](../infra/README.md): distinguish current, foundation and original stacks.
- [Scripts](../scripts/README.md): supported commands and their scope.
- [Tests](../tests/README.md): choose the correct dependency environment.
- [Contributing](../CONTRIBUTING.md): review and validation expectations.
- [Deliverables](../deliverables/README.md): historical submission records.

[Repository maintenance sequence](repository-maintenance.md) tracks the separate
follow-up refactors; documentation cleanup does not claim those moves are done.

## Decisions, history and alternatives

[ADRs](adr/) preserve decisions over time; ADR-013 through ADR-016 describe the
current Spark/protocol/publication/deployment path. Older ADRs may describe the
baseline or an earlier implementation stage. Their dates and original claims
are not rewritten as new acceptance evidence.

[Historical guides](history/README.md) preserve the earlier overview, catalog,
sources, lineage and report. The existing `phase1/` through `phase5/` guides and
`final/` plans record implementation stages. Their old pending statements are
not the current status ledger. Consult current guides first.

[Persistent-server hosting](manual-hosting.md) describes an alternative that has
not been deployed; it is distinct from the working public snapshot demonstration.
[Manual GitHub integration](manual-github-integration.md) documents an already
completed procedure, not a remaining setup requirement.
