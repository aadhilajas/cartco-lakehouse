# Project completion ledger

Updated **14 November 2026**. Current implementation is the bounded
protocol/Spark/Nessie application. Earlier applications and dated evidence remain
preserved; see the [documentation index](README.md).

## Completed implementation and verification

- [x] Four real synthetic protocol paths, durable raw evidence and replay/failure checks.
- [x] Spark Bronze/Silver/Gold through Iceberg/Nessie/MinIO with exact decimal state.
- [x] Independent reconciliation, seven GE suites and coherent publication.
- [x] Backend Marquez verification: 44 output fields and 34 nonconstant derivations.
- [x] Four meaningful Airflow DAGs and Grafana with a restricted database role.
- [x] Dashboard tests/browser checks, actual screenshots and a genuine interface GIF.
- [x] Compose recreation/replay, time travel, evolution, compaction and pruning.
- [x] Cold backup and exact restore of eight active volumes into a separate project.
- [x] Expanded local Terraform apply/recreation/replay and final no-change plan.
- [x] GitHub integration: PR #4 and hosting PR #5 merged; remote checks passed.
- [x] Public Vercel dashboard and real on-demand GitHub Actions processing.
- [x] Two successful publication runs; website metadata matched the second run.

## Evidence

| Check | Location |
|---|---|
| Local Compose | `test-results/assessed/20261113-203545/SUCCESS.txt` |
| Local cold restore | `test-results/assessed-recovery/20261113-205004/SUCCESS.txt` |
| Local Terraform, successful resume | `test-results/assessed/20261113-210244/SUCCESS.txt` |
| Final no-change plan | Same Terraform directory, `resume-terraform-convergence.log` |
| Remote checks and publication | [Verification report](test_report.md) |
| Public deployment | [Checkpoint](validation/public-demo-checkpoint.json) |

Local log paths are retained on the development machine and are not promised to
exist in a fresh clone. Small committed summaries are indexed in
[validation/README.md](validation/README.md). The accepted fixture contains seven
completed orders and INR 921.75. Existing local volumes were retained.

## Remaining review and submission tasks

- [ ] Genuine owner review of `cartco.commerce.v1`; approval must be recorded by
  the appropriate reviewer, not inferred from tests or generated documentation.
- [ ] Final narrated project walkthrough if required by the submission rubric.
  The existing interface GIF and written plan are not a narrated recording.
- [ ] Confirm whether the rubric accepts the chosen public snapshot/on-demand
  processing model or separately requires a persistent hosted backend.

## Deployment boundary

The public dashboard stays available between runs. Processing services run on a
temporary GitHub runner and stop afterward; local named-volume persistence and
cold restore do not establish durable cloud backend storage. A continuously
running cloud stack, ARM qualification and production merchant integration are
not completed capabilities. See [ADR-016](adr/ADR-016-public-snapshot-demo.md).

## Repository cleanup

This documentation pass adds current navigation and preserves earlier guides
under `history/`. Dashboard refactoring merged in PR #7 and its public build was verified. Script
grouping merged in PR #8 after runtime and recovery checks. Infrastructure path
separation is prepared as the next independently qualified change; they do not block the working demo.
