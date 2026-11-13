# Local project completion ledger

Updated 13 November 2026. Scope: finish the project locally, then guide manual
integration into the existing GitHub repo and hosting. No automatic publication,
paid resources or personal resume/interview work.

- [x] Four real local protocols, immutable evidence and replay/failure checks.
- [x] Actual Spark Bronze/Silver/Gold through Iceberg/Nessie/MinIO and catalog metadata.
- [x] Independent exact reconciliation, seven GE suites and safe publication.
- [x] Marquez backend verification: 44 output fields, 34 nonconstant derivations.
- [x] Four real Airflow DAGs and Grafana with a restricted database role.
- [x] Read-only dashboard, browser tests, screenshots and genuine interface GIF.
- [x] Compose recreation/replay, time travel, evolution, compaction and pruning.
- [x] Coordinated cold backup and exact restore into a separate project.
- [x] Real expanded Terraform apply/recreation/replay and final no-change plan.
- [x] README, C4 diagrams, ADRs, contracts, runbook and walkthrough/postmortem.
- [x] Manual integration/hosting guides and verified Git bundle handoff.

## Retained successful evidence

| Check | Evidence |
|---|---|
| Compose | `test-results/assessed/20261113-203545/SUCCESS.txt` |
| Cold recovery, eight active volumes | `test-results/assessed-recovery/20261113-205004/SUCCESS.txt` |
| Terraform, including successful resume | `test-results/assessed/20261113-210244/SUCCESS.txt` |
| No-change plan | Same directory, `resume-terraform-convergence.log` |
| Full unit suite | `test-results/final-local/unit.xml`: 298 passed, one Windows FIFO skip |
| Additional auth regression | `tests/unit/test_assessed_auth.py`: real valid/expired-cookie HTTP exchange passed |
| Spark reference | Three real tests plus the repeating-ratio regression passed |
| Browser | Compose directory, `ui/ui-verification.json` |

Machine-readable summaries are in `docs/validation/`. Original CartCo baseline
container IDs were unchanged. Old Phase 1/Compose volumes, recovery archives and
restored copies are retained. The active assessed stack is now the isolated
Terraform project `cartco-infra-p1-tf-assessed`; prior new Compose services are
stopped. The previous Terraform workspace selection was restored.

The tiny synthetic fixture has seven completed orders and INR 921.75 completed
sales. Compaction reduced five files to two with unchanged row hashes. This is
neither merchant onboarding nor a production throughput benchmark. The GIF and
written demo plan do not claim a narrated recording.

## Manual and external steps

- Integrate the existing feature branch into GitHub and run actual remote CI.
- Record genuine owner review of `cartco.commerce.v1` in the PR or a dated record.
- Qualify available free hosting and runtime architecture. Pinned amd64-only
  Marquez images are not already qualified for ARM.
- Deploy/verify public HTTPS backend/dashboard and hosted persistence/recovery.
- Record the narrated project presentation with the final real system if required.

Follow `manual-github-integration.md`, then `manual-hosting.md`. No public URL,
remote CI success, owner signature or guaranteed free capacity is invented.
Failed runs remain in the postmortem and original evidence folders; they do not
receive success markers.
