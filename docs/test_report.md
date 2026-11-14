# Verification report

Updated **14 November 2026**. Evidence below is scoped to the implementation and
run that produced it. Historical test counts are retained as dated measurements,
not combined into a single project score.

## Current acceptance and deployment

| Area | Verified result | Evidence |
|---|---|---|
| Current PR checks | Dashboard, Spark reference, scheduled stack, Python units, real-file transactions and baseline integration all passed | [PR #5](https://github.com/aadhilajas/cartco-lakehouse/pull/5) |
| First public publication | Full pipeline, hosted build, real browser acceptance, public files and cleanup passed | [Run 43552077988](https://github.com/aadhilajas/cartco-lakehouse/actions/runs/43552077988) |
| Repeat publication | Same acceptance path passed; a new snapshot and provenance were published | [Run 43554685157](https://github.com/aadhilajas/cartco-lakehouse/actions/runs/43554685157) |
| Live HTTPS result | Website and JSON returned successfully; run/release/source matched the repeat run | [Deployment checkpoint](validation/public-demo-checkpoint.json) |
| Quality | Seven persisted GE suites and independent exact reconciliation passed | Successful current stack/publication runs |
| Column lineage | 44 output schema fields and 34 nonconstant derivations checked against Marquez | [Compose checkpoint](validation/assessed-compose-checkpoint.json) |
| Local cold restore | Eight active volumes backed up and restored into an isolated project | [Recovery checkpoint](validation/assessed-recovery-checkpoint.json) |
| Local Terraform | Apply, recreation, replay and final no-change plan passed | [Terraform checkpoint](validation/assessed-terraform-checkpoint.json) |

Publication tests exercise refusal of incomplete acceptance, failed quality,
unverified lineage and rounded numeric snapshot IDs. Dashboard tests also cover
exact-cent arithmetic, unavailable/zero values and freshness. Five Node tests
passed in the hosting preparation and successful publication workflows.

Browser acceptance checks INR 921.75 total, the INR 301.50 Shopify filter, seven
quality suites, 44 lineage rows, mobile overflow and invalid-refresh retention.
Public deployment also received user checks in a browser and on the requested
views/filters. Server-side endpoint verification separately matched publication
metadata with the producing run; it is not a substitute for browser execution.

## Table and failure behavior

Controlled checks cover source-version replay, retained tombstones, bad candidates
leaving the previous published pointer intact, catalog/storage recreation,
time travel, additive evolution, incompatible-change refusal, partition pruning
and compaction with unchanged row hashes. The compaction fixture retained 120
rows while reducing five files to two; it is not a production throughput test.

The baseline application remains regression-tested in its own CI path. Its
REST catalog and older recovery/crash-drill results describe that application;
see the [preserved report](history/baseline-test-report.md).

## Reproduction and retention

[Test setup](../tests/README.md) separates unit, real-file, Spark and live
requirements. Full host outputs live under ignored `test-results/`. Committed
checkpoint summaries retain identities and result boundaries. GitHub artifacts
expire according to each workflow; publication evidence uses three-day retention.
Download needed viva evidence promptly. Workflow URLs remain useful even after
artifact expiration.

## Limits and pending items

No continuous production backend, distributed Spark performance SLA, independent
offsite backup or arbitrary whole-system failure recovery is claimed. The website
serves saved synthetic results; runner storage is temporary. Genuine contract
owner review and a final narrated presentation remain separate submission tasks.
See the [status ledger](local-completion-checklist.md).
