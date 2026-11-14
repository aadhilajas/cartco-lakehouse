# Testing guide

Use separate environments where dependency versions differ. A skipped Spark or
live test is not evidence that the corresponding runtime passed.

| Suite | Environment | Command from repository root |
|---|---|---|
| Python unit | Python 3.11 + `requirements/test.txt` | `python -m pytest tests/unit` |
| Real-file Iceberg | Separate Python 3.11 + `requirements/file-integration.txt` | `python -m pytest tests/file_integration` |
| Spark/reference | Java 17, test lock and separate Spark dependency installation | `python -m pytest tests/spark` |
| Live baseline | Earlier Docker application, initialized source data and opt-in flag | CI invokes `scripts/ci_baseline.sh` |
| Current scheduled application | Linux Docker/Compose and host Python 3.11 | `python scripts/run_assessed_stack.py` |
| Dashboard/publication | Node compatible with `dashboard/package.json` | `npm --prefix dashboard test` |

## Dependency setup

For unit tests, create a clean virtual environment, install the hash-locked
`requirements/test.txt`, and run `python -m pip check`. Real-file tests use
SQLAlchemy 2 and must not upgrade Airflow's SQLAlchemy 1 environment.

For Spark, follow the exact installation order and Java setup in
[assessed.yml](../.github/workflows/assessed.yml). The Spark dependency lock lives
in `infra/phase1/spark-requirements.txt`; it is not part of the normal unit lock.
Tests intentionally use an independent reference computation for comparison.

`tests/integration/` covers the **baseline**; current full application acceptance
is the host-side runner under `scripts/`. The live baseline opts in with
`RUN_INTEGRATION_TESTS=1`; the CI shell script prepares the actual inputs.

## Browser and publication checks

`dashboard/check-ui.mjs` checks business totals, filters, seven quality suites,
44 lineage fields, mobile layout, failed refresh retention and read-only routes.
The publication workflow tests hosted mode against its actual newly computed
snapshot before committing it. Publication unit tests also reject incomplete
acceptance, failed suites and rounded numeric snapshot identifiers.

Use the [verification report](../docs/test_report.md) for dated results and the
[checkpoint index](../docs/validation/README.md) for evidence boundaries. Full
logs and JUnit files belong in ignored `test-results/` or workflow artifacts;
small reviewed summaries may be committed under `docs/validation/`.
