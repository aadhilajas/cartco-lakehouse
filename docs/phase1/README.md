> Implementation-stage reference. Pending statements below belong to the
> original stage. See the [documentation index](../README.md) and
> [current verification report](../test_report.md) for later acceptance, remote CI
> and the working public snapshot deployment.

# Phase 1: reproducible baseline

Use Python 3.11 for local tests. Airflow itself runs in Linux containers.

```bash
python -m venv .venv
# Activate: .venv/Scripts/Activate.ps1 (PowerShell) or source .venv/bin/activate (bash)
python -m pip install --require-hashes -r requirements/test.txt
python scripts/dev.py collect
python scripts/dev.py test
```

Alternatively, after activation, python scripts/dev.py bootstrap installs the
same fully pinned dependency file and runs pip check. Python patch releases
may differ; the supported minor version is checked by the command helper.

Copy .env.example to .env if Compose interpolation settings need adjustment.
The .env file is ignored by Git; it is not automatically loaded by standalone
Python scripts. Export host endpoint variables explicitly when running outside
Docker. Default credentials are for this local synthetic-data stack only.

With Docker Desktop / Compose v2 available:

```bash
python scripts/dev.py build
python scripts/dev.py check-dags
python scripts/dev.py up
# Wait for docker compose ps to show healthy Airflow and ready catalog/storage.
python scripts/dev.py run-local
python scripts/dev.py down
```

run-local executes existing scripts sequentially (both source pipelines, then
four marts) and stops on the first failure. It is a baseline demonstration,
not the future scheduled workflow or an acceptance/reconciliation test. Current
scripts now accept a fixed batch date; known Gold semantics remain unfixed.
See [Phase 2 batch execution](../phase2/batch-execution.md) for current behavior.
The command does not erase volumes. generate only creates the two source CSVs.

The CI stack-baseline job builds both images, imports all real DAGs, runs both
source DAGs followed by Gold with Airflow's dags test command, then executes the
four live integration tests with strict missing-prerequisite failures. It saves
task logs, JUnit results and table snapshot/count evidence. This is the existing
two-source baseline, not acceptance of future metric or replay contracts.

MinIO is built from pinned upstream source because anonymous upstream container
pulls are unavailable. The image build may take longer on an empty cache; see
minio-source-build.md for the exact source and base-image provenance.

Read requirements-matrix.md, baseline.md, ../../requirements/README.md and
../adr/ADR-007-data-contracts-and-replay.md before interpreting test results.
The old README/test report contains historical counts and stale scope claims;
this folder records the current Phase 1 evidence.

The new [versioned runtime guide](../phase5/versioned-airflow-runtime.md) supersedes the old three-DAG inventory for import acceptance: scripts/check_dags.py now expects four DAGs, including versioned_commerce_release. Live CI additionally exercises all three versioned sources and optional inventory before the legacy baseline. These steps are prepared locally, not executed.
