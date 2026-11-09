# Phase 1 baseline evidence

Branch: phase-1/reproducible-baseline
Starting commit: a4b34063e4b6a9cec97fcfe1dd93dd8b816a9db8
Date: 2026-11-09

## Initial baseline

A clean, workspace-local CPython 3.11.14 environment installed the original test
requirements plus the missing Faker dependency and an explicit pandas 2.1.4 pin.
The original suite failed during collection with seven module import errors:
OpenLineage 1.11.3 imports pkg_resources, but setuptools was not declared.
Adding setuptools 75.8.0 allowed all 47 unit cases to pass (110 dependency warnings).
No ingestion or business-transform code was changed to obtain that result.

## Changes implemented

- Central runtime/test/Linux-Airflow dependency inputs and hash-pinned locks.
- Shared versions for overlapping Airflow and local-test dependencies.
- Python 3.11 runtime selection and explicit Airflow base-image digest.
- Explicit Faker, NumPy, pandas and setuptools dependencies.
- One Docker dependency install with pip check; removed unused Spark/Java.
- Registry-verified digest pins for Postgres, Iceberg REST and Marquez.
- Local .env example and Git exclusions for secrets/configuration/environments.
- Portable bootstrap/test/collect/build/up/down/generate/run-local commands.
- Real DagBag inventory and dependency-graph validation in a separate CI job.
- JUnit artifacts from the unit CI job.
- Requirements/evidence matrix and ADR-007 target contracts.

## Verification boundaries

Python syntax and Compose/workflow YAML parsing passed. All 147 shared package
versions agree between the test and Linux Airflow locks. Dependency resolution
and registry metadata verification do not prove a successful container build.

The host has no Docker executable/engine. Docker build, real DAG imports and
live integration tests have NOT been executed here. The new CI configuration
has not been pushed or triggered. MinIO/mc registry queries returned HTTP 401,
so those existing latest references were retained rather than inventing pins.
Their replacement pinning and a clean-stack run remain Phase 1 exit items.
APT packages are not snapshot-pinned. The environment is not claimed to be
bit-for-bit reproducible at the OS layer.

The existing integration workflow still seeds only Shopify although its suite
requires both channels; the shared-status assertion and stale metadata checks
also remain known follow-on failures. Existing Gold metric and Silver overwrite
issues remain unchanged. Passing units does not establish financial correctness,
replay safety, durability or full project completion.

A second clean environment was installed from requirements/test.txt with hash
checking enabled; its final execution result is recorded below.

## Final clean-lock result

Hash-checked installation into a second clean Python 3.11.14 environment succeeded.
pip check: No broken requirements found. Discovery: 47 unit cases collected.
Execution: 47 passed, 110 dependency warnings, 12.33 seconds.
All 35 repository Python files parsed successfully; git diff --check passed.
The 110 upstream warnings were retained, not suppressed.

## Continuation: storage source build and live-run preparation

Repeated Docker Hub and Quay requests confirmed no anonymous pull access for
MinIO/server or client. Replaced those references with one local image built
from fixed official source commits. Go 1.24.9 and Debian base manifest content
digests were verified from registry responses; provenance is recorded separately.
This supersedes the earlier pending image-reference note, but is not build proof.

CI now runs both source DAGs and Gold on a disposable fresh stack, then the four
integration checks. The status assertion is source-scoped, isolation verification
reloads metadata and sorts keyed rows, and CI fails on missing prerequisites.
Local recheck: 47 passed, four live tests skipped, 103 warnings in 12.30 seconds.
Python syntax and Compose/workflow YAML checks passed. Native Git Bash could
not start in the restricted host, so bash syntax execution is still unverified.

Publishing is blocked by external access: the GitHub account can read repository
metadata, but create-tree returned HTTP 403 Resource not accessible by integration.
The local Git credential helper also cannot start under the host restrictions.
No branch/PR was published, no CI was started, and no remote main branch changed.
The changes are committed locally and can be exported as a Git bundle.
