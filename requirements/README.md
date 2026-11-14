# Dependency contract

Python 3.11 is supported. Direct application dependencies live in runtime.in;
test.in adds pytest; airflow.in adds Airflow 2.9.0. The .txt files pin transitive
versions and distribution SHA-256 hashes. Windows/Linux test locks share package
versions with the Linux Airflow lock where packages overlap.

Install using pip inside a clean Python 3.11 virtual environment:

```bash
python -m pip install --require-hashes -r requirements/test.txt
python -m pip check
```

Airflow runs on Linux in Docker, not natively on Windows. The Docker image uses
airflow.txt and the digest-pinned slim Airflow Python 3.11 image. The full image
bundles unused cloud providers whose dependency bounds conflict with this lock
(observed during the first host build). The slim image avoids those extras;
application dependencies remain unchanged. Other inherited packages are covered
by its digest and checked with pip check during build. Do not suppress pip check
or install the local-file SQLAlchemy 2 lock into the Airflow image.
The metadata database driver `psycopg2-binary==2.9.10` is now explicit in
airflow.in and its hash-locked output; it is not inherited from an unused
Postgres provider. No pre-existing locked versions were changed for this fix.
APT build dependencies still use the base distribution repositories; this is
not a bit-for-bit reproducible OS package build.

Regenerate deliberately with uv 0.9.7 (install with pip install uv==0.9.7):

```bash
uv pip compile requirements/airflow.in --python-platform x86_64-unknown-linux-gnu --python-version 3.11 --generate-hashes -o requirements/airflow.txt
uv pip compile requirements/runtime.in -c requirements/airflow.txt --universal --python-version 3.11 --generate-hashes -o requirements/runtime.txt
uv pip compile requirements/test.in -c requirements/airflow.txt -c requirements/runtime.txt --universal --python-version 3.11 --generate-hashes -o requirements/test.txt
```

After regeneration, run clean dependency installation, pip check, unit tests,
Docker build and the DAG import check. Resolution alone is not runtime proof.
setuptools is intentional: OpenLineage 1.11.3 imports pkg_resources at runtime.
The test lock must always use `--universal`: Linux requires pinned/hash-locked
`pexpect`/`ptyprocess`, while `pywinpty` must be restricted to Windows. A
Windows-only resolution can pass locally but fail Linux CI before tests start.
For a platform repair that preserves current versions, use the exact regeneration
command in `test.txt`'s header, including the connector constraints.

The baseline Airflow environment excludes Spark/Java. The assessed deployment
adds Java and a separate hash-locked Spark virtual environment; it does not mix
Spark dependencies into Airflow's Python environment.

Container manifests were read from Docker Registry on 2026-11-09 for Airflow,
Postgres, Iceberg REST and Marquez. MinIO/mc registry requests were unauthorized;
`infra/shared/Dockerfile.minio` builds fixed official source commits with digest-pinned
Go/Debian base images. See docs/phase1/minio-source-build.md for provenance.
Its container build and live behavior have since passed remote CI and current
publication acceptance; see the [verification report](../docs/test_report.md).

## Optional local-file transaction checks

file-integration.in adds SQLAlchemy 2.0 for PyIceberg's SQLite catalog. Install
file-integration.txt in a separate Python 3.11 virtual environment. Do not install
it in the Airflow environment, whose SQLAlchemy 1.4 pin must remain intact.

```bash
uv pip compile requirements/file-integration.in -c requirements/test.txt --universal --python-version 3.11 --generate-hashes -o requirements/file-integration.txt
```

This lock reuses test.txt versions wherever packages overlap. The dedicated
file-integration CI job installs it separately and runs real local Iceberg files.
