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
airflow.txt and a pinned Python 3.11 base image. Other packages inherited from
the base image are covered by its digest and checked with pip check during build.
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
Unused PySpark and Java are no longer installed; no implemented job uses them.

Container manifests were read from Docker Registry on 2026-11-09 for Airflow,
Postgres, Iceberg REST and Marquez. MinIO/mc registry requests were unauthorized;
Dockerfile.minio now builds fixed official source commits with digest-pinned
Go/Debian base images. See docs/phase1/minio-source-build.md for provenance.
Its container build and live behavior still require a successful CI run.
