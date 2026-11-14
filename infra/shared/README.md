# Shared build inputs

- `Dockerfile.airflow-base`: hash-locked Airflow/Python parent used by baseline,
  foundation and current application images with their existing image tags.
- `Dockerfile.minio`: builds MinIO/mc from the recorded official source commits.
- `spark/`: Python build/Spark locks, JAR checksum manifest and fetch helper.
- `marquez/`: pinned Marquez configuration image and explicit configuration YAML.

Build context remains the repository root. Example:

```bash
docker build -f infra/shared/Dockerfile.airflow-base -t cartco/airflow:infra-p1-base .
```

These files were extracted from root Dockerfiles and the old Phase 1 directory;
the dependency versions, image digests and JAR checksums are preserved. Current
and foundation Dockerfiles consume the same Spark and Marquez inputs. Do not
regenerate locks or replace pinned images as part of a folder cleanup.

The old Python import `infra.phase1.fetch_jars` forwards to
`infra.shared.spark.fetch_jars` for compatibility. Use the shared path in new code;
no stack specification remains under `infra/phase1`.

Terraform build triggers cover these inputs so a shared change is not hidden by
an unrelated unchanged Dockerfile. See the [infrastructure map](../README.md).
