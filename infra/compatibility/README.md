# Spark and catalog compatibility foundation

This smaller specification proves Spark/Iceberg/Nessie/MinIO integration and
catalog/storage recreation. It is not the complete commerce application. The
readiness DAG and foundation Dockerfiles remain here; common build inputs are
under `../shared`.

Use `scripts/windows/Start-CartcoInfrastructure.ps1` from the repository root.
Terraform defaults to this foundation and retains project `cartco-infra-p1-tf`.
The optional acceptance container is finite; its absence from the normal service
set does not imply a failed long-running service.

Image/platform observations in `image-platforms.json` are dated evidence, not
proof that all images execute on ARM. Keep existing volumes and project names.
For the full application see [local/](../local/README.md).
