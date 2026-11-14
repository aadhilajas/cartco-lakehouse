> Implementation-stage reference. Pending statements below belong to the
> original stage. See the [documentation index](../README.md) and
> [current verification report](../test_report.md) for later acceptance, remote CI
> and the working public snapshot deployment.

# MinIO source-build provenance

Anonymous pulls of minio/minio and minio/mc fail on Docker Hub and Quay as of
2026-11-09. Registry tokens granted no pull access. We use no unofficial mirror.

Dockerfile.minio builds official archived source at exact commits:

- Server: https://github.com/minio/minio/tree/7aac2a2c5b7c882e68c1ce017d8256be2feea27f
- Client: https://github.com/minio/mc/tree/77f82e18b5401a65958f1619df6ebb994634bd88

Go 1.24.9 satisfies both source toolchain declarations. The Go builder and Debian
runtime image indexes were fetched from Docker Registry and their SHA-256 content
digests checked locally. Go verifies go.sum; -mod=readonly prevents module file
changes. Source revisions, base digests and licenses are retained in the image.
APT packages and resulting builds are not claimed to be bit-for-bit reproducible.
This preserves the existing local project dependency. A future production storage
choice requires a separate assessment of maintained software.

Compose builds one image containing server/client, uses curl health checks,
and runs its mc binary for bucket initialization. Successful build and runtime
behavior must be demonstrated by the CI baseline before closing this issue.
