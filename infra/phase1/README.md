# Legacy import compatibility

Only the historical `infra.phase1.fetch_jars` Python import/CLI is preserved here.
The implementation is under [shared/spark](../shared/spark/), and the foundation
stack is [compatibility/stack.json](../compatibility/stack.json).

This is not an active stack directory. Terraform still accepts its older stack
selection string through an explicit internal alias; direct Compose callers use
the canonical specification path. See [infra/README.md](../README.md).
