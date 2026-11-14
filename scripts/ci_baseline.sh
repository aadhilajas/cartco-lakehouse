#!/usr/bin/env bash
# Compatibility entrypoint. Run from the repository root.
exec bash "$(dirname "${BASH_SOURCE[0]}")/ci/ci_baseline.sh" "$@"
