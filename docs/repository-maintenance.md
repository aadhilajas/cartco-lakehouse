# Repository maintenance sequence

This plan separates documentation cleanup from runtime path changes. Each phase
should have a focused pull request and validation appropriate to its impact.
Nothing here authorizes deletion or migration of retained data.

## 1. Documentation and navigation

Prepared locally in the 14 November 2026 pass: current README, directory indexes,
source/catalog/lineage guides, scoped verification/status, dated public deployment
checkpoint, hosting ADR and preserved historical copies. Existing runtime paths,
DAG IDs, Terraform addresses, project names and volume names stay unchanged.
Review and merge this pass before starting unrelated runtime moves.

## 2. Dashboard maintainability

Prepared locally after PR #6: separate views, shared components, snapshot hook,
model/configuration helpers, grouped scripts/unit/browser tests and styles.
Package commands and workflow callers now use the grouped paths. Browser checks
for both serving modes are required in the dashboard PR job; successful remote
acceptance and merge are still pending for this refactor.

Extract `src/main.jsx` into an application shell, shared components and separate
Business/Engineering/Lineage views. Keep `dashboard/` as the Vercel root. Group
publication helpers and tests into dashboard subdirectories, updating package
commands, relative imports and workflow paths together. Preserve decimal-string,
zero/unavailable, failed-refresh and run-provenance behavior. Validate both local
and hosted builds, model/publication tests, mobile UI and real hosted publication.

## 3. Script organization and portable launchers

Group scripts into development, acceptance, recovery, deployment, diagnostics,
fixtures and Windows launchers. First enumerate callers from workflows, Docker
images, DAGs, tests and docs. Replace directory-depth assumptions before moves;
retain compatibility wrappers for supported old entrypoints. Prefer explicit
Python selection or a documented project environment to a neighbouring Codex
workspace environment. Validate CLI paths and current/baseline acceptance.

## 4. Infrastructure separation

A possible target is `infra/local/`, `infra/baseline/`, `infra/compatibility/`,
`infra/shared/`, `infra/terraform/` and `infra/hosting/`. Extract shared Spark locks,
JAR fetching and Marquez build dependencies before relocating Phase 1 files.
Update stack specifications, Docker COPY paths, Terraform allowed spec paths and
source triggers, recovery/acceptance tools, tests and docs in the same change.
Keep existing resource identities. Re-run the applicable Compose/recovery and
Terraform apply/recreation/convergence checks before declaring a move complete.

## 5. Optional deeper source/package cleanup

Identify current versus shared-reference versus baseline module usage before
renaming `assessed_*` files or adopting a `src/cartco/` package. Account for the
source-identity path list and image build inputs. Defer package migration unless
it improves use enough to justify requalification. Preserve older ADR dates and
original submission evidence; rename screenshot files only with updated links.

[Documentation index](README.md) · [Contribution process](../CONTRIBUTING.md)
