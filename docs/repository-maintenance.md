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

Completed in PR #7: separate views, shared components, snapshot hook,
model/configuration helpers, grouped scripts/unit/browser tests and styles.
Both local and hosted browser modes passed remote CI. After merge, the public
Vercel HTML and asset were checked against the refactored build; published run
metadata remained intact.

Extract `src/main.jsx` into an application shell, shared components and separate
Business/Engineering/Lineage views. Keep `dashboard/` as the Vercel root. Group
publication helpers and tests into dashboard subdirectories, updating package
commands, relative imports and workflow paths together. Preserve decimal-string,
zero/unavailable, failed-refresh and run-provenance behavior. Validate both local
and hosted builds, model/publication tests, mobile UI and real hosted publication.

## 3. Script organization and portable launchers

Prepared on `refactor/script-layout`: canonical purpose-based directories, a
reviewable compatibility map, old CLI/import/PowerShell forwarders, updated active
callers and standalone Docker COPY/hash paths. Executable discovery prefers normal
clone tools while retaining optional workspace fallbacks. PR #8 passed remote baseline/current Docker, foundation, browser and cold
recovery checks and was merged.

Group scripts into development, acceptance, recovery, deployment, diagnostics,
fixtures and Windows launchers. First enumerate callers from workflows, Docker
images, DAGs, tests and docs. Replace directory-depth assumptions before moves;
retain compatibility wrappers for supported old entrypoints. Prefer explicit
Python selection or a documented project environment to a neighbouring Codex
workspace environment. Validate CLI paths and current/baseline acceptance.

## 4. Infrastructure separation

Prepared on `refactor/infrastructure-layout`: current stack in `infra/local`,
foundation in `infra/compatibility`, extracted common build assets in `infra/shared`
and a baseline navigation guide for the intentionally preserved root Compose
entrypoints. Terraform stays in place with legacy stack-selection aliases; no
resource/state address is renamed. Five rendered Compose models match their
pre-move versions except reviewed Dockerfile paths. Remote qualification remains
required before merge.

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
