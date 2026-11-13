# Integrate the completed local project into the existing GitHub repository

This is a manual walkthrough, as requested. Do this after local acceptance and
the final clean commit/bundle checkpoint. Nothing in this guide publishes code
automatically. The repository is already `aadhilajas/cartco-lakehouse`.

## 1. Use the existing checkout

Open normal PowerShell in the project folder. Windows moved this workspace to:

```powershell
Set-Location 'C:\Users\abiaf\OneDrive\Documents\Codex\2026-11-09\i-x20\cartco-lakehouse'
git status --short
git branch --show-current
git remote -v
```

The working branch is `assignment/phase1-infrastructure`. If there are uncommitted
changes, finish reviewing/committing them before continuing. Do not clone over
this folder, replace `.git`, reset to remote main, or upload a ZIP over GitHub
files. The local branch includes valuable earlier implementation history.

## 2. Preserve a second recoverable copy

```powershell
git bundle create ..\outputs\cartco-before-github.bundle --all
git bundle verify ..\outputs\cartco-before-github.bundle
git branch backup/before-github-integration
```

If that backup branch already exists, choose a new descriptive suffix. A Git
bundle preserves tracked history, not ignored warehouse data, runtime credentials
or Docker volumes. Preserve those separately; do not commit them.

## 3. Fetch and inspect the existing repository

```powershell
git fetch origin
git log --oneline --graph --left-right origin/main...HEAD
git diff --stat origin/main...HEAD
```

The remote should be `https://github.com/aadhilajas/cartco-lakehouse.git`. If it
differs, stop and inspect it before pushing. Fetching does not discard local work.
If Git asks for sign-in, complete Git Credential Manager's browser flow privately.
Do not paste a token or password into this chat or commit one into a file.

If remote main gained changes, merge them on this feature branch:

```powershell
git merge origin/main
```

Resolve each conflict using the intended final behavior and rerun the relevant
checks. Keep new assessed code and original baseline preservation; do not simply
choose one side for all files. `git merge --abort` safely returns to the pre-merge
state if review is needed. Do not force-push or rewrite the existing main history.

## 4. Push the feature branch and open a pull request

```powershell
git push -u origin assignment/phase1-infrastructure
```

On the existing GitHub repository, choose **Compare & pull request**. Target main
from this branch. Suggested title: `Add verified protocol ingestion and Spark
commerce lakehouse`. Describe final behavior, local evidence, and any still-open
deployment checks. Do not claim remote CI is green until GitHub actually runs it.

The connected integration previously returned HTTP 403 for branch creation. A
normal signed-in Git push is a separate route. If it also denies access, check
that the signed-in account owns or can write this repository. For fine-grained
tokens, repository Contents and Workflows write access are needed to push workflow
changes. Prefer the normal credential flow; never put credentials in the remote URL.

## 5. Review CI and merge normally

Check the existing unit/file/infrastructure workflows and the new assessed
workflow. The latter builds the dashboard, compares real Spark calculations
against the independent reference, and builds/runs the scheduled assessed stack.
Read failures and retain actual artifacts. Local passing results do not replace
these remote checks.

Once relevant checks pass and the final diff is reviewed, merge the pull request
normally. Preserve real dates; these November improvements are not backdated July
submissions. Keep the existing milestone tag. Create final release tags/assets
only after the published code and evidence are verified.

## 6. Then move to hosting

Use `manual-hosting.md`. A public static dashboard alone does not satisfy the
required hosted backend. The pipeline, catalog and persistent storage need a real
host, with protected operational access and verified restart behavior.
