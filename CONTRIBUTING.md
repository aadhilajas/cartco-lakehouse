# Contributing to CartCo

## Choose the right application

Read the [repository overview](README.md), [infrastructure map](infra/README.md)
and [script guide](scripts/README.md). The current Spark application uses
`infra/assessed/stack.json`; root Compose and `scripts/development/dev.py` refer to the earlier
application. Shared reference modules remain in active use.

Create a focused branch from up-to-date main. Keep documentation-only changes
separate from path migrations, application behavior and infrastructure changes.
Review `git diff` and the PR checks before merging. Do not commit dependency
caches, environment files, Terraform state, raw payloads or local runtime logs.

## Validate according to the change

- Documentation: check relative links, commands, schema names and evidence dates.
- Python behavior: run relevant unit/reference tests and the required CI checks.
- Dashboard: run its tests and build; verify browser behavior for UI changes.
- Paths or deployment: update Docker COPY paths, workflow filters, wrappers,
  Terraform build triggers and source-identity inputs together. Requalify the
  affected images/stack before claiming the migration is complete.

[Test environments](tests/README.md) have deliberately separate dependency locks.
Do not install the real-file SQLAlchemy 2 lock into Airflow. Keep exact decimal
strings and snapshot identifiers intact across Python, JSON and JavaScript.

## Evidence and data preservation

Synthetic fixtures must stay labelled. A successful test is not owner sign-off,
merchant onboarding or a production SLA. Record actual workflow/deployment URLs
and distinguish local retained volumes from temporary runner storage.

Do not rename Docker volumes/projects, Terraform resources/workspaces or Airflow
DAG IDs as cosmetic cleanup. Never delete existing data to repair a test. Retain
historical checkpoint files as dated records and add a new summary for new proof.

Public snapshots under `dashboard/public/published/` are intentional versioned
outputs. Publish them with the reviewed workflow after acceptance; do not type
business totals by hand. The website refresh button does not start processing.

Operator script implementations belong in the documented purpose-based groups.
Keep the `scripts/layout.json` compatibility map and supported forwarding files
synchronized. Update standalone Docker copies and source-hash checks together
with script changes; a forwarder is not the file to fingerprint as probe source.
