# ADR-016: Public snapshots with on-demand remote processing

Date: 14 November 2026

Status: Implemented and verified for the public demonstration.

## Context

The project needs a public demonstration under a zero-spend, no-card constraint.
The complete current stack is validated locally, but a static frontend host does
not run Kafka, Airflow, MinIO, Nessie or the Spark runtime continuously.

## Decision

Use Vercel Hobby for the personal, non-commercial website and standard
GitHub-hosted Actions runners in the public repository for requested batch runs.
The owner manually starts `Publish verified demo` on main. It runs the real
scheduled stack and acceptance checks, builds and browser-tests hosted mode,
then commits only the approved snapshot and run metadata. Vercel's Git
integration publishes those assets. No public compute-trigger API or frontend
credential is required.

## Evidence

[First run](https://github.com/aadhilajas/cartco-lakehouse/actions/runs/43552077988)
and [update run](https://github.com/aadhilajas/cartco-lakehouse/actions/runs/43554685157)
both passed. The live HTTPS publication metadata matched the second run's ID,
release and source identity. The user confirmed the dashboard views and filters
worked. See [deployment checkpoint](../validation/public-demo-checkpoint.json).

## Consequences

The website remains accessible between runs and labels its data as saved
synthetic results. Refresh does not trigger processing. GitHub runner storage
is temporary, so this route does not provide durable online lakehouse storage
or continuous public operator services. Local volume restore remains a separate
proof. A rubric requiring persistent hosted services needs a separate decision.

Provider allowances and terms still apply. Avoid paid runners/plans and keep
artifact retention bounded. No custom domain or VM is needed. On failure, do not
publish a replacement snapshot; retain the previous valid site/result. Source
identities and decimal/snapshot strings remain exact.

A protected in-site trigger, persistent cloud host and production credentials
would be separate changes with their own validation. None is implied by this ADR.
