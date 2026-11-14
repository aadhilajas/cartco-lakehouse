# Lineage and operational evidence

## Current Spark application

The current implementation builds column mappings in
[pipeline/assessed_lineage.py](../pipeline/assessed_lineage.py), emits OpenLineage
information to Marquez, and checks the backend schema/graph rather than treating
a successful HTTP emission as complete verification.

Current acceptance distinguishes **44 output schema fields** from **34
nonconstant derivations**. Constants do not receive invented input edges. These
are field-dependency checks, not exhaustive per-row or historical-version lineage.

The dashboard's Lineage view displays the declared mappings from the same
published release. Its verified transport status refers to the backend check
performed during that run; the static website is not a continuously queried
Marquez service. The public Marquez admin/API endpoints are not exposed.

## Airflow and Grafana

The current four DAGs cover order ingestion, inventory ingestion, commerce
release and system health. Grafana reads actual Airflow task/run metadata through
a restricted PostgreSQL role and health view. The role is checked for read scope
and refusal of configuration access/writes. Its five panels do not fabricate
success counts, durations or missing history.

Local operator URLs and fixture credentials are documented in the
[runbook](assessed-runbook.md). In GitHub Actions the stack is temporary, and the
public website retains only the approved result snapshot. Local cold recovery
is separate from remote publication.

## Earlier instrumentation

The earlier versioned application has its own intended-event/delivery journals,
retry alerts and a larger source/state field-check total. Those measurements
belong to that implementation; they must not be combined with the current 44/34
counts. Its legacy DAGs are not all column-instrumented.

See the [preserved baseline lineage guide](history/baseline-lineage.md) for those
commands, callback checks and local alert boundaries. External email/Slack/paging
has not been configured as part of this project.

[Verification report](test_report.md) · [Architecture](architecture/assessed-narrative.md)
