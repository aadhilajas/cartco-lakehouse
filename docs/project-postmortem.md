# What the local integration taught us

This records real observed incidents, not an invented production outage.

## A process log was not a JSON artifact

The first full compatibility run successfully wrote Iceberg data, recreated
storage/catalog containers and matched the checkpoint. Collection then failed
because Compose progress text from stderr was merged into stdout before JSON
parsing. The error began with `Invalid JSON primitive: Container`.

The correction separated artifact stdout from process stderr, validated JSON and
wrote the original text without a PowerShell serializer roundtrip. Snapshot IDs
are 64-bit integers; an unnecessary serialization step could alter their digits.
A collection-only resume recovered the completed evidence without repeating the
data writes or storage recreation. Operational success and evidence collection
were separate stages and needed separate failure handling.

## Environment variables did not override hard-coded defaults

Marquez could not authenticate to its new PostgreSQL store. Its upstream
development configuration used fixed credentials rather than the supplied
password variable. We preserved the initialized store and added an explicit
environment-backed configuration in a small derived image. Ninety-one migrations
then completed and the API became healthy. Resetting the database would have
hidden the configuration defect and discarded evidence.

## A valid Terraform configuration still needed a real apply

The first Windows apply reached the Docker resources but failed at Buildx's
context selection. The provider constructed a host-based Docker client while a
named desktop context builder expected a different CLI context. Selecting the
default builder with the explicit Linux engine endpoint fixed that boundary.
The next run exposed relative Dockerfile resolution from Terraform's working
folder. Absolute repository-root Dockerfile paths fixed it. The isolated existing
state and volumes were reused. Mock plans had checked wiring; they could not have
proved image-building behavior.

## UTC in Spark did not make collected Python datetimes UTC

Native Windows Spark/reference comparison exposed a 5½-hour shift in collected
timestamp values. Spark's SQL session was UTC, but Python's local conversion of a
timezone timestamp used the host timezone. Canonical UTC clock values were moved
to timestamp-without-timezone columns. The independent reference comparison then
passed. A field called `source_updated_at` is not a reliable version key unless
its physical representation and timezone rules agree across boundaries.

## Ratio precision needed its own edge case

Simple stock/cost fixtures produced terminating turnover ratios. Reviewing
Spark's bounded decimal division showed that intermediate scale reduction could
double-round a repeating 12-place ratio. The final small per-pool division now
uses one exact decimal quantization in a Spark UDF, after Spark performs the
distributed joins and aggregates. A genuine one-third case was added rather
than relying on the visually attractive 0.1 fixture. Its targeted real Spark
check passed after local socket access was restored.

## A changed workstation was not a changed business result

Windows relocated Documents into OneDrive. The old workspace path stopped
resolving and Python virtual environments still pointed at their previous home.
The repository and evidence were found intact and the local environments were
repaired at the new path. A later execution-runtime reset temporarily prevented
shell launch and then dropped folder/socket access. These were retained as
environment incidents, not passed project tests or reasons to overwrite data.

Native Java also hit an out-of-memory failure while heavy local processes were
overlapping. Subsequent targeted checks used a bounded driver and sequential
heavy work. The tiny earlier memory sample is not a full-stack sizing guarantee.

## What remains a boundary

The original baseline remains preserved. A prepared container specification,
diagram or workflow is not runtime acceptance. Expanded scheduled DAG/Grafana,
coordinated recovery, hosted architecture, account capacity and remote CI must
each have their own actual evidence. No free-host promise, fake public URL,
merchant onboarding or retrospective on-time submission claim follows from a
successful local compatibility probe.
