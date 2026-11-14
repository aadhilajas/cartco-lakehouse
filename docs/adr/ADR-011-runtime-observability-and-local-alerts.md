# ADR-011: Best-effort runtime lineage and local-only alerts

Status: Implemented and locally verified; live backend/scheduler acceptance pending
Date: 2026-11-11

Record each runtime invocation with paired OpenLineage events and a new UUID,
while retaining the business batch/run identity separately. Attach only verified
receipt, schema, snapshot and publication facts. Retried cached work is observable
without inventing new data commits. FAIL describes task outcome; a publication
that committed before an acknowledgement failure is reported explicitly.

Persist serialized intended events before client emission and checksum-link their
delivery status. Disabled, unavailable, failed and client-completed outcomes are
distinct. Client completion is not evidence of HTTP delivery or Marquez graph
persistence. Custom facet schemas use a stable local URN and are versioned in the
repository, without claiming an unpublished public schema endpoint.

All instrumentation is best-effort and cannot alter the pipeline's returned value,
replace its original exception or trigger another data commit. Journal, client
and alert failures remain observable gaps where possible. The legacy emitter also
catches construction errors and no longer logs unconditional emission success.

Write local stage-failure warnings plus deduplicated retry/terminal Airflow alerts.
Store error classes and context, not raw source values or exception text. Do not
send email, Slack, webhooks or other external notifications. External delivery,
incident acknowledgement/resolution, retention and monitoring availability need
separate configuration and acceptance.

The read-only inspector validates custom schemas, checksums and invocation pairing
and distinguishes clean historical evidence from recovered failures. Pinned-client
FileTransport tests and retained 120,000-record replay establish local behavior;
actual Airflow callback dispatch and Marquez HTTP/graph verification remain pending.
See [runtime observability](../phase5/runtime-observability.md).
