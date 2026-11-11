# Lineage and runtime observability

The pinned client is openlineage-python 1.11.3. Compose targets the local Marquez
services; live backend acceptance has not been re-established on this host.

The [versioned runtime observability guide](phase5/runtime-observability.md)
defines the current three-source journal, custom run/schema/snapshot facets,
publication evidence, fail-safe client handling and local failure-alert outbox.
Tests exercise the actual client/FileTransport and validate local custom schemas.
They do not establish Marquez HTTP/graph persistence or Airflow scheduler dispatch.

Legacy Shopify/Amazon ingestion and Gold scripts retain their existing manual
START/COMPLETE/FAIL emitter calls. The emitter now catches construction as well
as client errors and logs actual disabled/unavailable/client outcome states.
These older jobs do not automatically get the versioned runtime's journal or
alert outbox. Automatic provider lineage remains disabled in Compose to avoid
duplicate dataset-less task events.

Use OPENLINEAGE_URL and OPENLINEAGE_NAMESPACE only for the intended lineage
service. Client completion means emit() returned, not proof of backend indexing.
No email, Slack or webhook alert consumer is implemented or triggered.
