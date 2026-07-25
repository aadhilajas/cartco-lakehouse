"""Minimal OpenLineage emission for the working Shopify Bronze -> GE -> Silver path.

Scope (deliberately limited to what is actually implemented and verified end
to end — see ``docs/lineage.md`` for the full coverage statement):
    - ``bronze.shopify_orders`` write (``ingestion/shopify_bronze.py``)
    - the pre-Silver Great Expectations checkpoint
      (``transforms/ge_bronze_shopify_checkpoint.py``)
    - ``silver.orders`` write (``transforms/silver_shopify_orders.py``)

Not covered: any other channel (Amazon, etc.), Gold, or column-level
lineage. Do not instrument connectors that do not exist yet.

Design choice — manual client emission, not the Airflow OpenLineage provider:
``shopify_bronze_dag.py`` runs its Bronze task via ``BashOperator`` (a
separate ``python ingestion/shopify_bronze.py`` process) and its GE/Silver
tasks via ``PythonOperator`` calling into plain Python modules — none of
these are declarative SQL/operator-level work that
``apache-airflow-providers-openlineage``'s automatic extraction is built to
introspect. Emitting explicitly from inside each script's own ``run()``
gives correct, verifiable dataset-level lineage regardless of whether the
script is invoked by Airflow or by hand (e.g.
``docker exec ... python ingestion/shopify_bronze.py``), and matches what
the OpenLineage docs themselves recommend for custom integrations
(https://openlineage.io/docs/client/python/).

API note: ``apache/airflow:2.9.0`` already ships
``apache-airflow-providers-openlineage``, which pulls in
``openlineage-python==1.11.3`` as a transitive dependency (see
``Dockerfile.airflow``). This module deliberately reuses that already-pinned
version rather than upgrading it, to avoid a dependency conflict with the
provider package. ``openlineage-python==1.11.3`` does not have the newer
``openlineage.client.event_v2`` module documented on openlineage.io's
current site (that module raised ``ModuleNotFoundError`` when checked live
against this exact installed version, inside the Airflow container) -- its
``RunEvent``/``Job``/``Run``/``Dataset`` classes live in
``openlineage.client.run`` instead, which is what this module imports.

Failure handling: a Marquez outage must never break the pipeline it is
observing. Every public function here catches and logs (never raises) any
exception raised while building or emitting an event.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Sequence

from openlineage.client import OpenLineageClient
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState

logger = logging.getLogger("lineage")

# --- Configuration (env-overridable; unset OPENLINEAGE_URL disables lineage) ---
OPENLINEAGE_URL = os.environ.get("OPENLINEAGE_URL", "")
JOB_NAMESPACE = os.environ.get("OPENLINEAGE_NAMESPACE", "cartco-lakehouse")
# Iceberg tables live in one REST catalog/warehouse; a fixed scheme-style
# namespace is enough to distinguish them from any future dataset source
# without inventing per-table connection strings.
DATASET_NAMESPACE = "iceberg://cartco-lakehouse"
PRODUCER = "https://github.com/cartco-lakehouse/shopify-pipeline"

_client: Optional[OpenLineageClient] = None
_client_initialized = False


def _get_client() -> Optional[OpenLineageClient]:
    """Lazily build the OpenLineage HTTP client, or ``None`` if lineage is disabled.

    Lineage is disabled (not an error) whenever ``OPENLINEAGE_URL`` isn't
    set -- e.g. running a script by hand outside the compose network without
    Marquez up. Client construction failures are also treated as
    "disabled", logged once, and never raised.
    """
    global _client, _client_initialized
    if _client_initialized:
        return _client

    _client_initialized = True
    if not OPENLINEAGE_URL:
        logger.info("OPENLINEAGE_URL not set; lineage emission disabled for this run.")
        return None

    try:
        _client = OpenLineageClient(url=OPENLINEAGE_URL)
    except Exception:
        logger.warning(
            "Failed to construct OpenLineage client for %s; lineage emission "
            "disabled for this run.",
            OPENLINEAGE_URL,
            exc_info=True,
        )
        _client = None
    return _client


def new_run_id() -> str:
    """Generate a fresh run id, shared by one job's START/COMPLETE/FAIL events."""
    return str(uuid.uuid4())


def table_dataset(table_identifier: str) -> Dataset:
    """Build an OpenLineage Dataset for one of our Iceberg tables.

    ``table_identifier`` is the same dotted name used everywhere else in the
    repo, e.g. ``"bronze.shopify_orders"`` or ``"silver.shopify_orders"``.
    """
    return Dataset(namespace=DATASET_NAMESPACE, name=table_identifier)


def _emit(event: RunEvent) -> None:
    client = _get_client()
    if client is None:
        return
    try:
        client.emit(event)
    except Exception:
        logger.warning(
            "Failed to emit OpenLineage %s event for job=%s; continuing "
            "without lineage for this event.",
            event.eventType,
            event.job.name,
            exc_info=True,
        )


def _run_event(
    event_type: RunState,
    job_name: str,
    run_id: str,
    inputs: Sequence[Dataset],
    outputs: Sequence[Dataset],
) -> RunEvent:
    return RunEvent(
        eventType=event_type,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id),
        job=Job(namespace=JOB_NAMESPACE, name=job_name),
        producer=PRODUCER,
        inputs=list(inputs),
        outputs=list(outputs),
    )


def emit_start(
    job_name: str,
    run_id: str,
    inputs: Sequence[Dataset] = (),
    outputs: Sequence[Dataset] = (),
) -> None:
    """Emit a START RunEvent for ``job_name``/``run_id``. Never raises."""
    _emit(_run_event(RunState.START, job_name, run_id, inputs, outputs))
    logger.info("Emitted OpenLineage START for job=%s run_id=%s", job_name, run_id)


def emit_complete(
    job_name: str,
    run_id: str,
    inputs: Sequence[Dataset] = (),
    outputs: Sequence[Dataset] = (),
) -> None:
    """Emit a COMPLETE RunEvent for ``job_name``/``run_id``. Never raises."""
    _emit(_run_event(RunState.COMPLETE, job_name, run_id, inputs, outputs))
    logger.info("Emitted OpenLineage COMPLETE for job=%s run_id=%s", job_name, run_id)


def emit_fail(
    job_name: str,
    run_id: str,
    inputs: Sequence[Dataset] = (),
    outputs: Sequence[Dataset] = (),
) -> None:
    """Emit a FAIL RunEvent for ``job_name``/``run_id``.

    Never raises itself -- call this from an ``except`` block and then
    re-raise the original exception so the Airflow task still fails.
    """
    _emit(_run_event(RunState.FAIL, job_name, run_id, inputs, outputs))
    logger.info("Emitted OpenLineage FAIL for job=%s run_id=%s", job_name, run_id)
