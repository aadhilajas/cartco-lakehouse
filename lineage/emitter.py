"""Best-effort manual OpenLineage emitter for the legacy pipeline jobs.

Uses the pinned 1.11.3 client's run models. Unset OPENLINEAGE_URL disables
client emission; construction and client failures never replace data outcomes.
Client completion is not proof of backend persistence. The versioned runtime's
journal, custom facets and local alerts live in pipeline.observability.
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
PRODUCER = "https://github.com/aadhilajas/cartco-lakehouse"

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


def _emit(event: RunEvent) -> str:
    """Report transport outcome truthfully; never confuse disabled with delivered."""
    try:
        client = _get_client()
        if client is None:
            return "unavailable" if OPENLINEAGE_URL else "disabled"
        client.emit(event)
        return "client_completed"
    except Exception:
        logger.warning("OpenLineage transport failed; pipeline result is unchanged", exc_info=True)
        return "failed"


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


def _safe_emit(event_type, job_name, run_id, inputs, outputs):
    try:
        outcome = _emit(_run_event(event_type, job_name, run_id, inputs, outputs))
        logger.info("OpenLineage job=%s event=%s transport=%s", job_name, event_type.value, outcome)
    except Exception:
        # Construction errors must be as harmless as endpoint failures.
        try:
            logger.warning("OpenLineage event construction failed; pipeline result is unchanged", exc_info=True)
        except Exception:
            pass


def emit_start(job_name: str, run_id: str, inputs: Sequence[Dataset] = (), outputs: Sequence[Dataset] = ()) -> None:
    """Best-effort START; construction and transport failures never replace task results."""
    _safe_emit(RunState.START, job_name, run_id, inputs, outputs)


def emit_complete(job_name: str, run_id: str, inputs: Sequence[Dataset] = (), outputs: Sequence[Dataset] = ()) -> None:
    """Best-effort COMPLETE; disabled or failed transport is logged accurately."""
    _safe_emit(RunState.COMPLETE, job_name, run_id, inputs, outputs)


def emit_fail(job_name: str, run_id: str, inputs: Sequence[Dataset] = (), outputs: Sequence[Dataset] = ()) -> None:
    """Best-effort FAIL; callers must still re-raise the original pipeline failure."""
    _safe_emit(RunState.FAIL, job_name, run_id, inputs, outputs)
