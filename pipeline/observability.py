"""Best-effort runtime lineage, immutable local event journal and local-only alerts."""
from datetime import datetime, timezone
import csv
import hashlib
import json
import logging
from pathlib import Path
from uuid import uuid4

import attr
from openlineage.client.facet import BaseFacet, SchemaDatasetFacet, SchemaField, OutputStatisticsOutputDatasetFacet, DataQualityMetricsInputDatasetFacet
from openlineage.client.run import Dataset, InputDataset, OutputDataset, Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde

from lineage import emitter
from pipeline.order_state import STATE_SCHEMA
from pipeline.raw_batches import _write_once
from pipeline import column_lineage

logger = logging.getLogger(__name__)
SCHEMA_URI = "urn:cartco:observability:v1"


@attr.s
class RuntimeRunFacet(BaseFacet):
    stage: str = attr.ib()
    batchDate: str = attr.ib()
    orchestrationRunId: str = attr.ib()
    invocationId: str = attr.ib()
    evidence: dict = attr.ib(factory=dict)

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self._producer = emitter.PRODUCER

    @staticmethod
    def _get_schema():
        return SCHEMA_URI + "#/$defs/RuntimeRunFacet"


@attr.s
class SnapshotDatasetFacet(BaseFacet):
    snapshotId: int | None = attr.ib(default=None)
    tableUuid: str | None = attr.ib(default=None)

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self._producer = emitter.PRODUCER

    @staticmethod
    def _get_schema():
        return SCHEMA_URI + "#/$defs/SnapshotDatasetFacet"


def _bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _hash(value):
    return hashlib.sha256(value).hexdigest()


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _file(root, name):
    return Dataset(namespace=Path(root).resolve().as_uri(), name=name)


def _table(name, schema=None, snapshot=None, uuid=None):
    facets = {}
    if schema is not None:
        facets["schema"] = SchemaDatasetFacet(fields=[SchemaField(name=field.name, type=str(field.field_type)) for field in schema.fields])
    if snapshot is not None or uuid is not None:
        facets["cartco_snapshot"] = SnapshotDatasetFacet(snapshotId=snapshot, tableUuid=uuid)
    return Dataset(namespace=emitter.DATASET_NAMESPACE, name=name, facets=facets)


def _evidence(token, result, error):
    path, stage = token["path"], token["stage"]
    details, inputs, outputs = {}, [], []
    manifest = _read(path / "run.json") if (path / "run.json").exists() else {}
    mode = manifest.get("mode", token.get("mode_hint"))
    if mode in {"delivered", "synthetic"}:
        details["mode"] = mode
    if error is not None:
        details["errorType"] = type(error).__name__
    if stage == "prepare":
        if mode == "delivered":
            inputs = [_file(token["input_root"], f"{token['batch_date']}/{source}.csv") for source in ("shopify", "amazon", "pos")]
        outputs = [_file(path, "run.json")]
    elif stage in {"shopify", "amazon", "pos"}:
        details["source"] = stage
        inputs = [_file(path, f"inputs/{stage}.csv")]
        if (path / f"{stage}.result.json").exists():
            reference = _read(path / f"{stage}.result.json")
            receipt_bytes = Path(reference["receipt"]).read_bytes()
            if _hash(receipt_bytes) != reference["receipt_sha256"]:
                raise ValueError("Observability receipt checksum differs")
            receipt = json.loads(receipt_bytes)
            details.update(sourceBatchId=receipt["batch_id"], sourceSchemaVersion=receipt["delivery_contract"]["source_schema_version"],
                bronzeSnapshotId=receipt.get("bronze_snapshot_id"), receiptStateSnapshotId=receipt.get("state_snapshot_id"),
                stateTableUuid=receipt.get("state_table_uuid"), counts=receipt["counts"],
                receiptStateCommitted=receipt["committed"], reusedReceipt=token["cached_source"])
            from pipeline.versioned_ingestion import RAW_SCHEMA
            outputs = [_table(f"bronze.{stage}_order_events", RAW_SCHEMA, receipt.get("bronze_snapshot_id")),
                       _table("silver.order_state_v2", STATE_SCHEMA, receipt.get("state_snapshot_id"), receipt.get("state_table_uuid"))]
            with (path / f'inputs/{stage}.csv').open(encoding='utf-8',newline='') as stream:
                headers=next(csv.reader(stream))
            inputs[0]=InputDataset(namespace=inputs[0].namespace,name=inputs[0].name,
                facets={'schema':SchemaDatasetFacet(fields=[SchemaField(name=name,type='string') for name in headers])},
                inputFacets={'dataQualityMetrics':DataQualityMetricsInputDatasetFacet(rowCount=receipt['counts']['input_rows'])})
            contract_input=_file(path,f'inputs/{stage}.csv.contract.json')
            run_input=_file(path,'run.json')
            contract_input.facets['schema']=SchemaDatasetFacet(fields=[SchemaField(name=name,type='json') for name in sorted(receipt['delivery_contract'])])
            run_input.facets['schema']=SchemaDatasetFacet(fields=[SchemaField(name=name,type='json') for name in sorted(manifest)])
            previous=_table('silver.order_state_v2',STATE_SCHEMA)
            inputs.extend([contract_input,run_input,previous])
            outputs[0].facets['columnLineage']=column_lineage.facet({field.name:
                (([(inputs[0],name) for name in headers]+([(contract_input,name) for name in receipt['delivery_contract']]+[(run_input,'batch_date')] if field.name=='batch_id' else [])) if field.name in {'batch_id','raw_payload','source_row_number'} else [(run_input,'batch_date' if field.name=='ingestion_date' else 'sources')],
                 'Preserve source values in raw JSON' if field.name=='raw_payload' else 'Content-addressed batch/row ordinal or declared source/batch context') for field in RAW_SCHEMA.fields})
            outputs[1].facets['columnLineage']=column_lineage.silver(stage,inputs[0],previous)
        else:
            outputs = [_table(f"bronze.{stage}_order_events"), _table("silver.order_state_v2")]
    elif stage == "inventory":
        inputs = [_table("silver.order_state_v2")]
        enabled = manifest.get("inventory_mode", "none") != "none"
        details["inventoryEnabled"] = enabled
        if enabled:
            outputs = [_file(path, "inventory/manifest.json")]
            if (path / "inventory/manifest.json").exists():
                inventory = _read(path / "inventory/manifest.json")
                details.update(inventorySchemaVersion=inventory["schema_version"], costBasis=inventory["cost_basis"],
                    periodStart=inventory["period_start"], periodEnd=inventory["period_end"], isSynthetic=inventory["is_synthetic"],
                    receiptStateSnapshotId=inventory["state_snapshot_id"], stateTableUuid=inventory["state_table_uuid"])
                inputs[0] = _table("silver.order_state_v2", STATE_SCHEMA, inventory["state_snapshot_id"], inventory["state_table_uuid"])
    elif stage == "publish":
        inputs = [_table("silver.order_state_v2")]
        if manifest.get("inventory_mode", "none") != "none":
            inputs.append(_file(path, "inventory/manifest.json"))
        outputs = [_file(token["work_root"], "gold/CURRENT.json")]
        if (Path(token["work_root"]) / "gold/CURRENT.json").exists():
            from pipeline.gold_release import read_gold_release
            released_tables, released = read_gold_release(Path(token["work_root"]) / "gold")
            if (path / "run.json").exists() and released.get("publication_id") == _hash((path / "run.json").read_bytes()):
                details.update(releaseId=released["release_id"], publicationId=released["publication_id"],
                    receiptStateSnapshotId=released["state_snapshot_id"], stateTableUuid=released["state_table_uuid"],
                    requiredSources=released["required_sources"], martRows=released["quality"]["mart_rows"],
                    publicationOutcome="committed_before_task_failure" if error is not None else "completed_or_recovered")
                inputs[0] = _table("silver.order_state_v2", STATE_SCHEMA, released["state_snapshot_id"], released["state_table_uuid"])
                outputs = [_file(token["work_root"], f"gold/releases/{released['release_id']}/{name}") for name in released["artifacts"]]
                inventory_inputs={}
                if 'inventory' in released:
                    from pipeline.inventory import FILES
                    for filename in released['inventory']['files']:
                        inventory_inputs[filename]=_file(path,'inventory/'+filename)
                        names=released['inventory']['contract'].keys() if filename=='manifest.json' else FILES[filename]
                        inventory_inputs[filename].facets['schema']=SchemaDatasetFacet(fields=[SchemaField(name=name,type='string') for name in sorted(names)])
                    inputs=[inputs[0],*inventory_inputs.values()]
                typed=[]
                for dataset in outputs:
                    name=Path(dataset.name).stem
                    table=released_tables[name]
                    typed.append(OutputDataset(namespace=dataset.namespace,name=dataset.name,
                        facets={'schema':SchemaDatasetFacet(fields=[SchemaField(name=f.name,type=str(f.type)) for f in table.schema]),
                                'columnLineage':column_lineage.gold(name,table.column_names,inputs[0],inventory_inputs)},
                        outputFacets={'outputStatistics':OutputStatisticsOutputDatasetFacet(rowCount=table.num_rows)}))
                outputs=typed
                if "inventory" in released["quality"]:
                    quality = released["quality"]["inventory"]
                    details["inventoryQuality"] = {key: quality[key] for key in ("pools", "allocated_orders", "incomplete_pools", "zero_inventory_pools")}
    return {key: value for key, value in details.items() if value is not None}, inputs, outputs


def _event(token, event_type, result=None, error=None):
    try:
        evidence, inputs, outputs = _evidence(token, result, error)
    except Exception as exc:
        evidence, inputs, outputs = dict(evidenceErrorType=type(exc).__name__), [], []
        if error is not None:
            evidence["errorType"] = type(error).__name__
    event = RunEvent(eventType=event_type, eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=token["invocation_id"], facets={"cartco_runtime": RuntimeRunFacet(stage=token["stage"],
            batchDate=token["batch_date"], orchestrationRunId=token["run_id"], invocationId=token["invocation_id"], evidence=evidence)}),
        job=Job(namespace=emitter.JOB_NAMESPACE, name="versioned_commerce_release." + token["stage"]), producer=emitter.PRODUCER,
        inputs=inputs, outputs=outputs)
    content = Serde.to_json(event).encode()
    stem = token["invocation_id"] + "-" + event_type.value
    _write_once(token["path"] / "observability" / (stem + ".event.json"), content)
    try:
        client = emitter._get_client()
        if client is None:
            outcome = dict(status="unavailable" if emitter.OPENLINEAGE_URL else "disabled")
        else:
            client.emit(event)
            outcome = dict(status="client_completed")
    except Exception as exc:
        outcome = dict(status="failed", error_type=type(exc).__name__)
    _write_once(token["path"] / "observability" / (stem + ".delivery.json"), _bytes(dict(event_sha256=_hash(content), **outcome)))


def begin(*, stage, batch_date, run_id, path, work_root, input_root, mode_hint=None):
    token = dict(stage=stage, batch_date=batch_date, run_id=run_id, path=Path(path), work_root=Path(work_root),
        input_root=Path(input_root), mode_hint=mode_hint, invocation_id=str(uuid4()),
        cached_source=(Path(path) / f"{stage}.result.json").exists())
    best_effort(lambda: _event(token, RunState.START))
    return token


def _alert(work_root, payload, identity):
    path = Path(work_root) / "alerts" / (identity + ".json")
    expected = dict(schema_version="cartco-alert.v1", alert_id=identity, **payload)
    if path.exists():
        existing = _read(path)
        if {key: value for key, value in existing.items() if key != "created_at"} != expected:
            raise ValueError("Alert identity has conflicting content")
        return
    _write_once(path, _bytes(dict(created_at=datetime.now(timezone.utc).isoformat(), **expected)))


def finish(token, *, result=None, error=None):
    if token is None:
        return
    best_effort(lambda: _event(token, RunState.FAIL if error is not None else RunState.COMPLETE, result, error))
    if error is not None:
        best_effort(lambda: _alert(token["work_root"], dict(kind="stage_failure", terminal=False, severity="warning",
            run_id=token["run_id"], batch_date=token["batch_date"], stage=token["stage"],
            invocation_id=token["invocation_id"], error_type=type(error).__name__), "stage-" + token["invocation_id"]))


def task_alert(context, *, work_root, terminal):
    task = context["task_instance"]
    conf = getattr(context.get("dag_run"), "conf", None) or {}
    exception = context.get("exception")
    payload = dict(kind="task_failed" if terminal else "retry_scheduled", terminal=terminal,
        severity="error" if terminal else "warning", dag_id=task.dag_id, task_id=task.task_id,
        run_id=task.run_id, reported_try_number=task.try_number, batch_date=conf.get("batch_date", context.get("ds")),
        error_type=type(exception).__name__ if exception is not None else None)
    identity = "task-" + _hash(_bytes({key: payload[key] for key in ("dag_id", "task_id", "run_id", "reported_try_number", "kind")}))
    _alert(work_root, payload, identity)


def best_effort(action):
    try:
        return action()
    except Exception:
        try:
            logger.warning("Runtime observability unavailable; task result is unchanged", exc_info=True)
        except Exception:
            pass
        return None
