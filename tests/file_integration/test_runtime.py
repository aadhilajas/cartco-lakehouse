import json
import os
from pathlib import Path
import tempfile

import pytest

pytest.importorskip("sqlalchemy", minversion="2.0", reason="Use isolated file-integration dependencies")
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError

from pipeline import runtime
from pipeline.gold_release import read_gold_release
from pipeline.versioned_ingestion import STATE_TABLE
from pos_helpers import DAY, receipt, write_csv
from runtime_helpers import deliveries

pytestmark = pytest.mark.file_integration


@pytest.fixture
def environment(tmp_path_factory, monkeypatch):
    if os.name == "nt":
        scratch = Path(__file__).resolve().parents[3] / "work/u"
        scratch.mkdir(parents=True, exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="t", dir=scratch))
    else:
        root = tmp_path_factory.mktemp("runtime")
    incoming, work = deliveries(root / "input"), root / "runtime"
    from lineage import emitter
    monkeypatch.setattr(emitter, "_get_client", lambda: None)
    monkeypatch.setattr(emitter, "OPENLINEAGE_URL", "")
    catalog = SqlCatalog("runtime", uri="sqlite:///" + (root / "catalog.db").as_posix(), warehouse="file://" + (root / "w").as_posix())
    monkeypatch.setattr(runtime, "get_catalog", lambda: catalog)
    for key, value in dict(CARTCO_RUNTIME_WORK_ROOT=str(work), CARTCO_RUNTIME_INPUT_ROOT=str(incoming),
        CARTCO_RUNTIME_MODE="delivered", CARTCO_RUNTIME_INVENTORY_MODE="none", CARTCO_RUNTIME_ALLOW_EMPTY_INIT="true").items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("CARTCO_RUNTIME_ROWS_PER_SOURCE", raising=False)
    yield catalog, root, work, incoming
    catalog.engine.dispose()


def execute(run_id):
    runtime.prepare_task(DAY, run_id)
    for source in runtime.SOURCE_ORDER:
        runtime.ingest_task(DAY, run_id, source)
    runtime.inventory_task(DAY, run_id)
    return runtime.publish_task(DAY, run_id)


def test_exact_dag_callbacks_delivered_data_retry_and_sealed_receipts(environment):
    catalog, root, work, incoming = environment
    first = execute("run-1")
    pointer = (work / "gold/CURRENT.json").read_bytes()
    receipts = list((work / "archive").rglob("receipts/*.json"))
    (incoming / DAY / "pos.csv").write_bytes(b"changed outside frozen run")
    assert execute("run-1") == first
    assert (work / "gold/CURRENT.json").read_bytes() == pointer
    assert len(list((work / "archive").rglob("receipts/*.json"))) == len(receipts) == 3
    assert len(catalog.load_table(STATE_TABLE).metadata.snapshots) == 3
    tables, manifest = read_gold_release(work / "gold")
    assert manifest["quality"]["active_orders"] == 3 and len(tables) == 4
    assert manifest["quality"]["retained_completed_sales"] == "20.20"


def test_explicit_synthetic_runtime_produces_five_artifacts(environment, monkeypatch):
    catalog, root, work, incoming = environment
    monkeypatch.setenv("CARTCO_RUNTIME_MODE", "synthetic")
    monkeypatch.setenv("CARTCO_RUNTIME_INVENTORY_MODE", "synthetic")
    monkeypatch.setenv("CARTCO_RUNTIME_ROWS_PER_SOURCE", "7")
    first = execute("fixture-run")
    assert execute("fixture-run") == first
    tables, manifest = read_gold_release(work / "gold")
    assert manifest["quality"]["active_orders"] == 21 and len(tables) == 5
    assert manifest["inventory"]["contract"]["is_synthetic"] is True
    assert len(catalog.load_table(STATE_TABLE).metadata.snapshots) == 3


def test_frozen_empty_init_denial_requires_a_new_explicit_run(environment, monkeypatch):
    catalog, root, work, incoming = environment
    monkeypatch.setenv("CARTCO_RUNTIME_ALLOW_EMPTY_INIT", "false")
    runtime.prepare_task(DAY, "denied")
    with pytest.raises(ValueError, match="explicitly initialize"):
        runtime.ingest_task(DAY, "denied", "shopify")
    monkeypatch.setenv("CARTCO_RUNTIME_ALLOW_EMPTY_INIT", "true")
    with pytest.raises(ValueError, match="explicitly initialize"):
        runtime.ingest_task(DAY, "denied", "shopify")
    with pytest.raises(NoSuchTableError):
        catalog.load_table(STATE_TABLE)
    execute("authorized-new-run")


def test_incomplete_or_invalid_source_run_never_advances_gold(environment):
    catalog, root, work, incoming = environment
    write_csv(incoming / DAY / "pos.csv", {**receipt(), "status": "partial_refund"})
    runtime.prepare_task(DAY, "invalid")
    runtime.ingest_task(DAY, "invalid", "shopify")
    runtime.ingest_task(DAY, "invalid", "amazon")
    with pytest.raises(ValueError):
        runtime.ingest_task(DAY, "invalid", "pos")
    with pytest.raises(FileNotFoundError):
        runtime.publish_task(DAY, "invalid")
    assert not (work / "gold/CURRENT.json").exists()
    assert len(catalog.load_table(STATE_TABLE).metadata.snapshots) == 2
    write_csv(incoming / DAY / "pos.csv", receipt())
    execute("corrected-new-run")
    assert len(catalog.load_table(STATE_TABLE).metadata.snapshots) == 3


def test_lost_source_result_after_commit_replays_without_new_snapshot(environment, monkeypatch):
    catalog, root, work, incoming = environment
    runtime.prepare_task(DAY, "lost-source")
    original = runtime._write_once
    failed = []
    def write(path, data):
        if path.name == "shopify.result.json" and not failed:
            failed.append(True)
            raise OSError("lost task result")
        return original(path, data)
    monkeypatch.setattr(runtime, "_write_once", write)
    with pytest.raises(OSError):
        runtime.ingest_task(DAY, "lost-source", "shopify")
    snapshot = catalog.load_table(STATE_TABLE).current_snapshot().snapshot_id
    result = runtime.ingest_task(DAY, "lost-source", "shopify")
    assert catalog.load_table(STATE_TABLE).current_snapshot().snapshot_id == snapshot
    assert json.loads(Path(result["receipt"]).read_text())["committed"] is False
    execute("lost-source")


def test_lost_publication_result_is_recovered_without_moving_pointer(environment, monkeypatch):
    catalog, root, work, incoming = environment
    runtime.prepare_task(DAY, "lost-publication")
    for source in runtime.SOURCE_ORDER:
        runtime.ingest_task(DAY, "lost-publication", source)
    runtime.inventory_task(DAY, "lost-publication")
    original = runtime._write_once
    failed = []
    def write(path, data):
        if path.name == "published.json" and not failed:
            failed.append(True)
            raise OSError("lost publication task result")
        return original(path, data)
    monkeypatch.setattr(runtime, "_write_once", write)
    with pytest.raises(OSError):
        runtime.publish_task(DAY, "lost-publication")
    pointer = (work / "gold/CURRENT.json").read_bytes()
    runtime.publish_task(DAY, "lost-publication")
    assert (work / "gold/CURRENT.json").read_bytes() == pointer
    assert len(list((work / "gold/releases").iterdir())) == 1


def test_old_run_retry_does_not_replace_newer_release(environment):
    catalog, root, work, incoming = environment
    first = execute("first")
    second = execute("second")
    assert first["release_id"] != second["release_id"]
    pointer = (work / "gold/CURRENT.json").read_bytes()
    with pytest.raises(ValueError, match="refusing to roll back"):
        runtime.publish_task(DAY, "first")
    assert (work / "gold/CURRENT.json").read_bytes() == pointer


def test_delivered_inventory_is_copied_once_and_survives_external_input_change(environment, monkeypatch):
    from scripts.generate_inventory_bundle import generate_bundle
    catalog, root, work, incoming = environment
    monkeypatch.setenv("CARTCO_RUNTIME_INVENTORY_MODE", "delivered")
    runtime.prepare_task(DAY, "inventory")
    for source in runtime.SOURCE_ORDER:
        runtime.ingest_task(DAY, "inventory", source)
    table = catalog.load_table(STATE_TABLE)
    bundle = incoming / DAY / "inventory"
    generate_bundle(table.scan().to_arrow(), bundle, state_uuid=str(table.metadata.table_uuid),
        snapshot_id=table.current_snapshot().snapshot_id, business_timezone="Asia/Kolkata",
        period_start="2026-01-01", period_end="2026-01-03")
    runtime.inventory_task(DAY, "inventory")
    (bundle / "scope.csv").write_bytes(b"changed after intake")
    runtime.inventory_task(DAY, "inventory")
    runtime.publish_task(DAY, "inventory")
    assert len(read_gold_release(work / "gold")[0]) == 5



def test_inventory_result_loss_resumes_immutable_copy(environment, monkeypatch):
    catalog, root, work, incoming = environment
    monkeypatch.setenv("CARTCO_RUNTIME_MODE", "synthetic")
    monkeypatch.setenv("CARTCO_RUNTIME_INVENTORY_MODE", "synthetic")
    monkeypatch.setenv("CARTCO_RUNTIME_ROWS_PER_SOURCE", "3")
    runtime.prepare_task(DAY, "lost-inventory")
    for source in runtime.SOURCE_ORDER:
        runtime.ingest_task(DAY, "lost-inventory", source)
    original = runtime._write_once
    failed = []
    def write(path, data):
        if path.name == "inventory.result.json" and not failed:
            failed.append(True)
            raise OSError("lost inventory task result")
        return original(path, data)
    monkeypatch.setattr(runtime, "_write_once", write)
    with pytest.raises(OSError):
        runtime.inventory_task(DAY, "lost-inventory")
    runtime.inventory_task(DAY, "lost-inventory")
    runtime.publish_task(DAY, "lost-inventory")
    assert len(read_gold_release(work / "gold")[0]) == 5
    assert len(catalog.load_table(STATE_TABLE).metadata.snapshots) == 3


def test_lost_old_publication_ack_cannot_roll_back_a_new_run(environment, monkeypatch):
    catalog, root, work, incoming = environment
    original = runtime._write_once
    failed = []
    def write(path, data):
        if path.name == "published.json" and not failed:
            failed.append(True)
            raise OSError("lost old run acknowledgement")
        return original(path, data)
    monkeypatch.setattr(runtime, "_write_once", write)
    with pytest.raises(OSError):
        execute("old-unacknowledged")
    execute("new-acknowledged")
    pointer = (work / "gold/CURRENT.json").read_bytes()
    with pytest.raises(ValueError, match="pointer changed"):
        runtime.publish_task(DAY, "old-unacknowledged")
    assert (work / "gold/CURRENT.json").read_bytes() == pointer


@pytest.fixture
def observed(environment, monkeypatch):
    from lineage import emitter
    from openlineage.client import OpenLineageClient
    from openlineage.client.transport.file import FileConfig, FileTransport
    catalog, root, work, incoming = environment
    capture = root / "openlineage.jsonl"
    client = OpenLineageClient(transport=FileTransport(FileConfig(log_file_path=str(capture), append=True)))
    monkeypatch.setattr(emitter, "_get_client", lambda: client)
    return catalog, root, work, incoming, capture


def test_pinned_client_serialization_snapshot_facets_and_cached_attempts(observed):
    from scripts.check_runtime_observability import inspect_run
    catalog, root, work, incoming, capture = observed
    first = execute("observed")
    assert execute("observed") == first
    events = [json.loads(line) for line in capture.read_text().splitlines()]
    assert len(events) == 24
    report = inspect_run(work, DAY, "observed")
    assert report["clean_history"] and report["all_client_calls_completed"] and report["invocations"] == 12
    assert not report["backend_graph_verified"] and not report["notifications_sent"]
    for source in runtime.SOURCE_ORDER:
        complete = [event for event in events if event["job"]["name"].endswith("." + source) and event["eventType"] == "COMPLETE"]
        assert [event["run"]["facets"]["cartco_runtime"]["evidence"]["reusedReceipt"] for event in complete] == [False, True]
        state_output = next(dataset for dataset in complete[0]["outputs"] if dataset["name"] == STATE_TABLE)
        fields = {field["name"]: field["type"] for field in state_output["facets"]["schema"]["fields"]}
        assert fields["unit_price"] == "decimal(18, 2)" and fields["currency"] == "string"
        assert state_output["facets"]["cartco_snapshot"]["tableUuid"] == str(catalog.load_table(STATE_TABLE).metadata.table_uuid)
    publish = [event for event in events if event["job"]["name"].endswith(".publish") and event["eventType"] == "COMPLETE"][0]
    assert publish["inputs"][0]["facets"]["cartco_snapshot"]["snapshotId"] == first["state_snapshot_id"]
    assert all(dataset["name"].startswith("gold/releases/" + first["release_id"]) for dataset in publish["outputs"])


def test_failed_source_journal_and_terminal_alert_do_not_export_raw_rows(observed):
    from types import SimpleNamespace
    from scripts.check_runtime_observability import inspect_run
    catalog, root, work, incoming, capture = observed
    write_csv(incoming / DAY / "pos.csv", {**receipt(), "status": "partially_refunded", "unknown_field": "PRIVATE_RAW_ROW_VALUE"})
    with pytest.raises(ValueError):
        execute("failed-observed")
    task = SimpleNamespace(dag_id="versioned_commerce_release", task_id="ingest_pos", run_id="failed-observed", try_number=3)
    runtime.task_failure_callback(dict(task_instance=task, dag_run=SimpleNamespace(conf={"batch_date": DAY}), ds=DAY, exception=ValueError("PRIVATE_RAW_ROW_VALUE")))
    report = inspect_run(work, DAY, "failed-observed")
    assert report["event_counts"]["FAIL"] == 1 and report["terminal_alerts"] == 1 and not report["clean_history"]
    assert not (work / "gold/CURRENT.json").exists()
    run = runtime.run_path(work, DAY, "failed-observed")
    files = list((run / "observability").glob("*.json")) + list((work / "alerts").glob("*.json")) + [capture]
    assert all("PRIVATE_RAW_ROW_VALUE" not in path.read_text() for path in files)


def test_transport_failure_is_audit_metadata_not_a_business_failure(environment, monkeypatch):
    from lineage import emitter
    from scripts.check_runtime_observability import inspect_run
    catalog, root, work, incoming = environment
    class Client:
        def emit(self, event):
            raise OSError("backend unavailable")
    monkeypatch.setattr(emitter, "_get_client", lambda: Client())
    execute("transport-down")
    report = inspect_run(work, DAY, "transport-down")
    assert report["clean_history"] and report["delivery_counts"]["failed"] == 12
    assert not report["all_client_calls_completed"] and report["terminal_alerts"] == 0
    assert len(read_gold_release(work / "gold")[0]) == 4


def test_publication_committed_before_task_failure_is_reported_truthfully(observed, monkeypatch):
    catalog, root, work, incoming, capture = observed
    original = runtime._write_once
    failed = []
    def write(path, data):
        if path.name == "published.json" and not failed:
            failed.append(True)
            raise OSError("acknowledgement lost after commit")
        return original(path, data)
    monkeypatch.setattr(runtime, "_write_once", write)
    with pytest.raises(OSError):
        execute("committed-but-failed")
    _, manifest = read_gold_release(work / "gold")
    events = [json.loads(line) for line in capture.read_text().splitlines()]
    failure = next(event for event in events if event["job"]["name"].endswith(".publish") and event["eventType"] == "FAIL")
    evidence = failure["run"]["facets"]["cartco_runtime"]["evidence"]
    assert evidence["publicationOutcome"] == "committed_before_task_failure" and evidence["releaseId"] == manifest["release_id"]
    assert evidence["errorType"] == "OSError"
    pointer = (work / "gold/CURRENT.json").read_bytes()
    runtime.publish_task(DAY, "committed-but-failed")
    assert (work / "gold/CURRENT.json").read_bytes() == pointer
