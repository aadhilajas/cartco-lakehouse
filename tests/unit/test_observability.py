import json
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace

import jsonschema
import pytest
from openlineage.client.serde import Serde

from lineage import emitter
from pipeline import observability, runtime
from pos_helpers import DAY
from scripts.check_runtime_observability import inspect_run


@pytest.fixture
def tmp_path(tmp_path_factory):
    if os.name != "nt":
        return tmp_path_factory.mktemp("observability")
    scratch = Path(__file__).resolve().parents[3] / "work/o"
    scratch.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="t", dir=scratch))


@pytest.fixture
def capture(tmp_path, monkeypatch):
    captured = []
    class Client:
        def emit(self, event):
            captured.append(Serde.to_dict(event))
    monkeypatch.setattr(emitter, "_get_client", lambda: Client())
    monkeypatch.setenv("CARTCO_RUNTIME_WORK_ROOT", str(tmp_path))
    monkeypatch.setenv("CARTCO_RUNTIME_INPUT_ROOT", str(tmp_path / "input"))
    return tmp_path, captured


def event_files(root):
    path = runtime.run_path(root, DAY, "run-1")
    return list((path / "observability").glob("*.event.json"))


def test_runtime_serializes_paired_events_and_custom_schema(capture):
    root, captured = capture
    value = runtime._logged("prepare", DAY, "run-1", lambda: 42)
    assert value == 42
    assert [event["eventType"] for event in captured] == ["START", "COMPLETE"]
    assert captured[0]["run"]["runId"] == captured[1]["run"]["runId"]
    schema = json.loads((Path(__file__).resolve().parents[2] / "docs/schemas/cartco-observability-v1.json").read_text())
    validator = jsonschema.Draft202012Validator(schema["$defs"]["RuntimeRunFacet"], format_checker=jsonschema.FormatChecker())
    for event in captured:
        validator.validate(event["run"]["facets"]["cartco_runtime"])
    report = inspect_run(root, DAY, "run-1")
    assert report["event_counts"]["COMPLETE"] == 1 and report["all_client_calls_completed"]
    assert not report["validation_errors"] and report["incomplete_invocations"] == 0


@pytest.mark.parametrize("falsey", [False, True])
def test_primary_exception_is_preserved_and_alert_does_not_export_error_text(capture, falsey):
    root, captured = capture
    class FalseyError(ValueError):
        def __bool__(self):
            return False
    error = FalseyError("TOP_SECRET_RAW_VALUE") if falsey else ValueError("TOP_SECRET_RAW_VALUE")
    def fail():
        raise error
    with pytest.raises(ValueError) as caught:
        runtime._logged("prepare", DAY, "run-1", fail)
    assert caught.value is error
    assert [event["eventType"] for event in captured] == ["START", "FAIL"]
    files = event_files(root) + list((root / "alerts").glob("*.json"))
    assert all("TOP_SECRET_RAW_VALUE" not in path.read_text() for path in files)
    alert = json.loads(next((root / "alerts").glob("*.json")).read_text())
    assert alert["error_type"] == type(error).__name__ and not alert["terminal"]


@pytest.mark.parametrize("outcome", ["disabled", "unavailable", "failed"])
def test_client_outage_never_changes_success(capture, monkeypatch, outcome):
    root, captured = capture
    if outcome == "failed":
        class Client:
            def emit(self, event):
                raise OSError("PRIVATE_ENDPOINT_VALUE")
        monkeypatch.setattr(emitter, "_get_client", lambda: Client())
    else:
        monkeypatch.setattr(emitter, "_get_client", lambda: None)
        monkeypatch.setattr(emitter, "OPENLINEAGE_URL", "configured" if outcome == "unavailable" else "")
    assert runtime._logged("prepare", DAY, "run-1", lambda: 42) == 42
    report = inspect_run(root, DAY, "run-1")
    assert report["delivery_counts"][outcome] == 2 and not report["all_client_calls_completed"]
    assert all("PRIVATE_ENDPOINT_VALUE" not in path.read_text() for path in runtime.run_path(root, DAY, "run-1").rglob("*.json"))


def test_instrumentation_programming_error_cannot_replace_primary_outcome(capture, monkeypatch):
    root, captured = capture
    def broken(*args, **kwargs):
        raise RuntimeError("instrumentation failure")
    monkeypatch.setattr(observability, "begin", broken)
    monkeypatch.setattr(observability, "finish", broken)
    assert runtime._logged("prepare", DAY, "run-1", lambda: 17) == 17
    error = KeyError("primary")
    def fail():
        raise error
    with pytest.raises(KeyError) as caught:
        runtime._logged("prepare", DAY, "run-1", fail)
    assert caught.value is error


def test_journal_storage_failure_does_not_fail_data_action(capture, monkeypatch):
    root, captured = capture
    def denied(*args):
        raise PermissionError("read only journal")
    monkeypatch.setattr(observability, "_write_once", denied)
    assert runtime._logged("prepare", DAY, "run-1", lambda: 8) == 8
    assert captured == []


def test_airflow_retry_and_terminal_callbacks_deduplicate_without_notifications(capture):
    root, captured = capture
    task = SimpleNamespace(dag_id="versioned_commerce_release", task_id="ingest_pos", run_id="run-1", try_number=3)
    context = dict(task_instance=task, dag_run=SimpleNamespace(conf={"batch_date": DAY}), ds=DAY, exception=TimeoutError("TOP_SECRET"))
    runtime.task_retry_callback(context)
    runtime.task_retry_callback(context)
    runtime.task_failure_callback(context)
    runtime.task_failure_callback(context)
    records = [json.loads(path.read_text()) for path in (root / "alerts").glob("*.json")]
    assert len(records) == 2 and sum(record["terminal"] for record in records) == 1
    assert all(record["reported_try_number"] == 3 for record in records)
    assert captured == [] and all("TOP_SECRET" not in json.dumps(record) for record in records)


def test_legacy_emitter_handles_construction_errors_and_reports_disabled_truthfully(capture, monkeypatch, caplog):
    monkeypatch.setattr(emitter, "_get_client", lambda: None)
    monkeypatch.setattr(emitter, "OPENLINEAGE_URL", "")
    with caplog.at_level("INFO"):
        emitter.emit_start("job", emitter.new_run_id())
    assert "transport=disabled" in caplog.text and "Emitted" not in caplog.text
    def fail(*args):
        raise RuntimeError("construction failed")
    monkeypatch.setattr(emitter, "_run_event", fail)
    emitter.emit_start("job", "run")
    emitter.emit_complete("job", "run")
    emitter.emit_fail("job", "run")


def test_inspector_detects_tampering_and_empty_journal_is_not_verified(capture):
    root, captured = capture
    assert not inspect_run(root, DAY, "empty")["all_client_calls_completed"]
    runtime._logged("prepare", DAY, "run-1", lambda: None)
    path = event_files(root)[0]
    data = json.loads(path.read_text())
    data["run"]["facets"]["cartco_runtime"]["evidence"]["mode"] = "synthetic"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert inspect_run(root, DAY, "run-1")["validation_errors"]
