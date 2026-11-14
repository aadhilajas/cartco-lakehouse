import json

import pytest

from pipeline import runtime
from pipeline.gold_release import publish_gold_release
from pos_helpers import DAY
from runtime_helpers import deliveries


def test_freeze_is_immutable_and_retry_does_not_read_changed_deliveries(tmp_path):
    incoming = deliveries(tmp_path / "deliveries")
    kwargs = dict(work_root=tmp_path / "work", input_root=incoming, batch_date=DAY, run_id="run-1")
    first = runtime.prepare_run(**kwargs)
    path = runtime.run_path(kwargs["work_root"], DAY, "run-1")
    before = (path / "run.json").read_bytes(), (path / "inputs/shopify.csv").read_bytes()
    (incoming / DAY / "shopify.csv").write_bytes(b"changed after freeze")
    assert runtime.prepare_run(**kwargs, mode="synthetic", rows_per_source=1, allow_empty_init=True) == first
    assert before == ((path / "run.json").read_bytes(), (path / "inputs/shopify.csv").read_bytes())


def test_frozen_input_tampering_is_not_repaired(tmp_path):
    incoming = deliveries(tmp_path / "deliveries")
    kwargs = dict(work_root=tmp_path / "work", input_root=incoming, batch_date=DAY, run_id="run-1")
    runtime.prepare_run(**kwargs)
    path = runtime.run_path(kwargs["work_root"], DAY, "run-1")
    (path / "inputs/pos.csv").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        runtime.prepare_run(**kwargs)


def test_missing_delivery_is_not_silently_generated(tmp_path):
    with pytest.raises(FileNotFoundError):
        runtime.prepare_run(work_root=tmp_path / "work", input_root=tmp_path / "absent", batch_date=DAY, run_id="run-1")
    assert not (tmp_path / "work").exists()


@pytest.mark.parametrize("change", [{"mode": "unknown"}, {"inventory_mode": "unknown"}, {"rows_per_source": 1},
    {"allow_empty_init": "true"}, {"inventory_mode": "synthetic"}, {"mode": "synthetic", "rows_per_source": 0}])
def test_runtime_does_not_infer_unsafe_configuration(tmp_path, change):
    with pytest.raises(ValueError):
        runtime.prepare_run(work_root=tmp_path / "work", input_root=tmp_path / "input", batch_date=DAY, run_id="run-1", **change)


def test_mismatched_source_windows_fail_before_run_is_sealed(tmp_path):
    incoming = deliveries(tmp_path / "input")
    path = incoming / DAY / "pos.csv.contract.json"
    contract = json.loads(path.read_text())
    contract["extraction_end"] = "2026-01-07T00:00:00Z"
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="different windows"):
        runtime.prepare_run(work_root=tmp_path / "work", input_root=incoming, batch_date=DAY, run_id="run-1")
    assert not (tmp_path / "work").exists()


def test_run_id_is_hashed_instead_of_used_as_a_path(tmp_path):
    result = runtime.run_path(tmp_path, DAY, "../../escape")
    assert result.is_relative_to(tmp_path) and "escape" not in str(result)
    with pytest.raises(ValueError):
        runtime.run_path(tmp_path, None, "run")


def test_catalog_factory_uses_rest_without_sqlalchemy_upgrade(monkeypatch):
    import pyiceberg.catalog
    recorded = {}
    def load(name, **kwargs):
        recorded.update(kwargs)
        return "catalog"
    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", load)
    monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://catalog.test:8181")
    assert runtime.get_catalog() == "catalog"
    assert recorded["type"] == "rest" and recorded["uri"] == "http://catalog.test:8181"


def test_pointer_precondition_is_checked_before_catalog_access_and_preserves_pointer(tmp_path):
    root = tmp_path / "gold"
    root.mkdir()
    (root / "CURRENT.json").write_bytes(b"another publisher's pointer")
    with pytest.raises(ValueError, match="pointer changed"):
        publish_gold_release(object(), root, [], batch_date=DAY, publication_precondition={"pointer_sha256": None})
    assert (root / "CURRENT.json").read_bytes() == b"another publisher's pointer"
    assert not (root / ".publication.lock").exists()
