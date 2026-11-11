import json
import os
from pathlib import Path
import sqlite3
import tempfile

import pytest

pytest.importorskip("sqlalchemy", minversion="2.0", reason="Use the isolated local-file environment")
from pyiceberg.catalog.sql import SqlCatalog

from pipeline import runtime
from pipeline.gold_release import read_gold_release
from pipeline.recovery import audit, create_backup, restore_backup, verify_backup
from pipeline.versioned_ingestion import STATE_TABLE
from pos_helpers import DAY
from runtime_helpers import deliveries

pytestmark = pytest.mark.file_integration


@pytest.fixture
def source(tmp_path_factory, monkeypatch):
    if os.name == "nt":
        scratch = Path(__file__).resolve().parents[3] / "work/b"
        scratch.mkdir(parents=True, exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="t", dir=scratch))
    else:
        root = tmp_path_factory.mktemp("recovery")
    db, warehouse, work = root / "catalog.db", root / "w", root / "r"
    incoming = deliveries(root / "input")
    catalog = SqlCatalog("recovery", uri="sqlite:///" + db.as_posix(), warehouse="file://" + warehouse.as_posix())
    from lineage import emitter
    monkeypatch.setattr(emitter, "_get_client", lambda: None)
    context = dict(work_root=work, batch_date=DAY, run_id="backup-run")
    runtime.prepare_run(**context, input_root=incoming, allow_empty_init=True)
    for name in runtime.SOURCE_ORDER:
        runtime.ingest_source(**context, source=name, catalog=catalog)
    runtime.prepare_inventory(**context, input_root=incoming, catalog=catalog)
    runtime.publish_run(**context, catalog=catalog)
    catalog.engine.dispose()
    return root, db, warehouse, work


def backup(source):
    root, db, warehouse, work = source
    path = root / "backup"
    manifest = create_backup(db, warehouse, work, path, catalog_name="recovery", writers_stopped=True)
    return path, manifest


def quarantine_sources(source):
    root, db, warehouse, work = source
    # Reversible disaster simulation only in this test's explicitly resolved root.
    for path in (db, warehouse, work):
        saved = root / (path.name + "-preserved")
        assert path.resolve().is_relative_to(root.resolve()) and saved.resolve().is_relative_to(root.resolve())
        path.rename(saved)


def test_restore_preserves_rows_retained_snapshots_gold_and_replay(source):
    root, db, warehouse, work = source
    path, manifest = backup(source)
    assert verify_backup(path)["audit"] == manifest["audit"]
    quarantine_sources(source)
    assert restore_backup(path, db, warehouse, work, writers_stopped=True) == manifest["audit"]
    catalog = SqlCatalog("recovery", uri="sqlite:///" + db.as_posix(), warehouse="file://" + warehouse.as_posix())
    try:
        state = catalog.load_table(STATE_TABLE)
        assert state.scan().to_arrow().num_rows == 3 and len(state.metadata.snapshots) == 3
        assert state.scan(snapshot_id=state.metadata.snapshots[0].snapshot_id).to_arrow().num_rows == 1
        tables, release = read_gold_release(work / "gold")
        assert len(tables) == 4 and release["release_id"] == manifest["audit"]["gold"]["release_id"]
        context = dict(work_root=work, batch_date=DAY, run_id="backup-run", catalog=catalog)
        for name in runtime.SOURCE_ORDER:
            runtime.ingest_source(**context, source=name)
        assert runtime.publish_run(**context)["release_id"] == release["release_id"]
        assert len(catalog.load_table(STATE_TABLE).metadata.snapshots) == 3
    finally:
        catalog.engine.dispose()


def test_backup_and_restore_require_actual_quiescence_acknowledgement(source):
    root, db, warehouse, work = source
    with pytest.raises(ValueError, match="Stop all"):
        create_backup(db, warehouse, work, root / "no", catalog_name="recovery")
    path, _ = backup(source)
    with pytest.raises(ValueError, match="Stop all"):
        restore_backup(path, db, warehouse, work)


def test_restore_refuses_existing_targets_and_relocation(source):
    root, db, warehouse, work = source
    path, _ = backup(source)
    with pytest.raises(ValueError, match="existing targets"):
        restore_backup(path, db, warehouse, work, writers_stopped=True)
    with pytest.raises(ValueError, match="original paths"):
        restore_backup(path, root / "other.db", warehouse, work, writers_stopped=True)


def test_tampering_is_detected_before_creating_restore_targets(source):
    root, db, warehouse, work = source
    path, _ = backup(source)
    (path / "payload/catalog.sqlite").write_bytes(b"tampered")
    quarantine_sources(source)
    with pytest.raises(ValueError, match="checksum/size"):
        restore_backup(path, db, warehouse, work, writers_stopped=True)
    assert not db.exists() and not warehouse.exists() and not work.exists()


def test_backup_cannot_be_created_inside_source_or_replace_existing_backup(source):
    root, db, warehouse, work = source
    with pytest.raises(ValueError, match="outside source"):
        create_backup(db, warehouse, work, work / "backup", catalog_name="recovery", writers_stopped=True)
    path, _ = backup(source)
    with pytest.raises(ValueError, match="must be new"):
        create_backup(db, warehouse, work, path, catalog_name="recovery", writers_stopped=True)


def test_wrong_catalog_name_does_not_produce_an_empty_successful_backup(source):
    root, db, warehouse, work = source
    with pytest.raises(ValueError, match="exact single"):
        create_backup(db, warehouse, work, root / "no", catalog_name="wrong-name", writers_stopped=True)


def test_active_publication_lock_blocks_backup(source):
    root, db, warehouse, work = source
    (work / "gold/.publication.lock").write_text("active")
    with pytest.raises(ValueError, match="Publication lock"):
        create_backup(db, warehouse, work, root / "no", catalog_name="recovery", writers_stopped=True)


def test_path_traversal_and_unlisted_payload_are_rejected(source):
    root, db, warehouse, work = source
    path, manifest = backup(source)
    manifest["files"]["../escape"] = dict(bytes=0, sha256="0" * 64)
    (path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="Unsafe"):
        verify_backup(path)


def test_catalog_write_is_blocked_during_copy_and_released_afterward(source, monkeypatch):
    import pipeline.recovery as module
    root, db, warehouse, work = source
    original = module.shutil.copyfile
    checked = []
    def copy(*args):
        if not checked:
            connection = sqlite3.connect(db, timeout=0.01)
            try:
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    connection.execute("BEGIN IMMEDIATE")
                checked.append(True)
            finally:
                connection.close()
        return original(*args)
    monkeypatch.setattr(module.shutil, "copyfile", copy)
    backup(source)
    assert checked
    connection = sqlite3.connect(db, timeout=1)
    connection.execute("BEGIN IMMEDIATE")
    connection.rollback()
    connection.close()



def test_runtime_mutation_during_copy_leaves_incomplete_backup_not_accepted(source, monkeypatch):
    import pipeline.recovery as module
    root, db, warehouse, work = source
    original = module.shutil.copyfile
    mutated = []
    def copy(*args):
        result = original(*args)
        if not mutated:
            (work / "changed-during-backup.txt").write_text("concurrent writer")
            mutated.append(True)
        return result
    monkeypatch.setattr(module.shutil, "copyfile", copy)
    with pytest.raises(ValueError, match="Source changed"):
        backup(source)
    assert not (root / "backup/manifest.json").exists()
    with pytest.raises(ValueError, match="incomplete"):
        verify_backup(root / "backup")


def test_inventory_evidence_and_observability_are_restored(source, monkeypatch):
    from pipeline.source_contracts import TIMEZONE_PROPERTY
    from scripts.generate_inventory_bundle import generate_bundle
    from pipeline.gold_release import publish_gold_release
    root, db, warehouse, work = source
    catalog = SqlCatalog("recovery", uri="sqlite:///" + db.as_posix(), warehouse="file://" + warehouse.as_posix())
    try:
        state = catalog.load_table(STATE_TABLE)
        bundle = root / "inventory-source"
        generate_bundle(state.scan().to_arrow(), bundle, state_uuid=str(state.metadata.table_uuid),
            snapshot_id=state.current_snapshot().snapshot_id, business_timezone=state.properties[TIMEZONE_PROPERTY],
            period_start="2026-01-01", period_end="2026-01-03")
        run = runtime.run_path(work, DAY, "backup-run")
        frozen = json.loads((run / "run.json").read_text())
        receipts = [runtime._receipt(run, name, frozen) for name in runtime.SOURCE_ORDER]
        publish_gold_release(catalog, work / "gold", receipts, batch_date=DAY, inventory_bundle=bundle)
        (work / "alerts").mkdir()
        (work / "alerts/retained-alert.json").write_text('{"schema_version":"cartco-alert.v1","terminal":true}')
    finally:
        catalog.engine.dispose()
    path, manifest = backup(source)
    quarantine_sources(source)
    restore_backup(path, db, warehouse, work, writers_stopped=True)
    tables, restored = read_gold_release(work / "gold")
    assert len(tables) == 5 and restored["release_id"] == manifest["audit"]["gold"]["release_id"]
    assert (work / "alerts/retained-alert.json").is_file()
