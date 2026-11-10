import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile

import pyarrow.parquet as pq
import pytest

pytest.importorskip("sqlalchemy", minversion="2.0", reason="Use the isolated file-integration environment")
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import schema_to_pyarrow

from order_state_helpers import APPLIED, events, order
from pipeline.order_state import active_orders
from pipeline.state_migration import install_candidate
from pipeline.versioned_ingestion import STATE_TABLE, run_versioned_batch
from scripts.prepare_order_state import prepare_candidate
from transforms.silver_shopify_orders import TABLE_SCHEMA as LEGACY_SCHEMA

pytestmark = pytest.mark.file_integration


@pytest.fixture
def tmp_path(tmp_path_factory):
    if os.name != "nt":
        return tmp_path_factory.mktemp("versioned")
    # PyArrow 15 on Windows cannot write long hash-partition paths below the
    # sandbox's deep temp folder. Keep real test files in short workspace scratch.
    scratch = Path(__file__).resolve().parents[3] / "work" / "v"
    scratch.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="t", dir=scratch))


@pytest.fixture
def catalog(tmp_path):
    catalog = SqlCatalog("flow", uri="sqlite:///" + (tmp_path / "catalog.db").as_posix(),
                         warehouse="file://" + (tmp_path / "warehouse").as_posix())
    yield catalog
    catalog.engine.dispose()


def source_order(source="shopify", *, version=2, status=None, key="o1"):
    row = dict(order_id=key, customer_id="c1", channel=source, order_date="2026-01-01T08:00:00",
               product_category="Apparel", quantity="1", unit_price="10.00",
               status=status or ("delivered" if source == "shopify" else "shipped"),
               source_updated_at=f"2026-01-{version:02d}T00:00:00Z", is_deleted="false", extra_column="keep me")
    row["asin" if source == "amazon" else "product_id"] = "p1"
    return row


def csv_file(tmp_path, *rows):
    path = tmp_path / "input.csv"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=sorted({key for row in rows for key in row}))
        writer.writeheader()
        writer.writerows(rows)
    return path


def run(catalog, path, tmp_path, source="shopify", initialize=True):
    return run_versioned_batch(catalog, path, tmp_path / "archive", source=source,
        batch_date="2026-01-05", applied_at=APPLIED, initialize_empty=initialize)


@pytest.mark.parametrize("source", ["shopify", "amazon"])
def test_real_source_to_bronze_to_state_replay(source, catalog, tmp_path):
    path = csv_file(tmp_path, source_order(source))
    first = run(catalog, path, tmp_path, source)
    second = run(catalog, path, tmp_path, source)
    assert first["committed"] and not second["committed"]
    assert first["state_snapshot_id"] == second["state_snapshot_id"]
    raw = catalog.load_table(f"bronze.{source}_order_events").scan().to_arrow()
    assert raw.num_rows == 1
    assert json.loads(raw["raw_payload"][0].as_py())["extra_column"] == "keep me"
    assert len(list((tmp_path / "archive").rglob("manifest.json"))) == 1
    assert len(list((tmp_path / "archive").rglob("receipts/*.json"))) == 2


def test_updates_deletes_and_stale_replays_keep_history(catalog, tmp_path):
    original = source_order()
    run(catalog, csv_file(tmp_path, original), tmp_path)
    changed = run(catalog, csv_file(tmp_path, source_order(version=3, status="cancelled")), tmp_path)
    assert changed["counts"]["updated"] == 1
    deletion = dict(order_id="o1", source_updated_at="2026-01-04T00:00:00Z", is_deleted="true")
    run(catalog, csv_file(tmp_path, deletion), tmp_path)
    stale = run(catalog, csv_file(tmp_path, original), tmp_path)
    assert stale["counts"]["stale"] == 1
    state = catalog.load_table(STATE_TABLE).scan().to_arrow()
    assert active_orders(state).num_rows == 0 and state.num_rows == 1
    assert catalog.load_table("bronze.shopify_order_events").scan().to_arrow().num_rows == 3


def test_two_channels_share_state_without_key_collision(catalog, tmp_path):
    run(catalog, csv_file(tmp_path, source_order()), tmp_path)
    run(catalog, csv_file(tmp_path, source_order("amazon")), tmp_path, "amazon")
    rows = catalog.load_table(STATE_TABLE).scan().to_arrow()
    assert set(rows["channel"].to_pylist()) == {"shopify", "amazon"}


@pytest.mark.parametrize("change", [{"source_updated_at": ""}, {"source_updated_at": "2026-01-02"}, {"status": "made_up"}, {"unit_price": "NaN"}, {"is_deleted": "False"}])
def test_rejected_events_remain_in_raw_bronze_without_changing_state(change, catalog, tmp_path):
    row = source_order()
    row.update(change)
    with pytest.raises(ValueError):
        run(catalog, csv_file(tmp_path, row), tmp_path)
    assert catalog.load_table("bronze.shopify_order_events").scan().to_arrow().num_rows == 1
    with pytest.raises(NoSuchTableError):
        catalog.load_table(STATE_TABLE)
    receipt = next((tmp_path / "archive").rglob("receipts/*.json"))
    assert json.loads(receipt.read_text())["status"] == "rejected"


def test_state_initialization_is_explicit(catalog, tmp_path):
    with pytest.raises(ValueError, match="explicitly initialize"):
        run(catalog, csv_file(tmp_path, source_order()), tmp_path, initialize=False)


def test_explicit_empty_input_is_recorded_without_a_state_commit(catalog, tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("order_id,source_updated_at,is_deleted\n")
    report = run(catalog, path, tmp_path)
    assert report["status"] == "complete" and not report["committed"]
    assert report["counts"]["input_rows"] == 0
    assert catalog.load_table(STATE_TABLE).scan().to_arrow().num_rows == 0


def test_empty_file_with_wrong_headers_is_not_a_valid_empty_batch(catalog, tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("something_else\n")
    with pytest.raises(ValueError, match="headers"):
        run(catalog, path, tmp_path)
    with pytest.raises(NoSuchTableError):
        catalog.load_table(STATE_TABLE)


@pytest.mark.parametrize("status", ["made_up", "cancelled"])
def test_rejected_update_leaves_existing_state_snapshot_unchanged(status, catalog, tmp_path):
    good = run(catalog, csv_file(tmp_path, source_order()), tmp_path)
    # Same source version with a changed valid status conflicts with stored state;
    # an unknown status fails the source gate before reaching the merge.
    with pytest.raises(ValueError):
        run(catalog, csv_file(tmp_path, source_order(status=status)), tmp_path)
    assert catalog.load_table(STATE_TABLE).current_snapshot().snapshot_id == good["state_snapshot_id"]


def legacy_inputs(catalog, tmp_path):
    catalog.create_namespace("silver")
    full = events(order())
    legacy = catalog.create_table("silver.orders", schema=LEGACY_SCHEMA)
    legacy.append(full.select(full.column_names[:11]).cast(schema_to_pyarrow(LEGACY_SCHEMA)))
    old, versions, candidate = (tmp_path / name for name in ["legacy.parquet", "versions.parquet", "candidate.parquet"])
    pq.write_table(legacy.scan().to_arrow(), old)
    pq.write_table(full.select(["channel", "order_id", "source_updated_at"]), versions)
    prepare_candidate(old, versions, candidate, applied_at=datetime(2026, 1, 20, tzinfo=timezone.utc))
    return legacy.current_snapshot().snapshot_id, candidate


def test_legacy_orders_cannot_be_bypassed_by_empty_initialization(catalog, tmp_path):
    snapshot, _ = legacy_inputs(catalog, tmp_path)
    with pytest.raises(ValueError, match="trusted version-map migration"):
        run(catalog, csv_file(tmp_path, source_order()), tmp_path)
    assert catalog.load_table("silver.orders").current_snapshot().snapshot_id == snapshot


def test_prepared_migration_then_source_update_preserves_legacy(catalog, tmp_path):
    legacy_snapshot, candidate = legacy_inputs(catalog, tmp_path)
    migration = install_candidate(catalog, candidate)
    assert not migration["published_to_gold"]
    assert catalog.load_table(STATE_TABLE).properties["cartco.state.status"] == "ready"
    result = run(catalog, csv_file(tmp_path, source_order(status="cancelled")), tmp_path, initialize=False)
    assert result["counts"]["updated"] == 1
    assert catalog.load_table("silver.orders").current_snapshot().snapshot_id == legacy_snapshot
    with pytest.raises(ValueError, match="already exists"):
        install_candidate(catalog, candidate)


def test_corrupted_candidate_cannot_create_state(catalog, tmp_path):
    _, candidate = legacy_inputs(catalog, tmp_path)
    candidate.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        install_candidate(catalog, candidate)
    with pytest.raises(NoSuchTableError):
        catalog.load_table(STATE_TABLE)


def test_changed_legacy_baseline_cannot_be_migrated(catalog, tmp_path):
    _, candidate = legacy_inputs(catalog, tmp_path)
    legacy = catalog.load_table("silver.orders")
    extra = events(order("added-later")).select(events(order()).column_names[:11])
    legacy.append(extra.cast(schema_to_pyarrow(LEGACY_SCHEMA)))
    with pytest.raises(ValueError, match="differ from the migration baseline"):
        install_candidate(catalog, candidate)
    with pytest.raises(NoSuchTableError):
        catalog.load_table(STATE_TABLE)


def test_concurrent_legacy_change_leaves_migration_not_ready(catalog, tmp_path, monkeypatch):
    _, candidate = legacy_inputs(catalog, tmp_path)
    original = catalog._commit_table
    interleaved = []
    def commit(request):
        result = original(request)
        if request.identifier.name == "order_state" and not interleaved:
            interleaved.append(True)
            with monkeypatch.context() as patch:
                patch.setattr(catalog, "_commit_table", original)
                legacy = catalog.load_table("silver.orders")
                extra = events(order("concurrent")).select(events(order()).column_names[:11])
                legacy.append(extra.cast(schema_to_pyarrow(LEGACY_SCHEMA)))
        return result
    monkeypatch.setattr(catalog, "_commit_table", commit)
    with pytest.raises(RuntimeError, match="Legacy changed during migration"):
        install_candidate(catalog, candidate)
    assert catalog.load_table(STATE_TABLE).properties["cartco.state.status"] == "initializing"
    with pytest.raises(ValueError, match="not ready"):
        run(catalog, csv_file(tmp_path, source_order()), tmp_path)
