"""Real Iceberg metadata/Parquet tests using an isolated SQLite catalog.

Install requirements/file-integration.txt in its own environment. Airflow's
SQLAlchemy 1.4 environment must not be upgraded to run these optional tests.
"""
from datetime import date

import pytest

pytest.importorskip("sqlalchemy", minversion="2.0", reason="Use the isolated file-integration environment")
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

from order_state_helpers import APPLIED, events, order, tombstone
from pipeline.order_state import STATE_SCHEMA, VersionConflict, active_orders, write_order_state

pytestmark = pytest.mark.file_integration
TABLE = "silver.order_state"


@pytest.fixture
def catalog(tmp_path):
    uri = "sqlite:///" + (tmp_path / "catalog.db").as_posix()
    # PyIceberg 0.7.1 concatenates file URI netloc/path without URI decoding.
    # Keep the Windows drive in netloc and the literal local path unescaped.
    warehouse = "file://" + (tmp_path / "warehouse").as_posix()
    catalog = SqlCatalog("test", uri=uri, warehouse=warehouse)
    catalog.create_namespace("silver")
    catalog.create_table(TABLE, schema=STATE_SCHEMA, partition_spec=PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="channel")))
    yield catalog
    catalog.engine.dispose()


def write(catalog, incoming, channel="shopify"):
    return write_order_state(catalog, TABLE, incoming, channel=channel, applied_at=APPLIED)


def state(catalog):
    return catalog.load_table(TABLE).scan().to_arrow()


def test_real_files_replay_does_not_commit_another_snapshot(catalog, tmp_path):
    first = write(catalog, events(order()))
    replay = write(catalog, events(order()))
    assert first.committed and not replay.committed
    assert first.snapshot_id == replay.snapshot_id
    assert list(tmp_path.rglob("*.parquet"))
    assert state(catalog).num_rows == 1


def test_sparse_range_preserves_unmentioned_orders_on_disk(catalog):
    write(catalog, events(order("middle", order_date=date(2026, 1, 2))))
    write(catalog, events(order("left"), order("right", order_date=date(2026, 1, 3))))
    assert set(state(catalog)["order_id"].to_pylist()) == {"left", "middle", "right"}


def test_channel_overwrite_preserves_other_channel_and_same_order_id(catalog):
    write(catalog, events(order(channel="amazon")), "amazon")
    write(catalog, events(order()))
    write(catalog, events(order(version=2, order_status="cancelled")))
    rows = {r["channel"]: r for r in state(catalog).to_pylist()}
    assert rows["amazon"]["order_status"] == "completed"
    assert rows["shopify"]["order_status"] == "cancelled"


def test_tombstone_blocks_stale_replay_after_catalog_reopen(catalog):
    write(catalog, events(order()))
    write(catalog, tombstone(version=3))
    reopened = SqlCatalog("test", **catalog.properties)
    try:
        result = write(reopened, events(order(version=1)))
        assert result.counts["stale"] == 1 and not result.committed
        assert active_orders(state(reopened)).num_rows == 0
    finally:
        reopened.engine.dispose()


def test_conflict_does_not_commit_partial_changes(catalog):
    before = write(catalog, events(order())).snapshot_id
    with pytest.raises(VersionConflict):
        write(catalog, events(order(order_status="refunded"), order("new")))
    assert catalog.load_table(TABLE).current_snapshot().snapshot_id == before
    assert state(catalog).num_rows == 1


def test_actual_commit_conflict_reloads_and_preserves_interleaved_write(catalog, monkeypatch):
    write(catalog, events(order()))
    original = catalog._commit_table
    calls = []
    def interleaved(request):
        if not calls:
            calls.append("interleaved")
            with monkeypatch.context() as patch:
                patch.setattr(catalog, "_commit_table", original)
                write(catalog, events(order("concurrent")))
        return original(request)
    monkeypatch.setattr(catalog, "_commit_table", interleaved)
    write(catalog, events(order(version=2, order_status="cancelled")))
    rows = {r["order_id"]: r for r in state(catalog).to_pylist()}
    assert set(rows) == {"o1", "concurrent"}
    assert rows["o1"]["order_status"] == "cancelled"


def test_ambiguous_transport_failure_is_not_blindly_retried(catalog, monkeypatch):
    write(catalog, events(order()))
    calls = []
    def unavailable(request):
        calls.append(request)
        raise OSError("transport unavailable")
    monkeypatch.setattr(catalog, "_commit_table", unavailable)
    with pytest.raises(OSError, match="transport unavailable"):
        write(catalog, events(order(version=2)))
    assert len(calls) == 1
    assert state(catalog)["source_updated_at"].to_pylist() == events(order())["source_updated_at"].to_pylist()


def test_commit_conflict_retries_are_bounded(catalog, monkeypatch):
    write(catalog, events(order()))
    calls = []
    def conflict(request):
        calls.append(request)
        raise CommitFailedException("simulated contention")
    monkeypatch.setattr(catalog, "_commit_table", conflict)
    with pytest.raises(CommitFailedException):
        write(catalog, events(order(version=2)))
    assert len(calls) == 3
    assert state(catalog).num_rows == 1


def test_lost_response_after_successful_commit_can_be_replayed_safely(catalog, monkeypatch):
    write(catalog, events(order()))
    original = catalog._commit_table
    def lost_response(request):
        original(request)
        raise OSError("response lost after commit")
    with monkeypatch.context() as patch:
        patch.setattr(catalog, "_commit_table", lost_response)
        with pytest.raises(OSError, match="response lost"):
            write(catalog, events(order(version=2, order_status="refunded")))
    snapshot_id = catalog.load_table(TABLE).current_snapshot().snapshot_id
    replay = write(catalog, events(order(version=2, order_status="refunded")))
    assert not replay.committed
    assert replay.snapshot_id == snapshot_id
    assert state(catalog)["order_status"].to_pylist() == ["refunded"]


def test_wrong_channel_cannot_write_any_data(catalog):
    with pytest.raises(ValueError, match="requested channel"):
        write(catalog, events(order(channel="amazon")))
    assert state(catalog).num_rows == 0


def test_legacy_schema_is_not_migrated_implicitly(catalog):
    from transforms.silver_shopify_orders import TABLE_SCHEMA
    catalog.create_table("silver.legacy", schema=TABLE_SCHEMA)
    with pytest.raises(ValueError, match="explicit migration"):
        write_order_state(catalog, "silver.legacy", events(order()), channel="shopify", applied_at=APPLIED)
    assert catalog.load_table("silver.legacy").current_snapshot() is None
