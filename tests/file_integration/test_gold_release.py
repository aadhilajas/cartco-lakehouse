import csv
from datetime import datetime
from decimal import Decimal
import os
from pathlib import Path
import tempfile

import pytest

pytest.importorskip("sqlalchemy", minversion="2.0", reason="Use the isolated catalog environment")
from pyiceberg.catalog.sql import SqlCatalog

from pipeline.gold_release import publish_gold_release, read_gold_release
from pipeline.versioned_ingestion import run_versioned_batch
from transforms.gold import daily_revenue

pytestmark = pytest.mark.file_integration
DAY = "2026-01-05"


@pytest.fixture
def environment(tmp_path_factory):
    if os.name == "nt":
        scratch = Path(__file__).resolve().parents[3] / "work" / "g"
        scratch.mkdir(parents=True, exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="t", dir=scratch))
    else:
        root = tmp_path_factory.mktemp("gold")
    catalog = SqlCatalog("gold", uri="sqlite:///" + (root / "catalog.db").as_posix(), warehouse="file://" + (root / "w").as_posix())
    yield catalog, root
    catalog.engine.dispose()


def source_rows(source, *, updated=False):
    rows = []
    for index, status in enumerate((["delivered", "cancelled"] if source == "shopify" else ["shipped", "pending"])):
        row = dict(order_id=f"o{index}", customer_id="c1", channel=source, order_date="2026-01-01T08:00:00",
                   product_category="Apparel", quantity="1", unit_price="0.10" if index == 0 else "99.00",
                   status="refund_initiated" if updated and index == 0 else status,
                   source_updated_at="2026-01-03T00:00:00Z" if updated else "2026-01-02T00:00:00Z", is_deleted="false")
        row["asin" if source == "amazon" else "product_id"] = "p1"
        rows.append(row)
    return rows


def ingest(catalog, root, source, *, updated=False, empty=False):
    path = root / f"{source}.csv"
    rows = source_rows(source, updated=updated)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        if not empty:
            writer.writerows(rows)
    return run_versioned_batch(catalog, path, root / "archive", source=source, batch_date=DAY,
                              applied_at=datetime(2026, 1, 5), initialize_empty=True)["receipt"]


def seed(catalog, root):
    return [ingest(catalog, root, source) for source in ("shopify", "amazon")]


def publish(catalog, root, receipts):
    return publish_gold_release(catalog, root / "gold", receipts, batch_date=DAY)


def test_publish_reads_complete_decimal_marts_from_one_snapshot(environment):
    catalog, root = environment
    manifest = publish(catalog, root, seed(catalog, root))
    tables, read_manifest = read_gold_release(root / "gold")
    assert manifest == read_manifest
    assert manifest["quality"]["retained_completed_sales"] == "0.20"
    assert manifest["quality"]["completed_orders"] == 2
    assert sum(tables["daily_revenue"]["total_revenue"].to_pylist()) == Decimal("0.20")
    assert "category_unit_sales" in tables


def test_same_snapshot_and_receipts_are_idempotent(environment):
    catalog, root = environment
    receipts = seed(catalog, root)
    one = publish(catalog, root, receipts)
    before = (root / "gold/CURRENT.json").read_bytes()
    two = publish(catalog, root, receipts)
    assert one == two and before == (root / "gold/CURRENT.json").read_bytes()
    assert len(list((root / "gold/releases").iterdir())) == 1


def test_missing_source_retains_previous_release(environment):
    catalog, root = environment
    receipts = seed(catalog, root)
    publish(catalog, root, receipts)
    before = (root / "gold/CURRENT.json").read_bytes()
    with pytest.raises(ValueError, match="missing"):
        publish(catalog, root, receipts[:1])
    assert (root / "gold/CURRENT.json").read_bytes() == before


def test_corrupt_metric_blocks_publication_before_pointer_change(environment, monkeypatch):
    catalog, root = environment
    receipts = seed(catalog, root)
    publish(catalog, root, receipts)
    before = (root / "gold/CURRENT.json").read_bytes()
    original = daily_revenue.transform
    def corrupt(orders):
        import pyarrow as pa
        table = original(orders)
        return table.set_column(table.schema.get_field_index("total_revenue"), "total_revenue", pa.array([999.0] * table.num_rows))
    monkeypatch.setattr(daily_revenue, "transform", corrupt)
    with pytest.raises(ValueError, match="reconcile"):
        publish(catalog, root, receipts)
    assert (root / "gold/CURRENT.json").read_bytes() == before


def test_stale_source_receipt_cannot_hide_behind_another_sources_fresh_receipt(environment):
    catalog, root = environment
    receipts = seed(catalog, root)
    publish(catalog, root, receipts)
    before = (root / "gold/CURRENT.json").read_bytes()
    ingest(catalog, root, "amazon", updated=True)
    fresh_shopify = ingest(catalog, root, "shopify")
    with pytest.raises(ValueError, match="predates"):
        publish(catalog, root, [fresh_shopify, receipts[1]])
    assert (root / "gold/CURRENT.json").read_bytes() == before


def test_updated_receipts_publish_a_new_consistent_release(environment):
    catalog, root = environment
    receipts = seed(catalog, root)
    first = publish(catalog, root, receipts)
    amazon = ingest(catalog, root, "amazon", updated=True)
    second = publish(catalog, root, [receipts[0], amazon])
    assert first["release_id"] != second["release_id"]
    assert second["quality"]["retained_completed_sales"] == "0.10"


def test_input_drift_during_artifact_write_keeps_previous_pointer(environment, monkeypatch):
    import pipeline.gold_release as module
    catalog, root = environment
    receipts = seed(catalog, root)
    publish(catalog, root, receipts)
    before = (root / "gold/CURRENT.json").read_bytes()
    original = module._write_once
    triggered = []
    def interleaved(path, content):
        original(path, content)
        if path.name == "daily_revenue.parquet" and not triggered:
            triggered.append(True)
            ingest(catalog, root, "amazon", updated=True)
    monkeypatch.setattr(module, "_write_once", interleaved)
    with pytest.raises(ValueError, match="State changed"):
        publish(catalog, root, receipts)
    assert (root / "gold/CURRENT.json").read_bytes() == before


def test_reader_detects_artifact_tampering(environment):
    catalog, root = environment
    manifest = publish(catalog, root, seed(catalog, root))
    path = root / "gold/releases" / manifest["release_id"] / "daily_revenue.parquet"
    path.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="artifact checksum"):
        read_gold_release(root / "gold")


def test_both_explicit_empty_sources_can_publish_an_empty_release(environment):
    catalog, root = environment
    receipts = [ingest(catalog, root, source, empty=True) for source in ("shopify", "amazon")]
    manifest = publish(catalog, root, receipts)
    assert manifest["quality"]["active_orders"] == 0
    tables, _ = read_gold_release(root / "gold")
    assert all(table.num_rows == 0 for table in tables.values())


def test_publication_lock_is_not_stolen(environment):
    catalog, root = environment
    receipts = seed(catalog, root)
    (root / "gold").mkdir()
    (root / "gold/.publication.lock").write_text("other publisher")
    with pytest.raises(FileExistsError):
        publish(catalog, root, receipts)
    assert (root / "gold/.publication.lock").read_text() == "other publisher"


def test_archived_source_tampering_blocks_new_publication(environment):
    catalog, root = environment
    receipts = seed(catalog, root)
    publish(catalog, root, receipts)
    before = (root / "gold/CURRENT.json").read_bytes()
    raw = Path(receipts[0]).parent.parent / "source.csv"
    raw.write_bytes(b"changed after completion")
    with pytest.raises(ValueError, match="Archived source"):
        publish(catalog, root, receipts)
    assert (root / "gold/CURRENT.json").read_bytes() == before


def test_receipt_from_another_catalog_is_rejected(environment):
    catalog, root = environment
    receipts = seed(catalog, root)
    other_root = root / "other"
    other_root.mkdir()
    other = SqlCatalog("gold", uri="sqlite:///" + (other_root / "catalog.db").as_posix(), warehouse="file://" + (other_root / "w").as_posix())
    try:
        foreign = ingest(other, other_root, "shopify")
        with pytest.raises(ValueError, match="another state table"):
            publish(catalog, root, [foreign, receipts[1]])
    finally:
        other.engine.dispose()
