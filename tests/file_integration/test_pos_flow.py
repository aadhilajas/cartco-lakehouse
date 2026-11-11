from decimal import Decimal
import json
import os
from pathlib import Path
import tempfile

import pytest

pytest.importorskip("sqlalchemy", minversion="2.0", reason="Use the isolated file-integration environment")
from pyiceberg.catalog.sql import SqlCatalog

from pipeline.gold_release import publish_gold_release, read_gold_release
from pipeline.source_contracts import daily_contract
from pipeline.versioned_ingestion import STATE_TABLE, run_versioned_batch
from pos_helpers import DAY, APPLIED, receipt, deletion, write_csv

pytestmark = pytest.mark.file_integration


@pytest.fixture
def environment(tmp_path_factory):
    if os.name == "nt":
        scratch = Path(__file__).resolve().parents[3] / "work" / "p"
        scratch.mkdir(parents=True, exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="t", dir=scratch))
    else:
        root = tmp_path_factory.mktemp("pos")
    catalog = SqlCatalog("pos", uri="sqlite:///" + (root / "catalog.db").as_posix(),
                         warehouse="file://" + (root / "w").as_posix())
    yield catalog, root
    catalog.engine.dispose()


def ingest(catalog, root, source, *rows):
    path = write_csv(root / f"{source}.csv", *rows)
    return run_versioned_batch(catalog, path, root / "archive", source=source, batch_date=DAY,
        contract=daily_contract(source, DAY), applied_at=APPLIED, initialize_empty=True)


def web_sources(catalog, root):
    receipts = []
    for source, status in [("shopify", "delivered"), ("amazon", "shipped")]:
        row = dict(order_id="web1", customer_id="online", channel=source, order_date="2026-01-01T12:00:00Z",
            product_category="Apparel", quantity="1", unit_price="1.00", currency="INR", status=status,
            source_updated_at="2026-01-02T00:00:00Z", is_deleted="false")
        row["asin" if source == "amazon" else "product_id"] = "p1"
        receipts.append(ingest(catalog, root, source, row)["receipt"])
    return receipts


def publish(catalog, root, receipts):
    return publish_gold_release(catalog, root / "gold", receipts, batch_date=DAY)


def test_three_sources_reconcile_pos_sales_customers_and_rates(environment):
    catalog, root = environment
    receipts = web_sources(catalog, root)
    pos = ingest(catalog, root, "pos", receipt(), receipt(store="S2"), receipt(transaction="R2", customer="L1"),
                 receipt(transaction="R3", status="voided"), receipt(transaction="R4", status="refunded"),
                 receipt(transaction="R5", status="open"))
    manifest = publish(catalog, root, receipts + [pos["receipt"]])
    marts, verified = read_gold_release(root / "gold")
    assert verified == manifest
    assert manifest["required_sources"] == ["amazon", "pos", "shopify"]
    assert manifest["quality"]["active_orders"] == 8 and manifest["quality"]["completed_orders"] == 5
    assert manifest["quality"]["retained_completed_sales"] == "2.60"
    channels = {r["channel"]: r for r in marts["channel_performance"].to_pylist()}
    assert channels["pos"]["total_orders"] == 6 and channels["pos"]["total_revenue"] == Decimal("0.60")
    assert channels["pos"]["cancellation_rate"] == pytest.approx(1 / 6)
    assert channels["pos"]["refund_rate"] == pytest.approx(1 / 6)
    assert len([r for r in marts["customer_360"].to_pylist() if r["channel"] == "pos"]) == 3
    batch = catalog.load_table("bronze.pos_order_events").scan().to_arrow()
    assert {json.loads(value)["store_id"] for value in batch["raw_payload"].to_pylist()} == {"S1", "S2"}


def test_pos_replay_refund_tombstone_and_stale_input_keep_all_source_state(environment):
    catalog, root = environment
    receipts = web_sources(catalog, root)
    original = [receipt(), receipt(store="S2"), receipt(transaction="R2", customer="L1")]
    first = ingest(catalog, root, "pos", *original)
    replay = ingest(catalog, root, "pos", *original)
    assert not replay["committed"] and replay["state_snapshot_id"] == first["state_snapshot_id"]
    refund = ingest(catalog, root, "pos", receipt(version=3, status="refunded"))
    assert refund["counts"]["updated"] == 1
    removed = ingest(catalog, root, "pos", deletion(store="S2", version=4))
    assert removed["counts"]["deleted"] == 1
    stale = ingest(catalog, root, "pos", *original)
    assert not stale["committed"] and stale["counts"]["stale"] == 2
    manifest = publish(catalog, root, receipts + [stale["receipt"]])
    assert manifest["quality"]["active_orders"] == 4
    assert manifest["quality"]["completed_orders"] == 3
    assert manifest["quality"]["retained_completed_sales"] == "2.20"
    state = catalog.load_table(STATE_TABLE).scan().to_arrow()
    assert state.num_rows == 5 and sum(state["is_deleted"].to_pylist()) == 1
    assert set(state["channel"].to_pylist()) == {"shopify", "amazon", "pos"}


def test_missing_pos_blocks_default_but_two_source_compatibility_is_explicit(environment):
    catalog, root = environment
    receipts = web_sources(catalog, root)
    with pytest.raises(ValueError, match="missing"):
        publish(catalog, root, receipts)
    assert not (root / "gold/CURRENT.json").exists()
    manifest = publish_gold_release(catalog, root / "gold", receipts, batch_date=DAY, expected_channels=("shopify", "amazon"))
    assert manifest["required_sources"] == ["amazon", "shopify"]


def test_explicit_empty_pos_satisfies_readiness_without_pos_state_commit(environment):
    catalog, root = environment
    receipts = web_sources(catalog, root)
    empty = ingest(catalog, root, "pos")
    assert not empty["committed"] and empty["counts"]["input_rows"] == 0
    manifest = publish(catalog, root, receipts + [empty["receipt"]])
    assert manifest["quality"]["active_orders"] == 2
    assert manifest["quality"]["retained_completed_sales"] == "2.00"


@pytest.mark.parametrize("change", [{"order_id": "forged"}, {"line_count": "2"}, {"order_total": "0.21"}, {"status": "refunded"}])
def test_invalid_pos_update_retains_raw_and_does_not_change_release(environment, change):
    catalog, root = environment
    receipts = web_sources(catalog, root)
    first = ingest(catalog, root, "pos", receipt())
    publish(catalog, root, receipts + [first["receipt"]])
    before = (root / "gold/CURRENT.json").read_bytes()
    with pytest.raises(ValueError):
        ingest(catalog, root, "pos", {**receipt(), **change})
    assert catalog.load_table(STATE_TABLE).current_snapshot().snapshot_id == first["state_snapshot_id"]
    assert catalog.load_table("bronze.pos_order_events").scan().to_arrow().num_rows == 2
    assert (root / "gold/CURRENT.json").read_bytes() == before
