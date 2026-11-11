from decimal import Decimal

import pandas as pd
import pytest

from ingestion import generate_synthetic_pos_data as generator
from pipeline.pos import pos_order_id
from pipeline.raw_batches import archive_csv
from pipeline.source_contracts import daily_contract
from pipeline.versioned_ingestion import canonical_events
from pos_helpers import DAY, receipt, deletion, write_csv


def canonical(tmp_path, *rows):
    path = write_csv(tmp_path / "pos.csv", *rows)
    batch = archive_csv(path, tmp_path / "archive", source="pos", batch_date=DAY, contract=daily_contract("pos", DAY))
    return canonical_events(batch)


def test_pos_ids_scope_reused_numbers_without_delimiter_collisions():
    keys = [pos_order_id("S1", "T1", "R1"), pos_order_id("S2", "T1", "R1"),
            pos_order_id("S1", "T2", "R1"), pos_order_id("a/b", "c", "d"), pos_order_id("a", "b/c", "d")]
    assert len(set(keys)) == 5
    assert keys[0] == pos_order_id("S1", "T1", "R1")


@pytest.mark.parametrize("status,expected", [("paid", "completed"), ("voided", "cancelled"), ("refunded", "refunded"), ("open", "pending")])
def test_pos_status_money_and_business_date_contract(status, expected, tmp_path):
    row = canonical(tmp_path, receipt(status=status)).to_pylist()[0]
    assert row["order_status"] == expected
    assert row["order_total"] == Decimal("0.20") and row["currency"] == "INR"
    assert str(row["order_date"]) == "2026-01-02"
    assert row["product_id"] == "SKU1"


def test_walk_ins_are_separate_from_each_other_and_loyalty_ids(tmp_path):
    rows = canonical(tmp_path, receipt(), receipt(transaction="R2"), receipt(transaction="R3", customer="c1"),
                     receipt(transaction="R4", customer="c1")).to_pylist()
    assert rows[0]["customer_id"] != rows[1]["customer_id"]
    assert rows[2]["customer_id"] == rows[3]["customer_id"] == "pos:customer:c1"
    assert rows[0]["customer_id"].startswith("pos:guest:")


@pytest.mark.parametrize("change", [{"store_id": ""}, {"terminal_id": " T1"}, {"order_id": "forged"},
    {"line_count": "2"}, {"sku": ""}, {"status": "partially_refunded"}, {"tax_amount": "0.01"},
    {"discount_amount": "0.01"}, {"order_total": "0.21"}, {"unit_price": "0.101"}, {"customer_id": " "}])
def test_invalid_pos_receipt_is_rejected(change, tmp_path):
    with pytest.raises(ValueError):
        canonical(tmp_path, {**receipt(), **change})


def test_mixed_live_and_native_key_tombstone_accepts_blank_business_payload(tmp_path):
    rows = canonical(tmp_path, receipt(), deletion(transaction="R2"))
    assert rows["is_deleted"].to_pylist() == [False, True]
    assert rows["unit_price"][1].as_py() is None


def test_missing_pos_key_dimensions_are_rejected_even_for_empty_deliveries(tmp_path):
    path = tmp_path / "pos.csv"
    path.write_text("order_id,source_updated_at,is_deleted\n", encoding="utf-8")
    batch = archive_csv(path, tmp_path / "archive", source="pos", batch_date=DAY, contract=daily_contract("pos", DAY))
    with pytest.raises(ValueError):
        canonical_events(batch)


def test_generator_is_deterministic_and_has_unique_scoped_receipts(monkeypatch, tmp_path):
    monkeypatch.setattr(generator, "N_ROWS", 200)
    one, two = generator.generate(DAY), generator.generate(DAY)
    pd.testing.assert_frame_equal(one, two)
    assert one["order_id"].is_unique
    assert one["store_id"].nunique() == 20 and one["terminal_id"].nunique() == 4
    path = tmp_path / "pos.csv"
    one.to_csv(path, index=False)
    batch = archive_csv(path, tmp_path / "archive", source="pos", batch_date=DAY, contract=daily_contract("pos", DAY))
    assert canonical_events(batch).num_rows == 200
