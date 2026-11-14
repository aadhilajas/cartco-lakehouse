from decimal import Decimal

import pyarrow as pa
import pytest

from inventory_helpers import inputs, manifest, orders, write_bundle
from pipeline.inventory import build_turnover, validate_turnover, read_bundle
from scripts.generate_inventory_bundle import generate_bundle


def test_shared_pool_turnover_uses_one_daily_inventory_denominator():
    data, contract = inputs(), manifest()
    table, report = build_turnover(orders(), contract, data)
    row = table.to_pylist()[0]
    assert row["cogs"] == Decimal("6.00")
    assert row["inventory_value_days"] == Decimal("60.00")
    assert row["average_daily_inventory_value"] == Decimal("30.0000000000")
    assert row["turnover"] == Decimal("0.200000000000")
    assert row["observed_days"] == row["expected_days"] == 2
    assert report["allocated_orders"] == 2 and report["pools"] == 1
    validate_turnover(table, data, contract)


def test_missing_inventory_day_is_null_with_explicit_coverage_status():
    data = inputs()
    data["snapshots.csv"].pop()
    table, report = build_turnover(orders(), manifest(), data)
    row = table.to_pylist()[0]
    assert row["quality_status"] == "incomplete_daily_coverage"
    assert row["average_daily_inventory_value"] is row["turnover"] is None
    assert row["cogs"] == Decimal("6.00") and report["incomplete_pools"] == 1
    validate_turnover(table, data, manifest())


def test_zero_denominator_is_null_and_not_infinity():
    data = inputs()
    for row in data["snapshots.csv"]:
        row.update(on_hand="0", inventory_value="0.00")
    table, report = build_turnover(orders(), manifest(), data)
    row = table.to_pylist()[0]
    assert row["quality_status"] == "zero_inventory_value" and row["turnover"] is None
    assert row["average_daily_inventory_value"] == 0 and report["zero_inventory_pools"] == 1


def test_unsold_declared_pool_is_present_with_zero_turnover():
    data = inputs()
    data["scope.csv"].append(dict(sku="UNSOLD", location_id="L1", unit_cost="3.00"))
    data["snapshots.csv"] += [dict(sku="UNSOLD", location_id="L1", snapshot_date=f"2026-01-0{day}", on_hand="2", inventory_value="6.00") for day in (1, 2)]
    table, _ = build_turnover(orders(), manifest(), data)
    row = next(row for row in table.to_pylist() if row["sku"] == "UNSOLD")
    assert row["cogs"] == row["turnover"] == 0 and row["quality_status"] == "complete"


@pytest.mark.parametrize("filename,field,value", [
    ("allocations.csv", "quantity", "3"), ("allocations.csv", "unit_cost", "3.00"),
    ("allocations.csv", "cogs", "4.01"), ("allocations.csv", "cogs_date", "2026-01-02"),
    ("allocations.csv", "location_id", "OTHER"), ("allocations.csv", "order_id", "unknown"),
    ("products.csv", "sku", "unknown"), ("snapshots.csv", "inventory_value", "20.01"),
    ("snapshots.csv", "on_hand", "-1"), ("snapshots.csv", "snapshot_date", "2026-01-03"),
    ("scope.csv", "unit_cost", "2.001"), ("scope.csv", "unit_cost", "NaN")])
def test_inconsistent_or_invalid_inventory_inputs_fail(filename, field, value):
    data = inputs()
    data[filename][0][field] = value
    with pytest.raises(ValueError):
        build_turnover(orders(), manifest(), data)


@pytest.mark.parametrize("filename", ["scope.csv", "products.csv", "allocations.csv", "snapshots.csv"])
def test_duplicate_input_keys_are_rejected(filename):
    data = inputs()
    data[filename].append(dict(data[filename][0]))
    with pytest.raises(ValueError, match="Duplicate"):
        build_turnover(orders(), manifest(), data)


def test_missing_cogs_allocation_cannot_silently_reduce_numerator():
    data = inputs()
    data["allocations.csv"].pop()
    with pytest.raises(ValueError, match="exactly cover"):
        build_turnover(orders(), manifest(), data)


def test_ratio_uses_unrounded_average():
    data = inputs()
    data["scope.csv"][0]["unit_cost"] = "0.01"
    for row in data["allocations.csv"]:
        row.update(unit_cost="0.01", cogs=f"{Decimal(row['quantity']) / 100:.2f}")
    data["snapshots.csv"] = [dict(sku="S1", location_id="L1", snapshot_date=f"2026-01-0{day}",
        on_hand="1" if day == 1 else "0", inventory_value="0.01" if day == 1 else "0.00") for day in (1, 2, 3)]
    table, _ = build_turnover(orders(), manifest(period_end="2026-01-04"), data)
    assert table["turnover"][0].as_py() == Decimal("9.000000000000")
    assert table["average_daily_inventory_value"][0].as_py() == Decimal("0.0033333333")


def test_independent_gate_rejects_corrupted_ratio():
    data, contract = inputs(), manifest()
    table, _ = build_turnover(orders(), contract, data)
    table = table.set_column(table.schema.get_field_index("turnover"), "turnover", pa.array([Decimal("99.0")], pa.decimal128(38, 12)))
    with pytest.raises(ValueError, match="does not reconcile"):
        validate_turnover(table, data, contract)


@pytest.mark.parametrize("change", [{"state_table_uuid": "foreign"}, {"state_snapshot_id": 2}, {"currency": "USD"},
    {"business_timezone": "Asia/Kolkata"}, {"cost_basis": "FIFO"}, {"period_end": "2027-01-01"}, {"is_synthetic": "false"}, {"period_start": None}, {"period_end": None}])
def test_bundle_must_match_pinned_state_and_supported_contract(tmp_path, change):
    path = write_bundle(tmp_path / "bundle", manifest(**change), inputs())
    with pytest.raises(ValueError):
        read_bundle(path, state_uuid="test-uuid", snapshot_id=1, business_timezone="UTC", batch_date="2026-01-05")


def test_tampered_source_file_is_rejected(tmp_path):
    path = write_bundle(tmp_path / "bundle", manifest(), inputs())
    (path / "scope.csv").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        read_bundle(path, state_uuid="test-uuid", snapshot_id=1, business_timezone="UTC", batch_date="2026-01-05")


def test_synthetic_bundle_is_deterministic_and_refuses_existing_output(tmp_path):
    args = dict(state_uuid="test-uuid", snapshot_id=1, business_timezone="UTC", period_start="2026-01-01", period_end="2026-01-03")
    first = generate_bundle(orders(), tmp_path / "one", **args)
    assert generate_bundle(orders(), tmp_path / "two", **args) == first
    assert first["is_synthetic"] is True
    contract, data, _ = read_bundle(tmp_path / "one", state_uuid="test-uuid", snapshot_id=1, business_timezone="UTC", batch_date="2026-01-05")
    table, _ = build_turnover(orders(), contract, data)
    assert table.num_rows == 3
    with pytest.raises(FileExistsError):
        generate_bundle(orders(), tmp_path / "one", **args)
