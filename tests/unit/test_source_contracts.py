from datetime import date

import pytest

from pipeline.source_contracts import business_date, check_source_version, daily_contract, timestamp, validate_contract
from pipeline.raw_batches import archive_csv


def contract(**changes):
    return {**daily_contract("shopify", "2026-01-05"), **changes}


def test_business_midnight_converts_utc_before_extracting_date():
    assert business_date("2026-01-04T18:30:00Z", contract()) == date(2026, 1, 5)
    assert business_date("2026-01-04T18:29:59Z", contract()) == date(2026, 1, 4)
    assert business_date("2026-01-04T18:30:00", contract()) == date(2026, 1, 5)


@pytest.mark.parametrize("value", ["2026-03-08T02:30:00", "2026-11-01T01:30:00"])
def test_dst_gap_and_fold_require_explicit_offsets(value):
    with pytest.raises(ValueError, match="Ambiguous or nonexistent"):
        business_date(value, contract(source_timezone="America/New_York"))
    assert isinstance(business_date(value + "-04:00", contract(source_timezone="America/New_York")), date)


def test_incremental_is_start_inclusive_end_exclusive():
    delivery = contract(delivery_mode="incremental")
    check_source_version(timestamp(delivery["extraction_start"]), delivery)
    for value in ["2026-01-04T18:29:59Z", delivery["extraction_end"]]:
        with pytest.raises(ValueError, match="outside"):
            check_source_version(timestamp(value), delivery)


def test_snapshot_accepts_older_versions_but_not_future_cutoff():
    delivery = contract()
    check_source_version(timestamp("2020-01-01T00:00:00Z"), delivery)
    with pytest.raises(ValueError, match="outside"):
        check_source_version(timestamp(delivery["extraction_end"]), delivery)


@pytest.mark.parametrize("changes", [
    {"source_schema_version": "shopify.orders.v999"}, {"contract_version": True},
    {"business_timezone": "Bogus/Timezone"}, {"source_timezone": None},
    {"extraction_start": "2026-01-05"}, {"extraction_end": "2026-01-05T00:00:00"},
    {"extraction_end": "2020-01-01T00:00:00Z"}, {"extra": "unrecognized"},
    {"delivery_mode": "replace"}, {"extraction_start": "2026-01-01T00:00:00Z"}])
def test_unsupported_or_inconsistent_contract_rejected(changes):
    with pytest.raises(ValueError):
        validate_contract(contract(**changes), source="shopify", batch_date="2026-01-05")


def test_identical_csv_with_changed_delivery_metadata_is_a_distinct_batch(tmp_path):
    path = tmp_path / "source.csv"
    path.write_text("order_id\no1\n", encoding="utf-8")
    one = archive_csv(path, tmp_path / "archive", source="shopify", batch_date="2026-01-05", contract=contract())
    same = archive_csv(path, tmp_path / "archive", source="shopify", batch_date="2026-01-05", contract=contract())
    other = archive_csv(path, tmp_path / "archive", source="shopify", batch_date="2026-01-05", contract=contract(delivery_mode="incremental"))
    assert one.batch_id == same.batch_id != other.batch_id


@pytest.mark.parametrize("source", ["shopify", "amazon", "pos"])
def test_source_generator_emits_matching_explicit_sidecar(source, tmp_path, monkeypatch):
    import importlib
    import json
    module = importlib.import_module(f"ingestion.generate_synthetic_{source}_data")
    output = tmp_path / f"{source}.csv"
    monkeypatch.setattr(module, "N_ROWS", 3)
    monkeypatch.setattr(module, "OUTPUT_PATH", str(output))
    monkeypatch.setenv("CARTCO_BATCH_DATE", "2026-01-05")
    monkeypatch.setenv("CARTCO_SOURCE_VERSIONS", "1")
    module.main()
    delivery = json.loads(output.with_suffix(".csv.contract.json").read_text(encoding="utf-8"))
    assert delivery == daily_contract(source, "2026-01-05")
    assert output.exists()
