"""Regression checks for batch boundaries and write verification, without Docker.

The small catalog double captures an immutable handle snapshot to reproduce stale
metadata reads. It does not claim to test Iceberg commit/overwrite semantics.
"""
from datetime import date, datetime, timedelta, timezone
import importlib

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from helpers import make_bronze_table
from pipeline.batch import resolve_batch_date

DAY = date(2026, 1, 5)
SOURCES = [("shopify", "silver_shopify_orders"), ("amazon", "amazon_silver")]


@pytest.mark.parametrize("value", ["", "2026-1-05", "2026-02-30", "2026-01-05T00:00:00", datetime(2026, 1, 5), 42])
def test_invalid_batch_dates_fail_instead_of_falling_back_to_today(value):
    with pytest.raises(ValueError):
        resolve_batch_date(value, environ={})


def test_explicit_date_overrides_environment_and_clock():
    assert resolve_batch_date(DAY, environ={"CARTCO_BATCH_DATE": "invalid"}) == DAY


def test_environment_pins_date_even_after_midnight():
    assert resolve_batch_date(environ={"CARTCO_BATCH_DATE": "2026-01-05"}, now=datetime(2026, 1, 6, tzinfo=timezone.utc)) == DAY


def test_standalone_fallback_uses_utc_instead_of_local_date():
    now = datetime(2026, 1, 4, 23, 30, tzinfo=timezone(timedelta(hours=-5)))
    assert resolve_batch_date(environ={}, now=now) == DAY


def test_naive_batch_clock_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_batch_date(environ={}, now=datetime(2026, 1, 5))


@pytest.mark.parametrize("source", ["shopify", "amazon"])
def test_fixed_date_generation_repeats_every_column_and_has_unique_order_ids(source, monkeypatch):
    module = importlib.import_module(f"ingestion.generate_synthetic_{source}_data")
    for key, count in [("N_ROWS", 25), ("N_CUSTOMERS", 8), ("N_PRODUCTS", 6)]:
        monkeypatch.setattr(module, key, count)
    first = module.generate("2026-01-05")
    second = module.generate(DAY)
    pd.testing.assert_frame_equal(first, second)
    assert first.order_id.is_unique
    assert first.order_date.min() >= pd.Timestamp("2024-01-05")
    assert first.order_date.max() < pd.Timestamp("2026-01-05")
    assert set(first.order_id).isdisjoint(module.generate("2026-01-06").order_id)


class Scan:
    def __init__(self, rows):
        self.rows = rows

    def to_arrow(self):
        return self.rows


class Handle:
    def __init__(self, catalog, rows, *, writable=False):
        self.catalog = catalog
        self.rows = rows
        self.writable = writable

    def scan(self, row_filter=None):
        rows = self.rows
        if row_filter is not None and "ingestion_date" in rows.column_names:
            day = date.fromisoformat(row_filter.literal.value)
            rows = rows.filter(pc.equal(rows["ingestion_date"], pa.scalar(day, pa.date32())))
        return Scan(rows)

    def overwrite(self, df, overwrite_filter):
        assert self.writable
        self.catalog.writes.append((df, overwrite_filter))
        self.catalog.rows = df.slice(0, 0) if self.catalog.corrupt_readback else df


class Catalog:
    def __init__(self, bronze, silver):
        self.bronze = bronze
        self.rows = silver
        self.writes = []
        self.corrupt_readback = False

    def load_table(self, identifier):
        if identifier.startswith("bronze."):
            return Handle(self, self.bronze)
        return Handle(self, self.rows, writable=True)


def bronze_rows(source, *, customer_id="c1", times=None):
    times = times or [datetime(2026, 1, 1, 8)]
    rows = make_bronze_table(
        order_id=["o1"] * len(times), customer_id=[customer_id] * len(times),
        order_date=times, quantity=list(range(1, len(times) + 1)),
        unit_price=[10.0] * len(times),
        status=["delivered" if source == "shopify" else "shipped"] * len(times),
        channel=[source] * len(times),
    )
    if source == "amazon":
        rows = rows.rename_columns(["asin" if c == "product_id" else c for c in rows.column_names])
    return rows.append_column("ingestion_date", pa.array([DAY] * len(times), pa.date32()))


def record_lineage(monkeypatch, module):
    events = []
    for name in ["start", "complete", "fail"]:
        monkeypatch.setattr(module.lineage, f"emit_{name}", lambda *a, event=name, **kw: events.append(event))
    return events


@pytest.mark.parametrize("source", ["shopify", "amazon"])
def test_quality_gate_reads_only_requested_batch(source, monkeypatch):
    module = importlib.import_module(f"transforms.ge_bronze_{source}_checkpoint")
    rows = pa.table({"ingestion_date": pa.array([DAY, date(2026, 1, 4)], pa.date32()), "order_id": ["requested", "historical"]})
    catalog = Catalog(rows, None)
    monkeypatch.setattr(module, "get_catalog", lambda: catalog)
    selected = module.load_bronze_as_dataframe("2026-01-05")
    assert selected.order_id.tolist() == ["requested"]


@pytest.mark.parametrize("source", ["shopify", "amazon"])
def test_quality_gate_emits_fail_when_batch_read_fails(source, monkeypatch):
    module = importlib.import_module(f"transforms.ge_bronze_{source}_checkpoint")
    def broken_read(day):
        assert day == DAY
        raise ConnectionError("catalog unavailable")
    monkeypatch.setattr(module, "load_bronze_as_dataframe", broken_read)
    events = record_lineage(monkeypatch, module)
    with pytest.raises(ConnectionError, match="catalog unavailable"):
        module.run("2026-01-05")
    assert events == ["start", "fail"]


@pytest.mark.parametrize("source,silver", SOURCES)
def test_dedup_uses_timestamp_before_truncating_to_business_date(source, silver):
    module = importlib.import_module(f"transforms.{silver}")
    rows = bronze_rows(source, times=[datetime(2026, 1, 1, 8), datetime(2026, 1, 1, 16)])
    result = module.transform(rows, DAY)
    assert result.num_rows == 1
    assert result["quantity"].to_pylist() == [2]
    assert result["order_date"].to_pylist() == [date(2026, 1, 1)]


def prepare_silver(source, silver, monkeypatch, *, rows=None):
    module = importlib.import_module(f"transforms.{silver}")
    rows = bronze_rows(source) if rows is None else rows
    empty = module.transform(rows.slice(0, 0), DAY)
    catalog = Catalog(rows, empty)
    monkeypatch.setattr(module, "get_catalog", lambda: catalog)
    monkeypatch.setattr(module, "ensure_table", lambda c: c.load_table("silver.orders"))
    return module, catalog, record_lineage(monkeypatch, module)


@pytest.mark.parametrize("source,silver", SOURCES)
def test_silver_historical_batch_verifies_newly_loaded_snapshot(source, silver, monkeypatch):
    module, catalog, events = prepare_silver(source, silver, monkeypatch)
    # Today's partition is absent: success proves the requested date was used.
    module.run("2026-01-05")
    assert len(catalog.writes) == 1
    assert catalog.rows.num_rows == 1
    assert events == ["start", "complete"]


@pytest.mark.parametrize("source,silver", SOURCES)
def test_silver_readback_mismatch_fails_instead_of_reporting_complete(source, silver, monkeypatch):
    module, catalog, events = prepare_silver(source, silver, monkeypatch)
    catalog.corrupt_readback = True
    with pytest.raises(RuntimeError, match="Silver verification failed"):
        module.run(DAY)
    assert events == ["start", "fail"]


@pytest.mark.parametrize("source,silver", SOURCES)
def test_missing_batch_cannot_silently_succeed(source, silver, monkeypatch):
    module, catalog, events = prepare_silver(source, silver, monkeypatch)
    with pytest.raises(RuntimeError, match="No Bronze rows found"):
        module.run("2026-01-06")
    assert not catalog.writes
    assert events == ["start", "fail"]


@pytest.mark.parametrize("source,silver", SOURCES)
def test_all_rejected_rows_cannot_attempt_an_empty_overwrite(source, silver, monkeypatch):
    rows = bronze_rows(source, customer_id=None)
    module, catalog, events = prepare_silver(source, silver, monkeypatch, rows=rows)
    with pytest.raises(RuntimeError, match="No valid Silver rows remain"):
        module.run(DAY)
    assert not catalog.writes
    assert events == ["start", "fail"]


@pytest.mark.parametrize("source,silver", SOURCES)
def test_destructive_replacement_is_rejected_before_any_write(source, silver, monkeypatch):
    module, catalog, events = prepare_silver(source, silver, monkeypatch)
    old = bronze_rows(source)
    old = old.set_column(old.schema.get_field_index("order_id"), "order_id", pa.array(["previous-order"]))
    catalog.rows = module.transform(old, DAY)
    before = catalog.rows
    with pytest.raises(ValueError, match="would remove existing order"):
        module.run(DAY)
    assert not catalog.writes
    assert catalog.rows.equals(before)
    assert events == ["start", "fail"]


@pytest.mark.parametrize("source", ["shopify", "amazon"])
@pytest.mark.parametrize("mismatch", [False, True])
def test_bronze_batch_partition_and_fresh_readback(source, mismatch, monkeypatch):
    module = importlib.import_module(f"ingestion.{source}_bronze")
    rows = bronze_rows(source)
    catalog = Catalog(rows, rows.slice(0, 0))
    catalog.corrupt_readback = mismatch
    monkeypatch.setattr(catalog, "load_table", lambda identifier: Handle(catalog, catalog.rows))
    monkeypatch.setattr(module, "get_catalog", lambda: catalog)
    monkeypatch.setattr(module, "ensure_table", lambda c: Handle(c, c.rows, writable=True))
    def read_source(path, day):
        assert day == DAY
        return None, rows
    monkeypatch.setattr(module, "read_source_csv", read_source)
    events = record_lineage(monkeypatch, module)
    if mismatch:
        with pytest.raises(RuntimeError, match="Bronze verification failed"):
            module.run(DAY)
        assert events == ["start", "fail"]
    else:
        module.run(DAY)
        assert events == ["start", "complete"]
    assert catalog.writes[0][1].literal.value == "2026-01-05"
