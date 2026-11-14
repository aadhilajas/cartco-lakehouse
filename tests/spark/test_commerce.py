"""Real Spark calculations compared with the pre-existing independent oracle."""
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from pipeline.spark_commerce import STATE_SCHEMA, normalize, merge_state, gold_marts, inventory_turnover
from ingestion.protocols.fixtures import shopify_order, amazon_order, amazon_item
from ingestion.protocols.shopify import normalize as shopify_row
from ingestion.protocols.amazon import normalize as amazon_row
from pipeline.raw_batches import RawBatch
from pipeline.source_contracts import daily_contract
from pipeline.versioned_ingestion import canonical_events
from pipeline.order_state import ARROW_SCHEMA, empty_state, merge_order_versions
from pipeline.gold_quality import build_marts
import pyarrow as pa


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[2]").appName("cartco-commerce-tests").config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def dataframe(spark, rows):
    columns = sorted(set().union(*(row.keys() for row in rows)))
    return spark.createDataFrame([tuple(str(row.get(key, "")) for key in columns) for row in rows], ",".join(f"{name} string" for name in columns))


def canonical(source, rows):
    columns = sorted(set().union(*(row.keys() for row in rows)))
    return canonical_events(RawBatch("probe", source, "2026-11-12", Path("."), tuple(columns), tuple(rows), daily_contract(source, "2026-11-12")))


def ordered(rows):
    return sorted((dict(row) for row in rows), key=lambda row: str(sorted(row.items())))


def test_real_spark_normalization_merge_and_all_gold_match_reference(spark):
    inputs = {"shopify": [shopify_row(shopify_order(i)) for i in range(1, 4)],
              "amazon": [amazon_row(amazon_order(i), [amazon_item(i)]) for i in range(1, 4)]}
    inputs["shopify"][1]["status"] = "cancelled"
    inputs["amazon"][1]["status"] = "refund_initiated"
    # Same customer, equal quantity categories: lexical category is the stable tie breaker.
    inputs["shopify"][0]["customer_id"] = inputs["shopify"][2]["customer_id"] = "same"
    inputs["shopify"][0]["product_category"] = "Zebra"
    incoming = None
    reference = []
    for source, rows in inputs.items():
        part = normalize(dataframe(spark, rows), source=source, contract=daily_contract(source, "2026-11-12"), applied_at="2026-11-12T07:00:00Z")
        incoming = part if incoming is None else incoming.unionByName(part)
        reference.extend(canonical(source, rows).to_pylist())
    expected = merge_order_versions(empty_state(), pa.Table.from_pylist(reference, schema=ARROW_SCHEMA), applied_at=datetime(2026, 11, 12, 7)).rows
    merged = merge_state(spark.createDataFrame([], STATE_SCHEMA), incoming).cache()
    assert ordered([r.asDict() for r in merged.collect()]) == ordered(expected.to_pylist())
    marts, _ = build_marts(expected)
    for name, frame in gold_marts(merged).items():
        assert ordered([r.asDict() for r in frame.collect()]) == ordered(marts[name].to_pylist()), name
    # Exact replay retains the original audit time even if the attempt time changes.
    assert ordered([r.asDict() for r in merge_state(merged, incoming).collect()]) == ordered(expected.to_pylist())
    merged.unpersist()


def test_source_version_conflicts_late_arrivals_and_tombstones(spark):
    row = shopify_row(shopify_order(1))
    def frame(rows):
        return normalize(dataframe(spark, rows), source="shopify", contract=daily_contract("shopify", "2026-11-12"), applied_at="2026-11-12T07:00:00Z")
    existing = frame([row]).cache()
    conflict = {**row, "unit_price": "150.50"}
    with pytest.raises(ValueError, match="conflicting"):
        merge_state(existing, frame([conflict]))
    older = {**row, "source_updated_at": "2026-11-11T06:00:00Z", "unit_price": "50.50"}
    assert merge_state(existing, frame([older])).first().unit_price == Decimal("100.50")
    deleted = {**row, "source_updated_at": "2026-11-12T06:30:00Z", "is_deleted": "true"}
    tombstone = merge_state(existing, frame([deleted]))
    final = merge_state(tombstone, frame([row])).first()
    assert final.is_deleted and final.unit_price is None
    with pytest.raises(ValueError, match="exact INR"):
        frame([{**row, "unit_price": "10.001"}])
    existing.unpersist()


def test_inventory_uses_cost_and_real_daily_stock_and_exposes_missing_days(spark):
    row = shopify_row(shopify_order(1))
    state = normalize(dataframe(spark, [row]), source="shopify", contract=daily_contract("shopify", "2026-11-12"), applied_at="2026-11-12T07:00:00Z")
    stock = [dict(sku="shopify:" + row["product_id"], location_id="WH1", snapshot_date=day, on_hand="10", unit_cost="20.00", currency="INR") for day in ["2026-11-11", "2026-11-12"]]
    result = inventory_turnover(state, dataframe(spark, stock), period_start="2026-11-11", period_end="2026-11-13").first()
    assert result.cogs == Decimal("20.00") and result.average_daily_inventory_value == Decimal("200.00") and result.turnover == Decimal("0.1")
    result = inventory_turnover(state, dataframe(spark, stock[:1]), period_start="2026-11-11", period_end="2026-11-13").first()
    assert result.quality_status == "incomplete_daily_coverage" and result.turnover is None
    thirds = [{**row, "on_hand": "3", "unit_cost": "1.00"} for row in stock]
    result = inventory_turnover(state, dataframe(spark, thirds), period_start="2026-11-11", period_end="2026-11-13").first()
    assert result.turnover == Decimal("0.333333333333")
