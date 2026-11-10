from datetime import date
from decimal import Decimal

import pyarrow as pa
import pytest

from order_state_helpers import APPLIED, events, order, tombstone
from pipeline.commerce_metrics import legacy_money
from pipeline.gold_quality import build_marts, validate_marts
from pipeline.order_state import merge_order_versions
from transforms.gold import channel_performance, customer_360, daily_revenue, inventory_turnover


def mixed_orders():
    return events(
        order("done", unit_price=0.1, order_total=0.1, product_category="B"),
        order("done2", unit_price=0.2, order_total=0.2, product_category="A"),
        order("cancel", order_status="cancelled", unit_price=100.0, order_total=100.0),
        order("refund", order_status="refunded", unit_price=200.0, order_total=200.0),
        order("wait", order_status="pending", unit_price=300.0, order_total=300.0),
    )


def test_daily_sales_exclude_cancelled_refunded_pending_and_sum_exact_cents():
    rows = daily_revenue.transform(mixed_orders()).to_pylist()
    assert len(rows) == 1
    assert rows[0]["total_revenue"] == 0.3
    assert rows[0]["order_count"] == 2
    assert rows[0]["avg_order_value"] == 0.15


def test_channel_denominators_include_all_placed_orders_but_sales_only_completed():
    row = channel_performance.transform(mixed_orders()).to_pylist()[0]
    assert row["total_revenue"] == 0.3
    assert row["total_orders"] == 5
    assert row["cancellation_rate"] == row["refund_rate"] == 0.2
    assert row["avg_fulfillment_days"] is None


def test_customer_history_and_favorite_category_exclude_nonpurchases_and_break_ties():
    table = mixed_orders()
    one = customer_360.transform(table).to_pylist()[0]
    two = customer_360.transform(table.take(pa.array(list(range(table.num_rows))[::-1]))).to_pylist()[0]
    assert one == two
    assert one["total_orders"] == 2 and one["total_spend"] == 0.3
    assert one["favorite_category"] == "A"


def test_category_units_count_completed_sales_only():
    result = inventory_turnover.transform(mixed_orders())
    assert sum(result["units_sold"].to_pylist()) == 2


def test_tombstones_do_not_reach_any_gold_mart():
    state = merge_order_versions(mixed_orders(), tombstone("done2", 2), applied_at=APPLIED).rows
    marts, report = build_marts(state)
    assert report["active_orders"] == 4 and report["completed_orders"] == 1
    assert marts["daily_revenue"]["total_revenue"].to_pylist() == [Decimal("0.10")]
    assert marts["daily_revenue"]["total_revenue"].type == pa.decimal128(18, 2)
    assert marts["daily_revenue"]["currency"].to_pylist() == ["INR"]


def test_channel_with_no_completed_orders_has_zero_sales_and_no_purchase_profiles():
    rows = events(order(order_status="pending"))
    marts, report = build_marts(rows)
    assert report["completed_orders"] == 0
    assert marts["daily_revenue"].num_rows == marts["customer_360"].num_rows == 0
    assert marts["channel_performance"]["total_revenue"].to_pylist() == [Decimal("0.00")]


def test_placed_cancelled_refunded_pending_values_are_separate_from_completed_sales():
    marts, report = build_marts(mixed_orders())
    row = marts["channel_performance"].to_pylist()[0]
    assert row["placed_order_value"] == Decimal("600.30")
    assert row["cancelled_order_value"] == Decimal("100.00")
    assert row["refunded_order_value"] == Decimal("200.00")
    assert row["pending_order_value"] == Decimal("300.00")
    assert row["total_revenue"] == Decimal("0.30")
    assert report["order_values"]["placed_order_value"] == "600.30"


@pytest.mark.parametrize("change", [{"order_total": 99.0}, {"unit_price": None}, {"quantity": -1}, {"order_status": "invalid"}, {"product_category": None}])
def test_invalid_canonical_values_block_candidate_build(change):
    with pytest.raises(ValueError):
        build_marts(events(order(**change)))


def test_duplicate_business_keys_are_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        build_marts(events(order(), order()))


def test_unknown_status_is_visible_in_legacy_metrics_but_blocks_publication():
    with pytest.raises(ValueError, match="Unknown order status"):
        build_marts(events(order(order_status="unknown")))


def test_corrupt_mart_fails_independent_reconciliation():
    orders = mixed_orders()
    marts, _ = build_marts(orders)
    table = marts["daily_revenue"]
    marts["daily_revenue"] = table.set_column(table.schema.get_field_index("total_revenue"), "total_revenue", pa.array([Decimal("999.00")], pa.decimal128(18, 2)))
    with pytest.raises(ValueError, match="reconcile"):
        validate_marts(orders, marts)


def test_mixed_currency_values_are_not_silently_aggregated():
    rows = mixed_orders().append_column("currency", pa.array(["INR", "USD", "INR", "INR", "INR"]))
    with pytest.raises(ValueError, match="currencies"):
        build_marts(rows)


def test_legacy_float_cannot_silently_lose_a_cent():
    with pytest.raises(OverflowError):
        legacy_money([Decimal("9999999999999999.99")])
