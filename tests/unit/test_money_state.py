from decimal import Decimal

import pyarrow as pa
import pytest

from order_state_helpers import APPLIED, events, order
from pipeline.money import exact_money
from pipeline.order_state import empty_state, merge_order_versions
from pipeline.gold_quality import build_marts


@pytest.mark.parametrize("value", ["0.001", "NaN", "Infinity", "-0.01", "10000000000000000", True, None, 0.1])
def test_money_rejects_rounding_overflow_and_inexact_inputs(value):
    with pytest.raises(ValueError):
        exact_money(value)


def test_full_decimal_range_and_trailing_zeroes_are_exact():
    assert exact_money("9999999999999999.99") == Decimal("9999999999999999.99")
    assert exact_money("0.1000") == Decimal("0.10")


@pytest.mark.parametrize("changes", [{"currency": None}, {"currency": "USD"}, {"order_total": "9.99"}])
def test_live_state_rejects_missing_currency_and_inconsistent_totals(changes):
    with pytest.raises(ValueError):
        merge_order_versions(empty_state(), events(order(**changes)), applied_at=APPLIED)


def test_decimal_gold_does_not_round_trip_through_a_binary_float():
    rows = events(order(unit_price="9999999999999999.99", order_total="9999999999999999.99"))
    marts, quality = build_marts(rows)
    assert quality["retained_completed_sales"] == "9999999999999999.99"
    assert marts["daily_revenue"]["total_revenue"].type == pa.decimal128(18, 2)
    assert marts["customer_360"]["total_spend"][0].as_py() == Decimal("9999999999999999.99")
