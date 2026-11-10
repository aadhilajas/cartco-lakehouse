"""Build Gold candidates and reconcile each business grouping to canonical orders."""
from collections import Counter, defaultdict
from decimal import Decimal
import math

import pyarrow as pa

from pipeline.commerce_metrics import MONEY, amount, validated_orders
from transforms.gold import channel_performance, customer_360, daily_revenue, inventory_turnover

MARTS = {"daily_revenue": daily_revenue, "channel_performance": channel_performance,
         "customer_360": customer_360, "category_unit_sales": inventory_turnover}
MONEY_COLUMNS = {"daily_revenue": ("total_revenue", "avg_order_value"),
                 "channel_performance": ("total_revenue",), "customer_360": ("total_spend",)}
VALUE_BRIDGE = {"placed_order_value": None, "cancelled_order_value": "cancelled",
                "refunded_order_value": "refunded", "pending_order_value": "pending"}


def _rows(table, keys):
    rows = table.to_pylist()
    result = {tuple(row[name] for name in keys): row for row in rows}
    if len(result) != len(rows):
        raise ValueError("Gold output has duplicate grouping keys")
    return result


def _money(actual, expected):
    if actual is None or Decimal(str(actual)) != amount(actual) or amount(actual) != expected:
        raise ValueError("Gold money does not reconcile in exact cents")


def validate_marts(orders, marts):
    if set(marts) != set(MARTS):
        raise ValueError("Gold candidate has missing or unexpected marts")
    rows = validated_orders(orders).to_pylist()
    if any(row["order_status"] == "unknown" for row in rows):
        raise ValueError("Unknown order status blocks Gold publication")
    daily, customers, units = defaultdict(list), defaultdict(list), defaultdict(int)
    channels = defaultdict(list)
    for row in rows:
        channels[row["channel"]].append(row)
        if row["order_status"] == "completed":
            daily[(row["order_date"], row["channel"])].append(row)
            customers[(row["customer_id"], row["channel"])].append(row)
            units[(row["product_category"], row["channel"])] += row["quantity"]
    actual = _rows(marts["daily_revenue"], ("order_date", "channel"))
    if set(actual) != set(daily):
        raise ValueError("Daily revenue coverage differs from completed orders")
    for key, group in daily.items():
        total = sum((row["order_total"] for row in group), Decimal("0.00"))
        _money(actual[key]["total_revenue"], total)
        _money(actual[key]["avg_order_value"], amount(total / len(group)))
        if actual[key]["order_count"] != len(group):
            raise ValueError("Daily completed-order count does not reconcile")
    actual = _rows(marts["channel_performance"], ("channel",))
    if set(actual) != {(channel,) for channel in channels}:
        raise ValueError("Channel coverage does not reconcile")
    for channel, group in channels.items():
        row = actual[(channel,)]
        _money(row["total_revenue"], sum((r["order_total"] for r in group if r["order_status"] == "completed"), Decimal("0.00")))
        if row["total_orders"] != len(group) or row["avg_fulfillment_days"] is not None:
            raise ValueError("Channel counts or unavailable fulfillment metric are invalid")
        for status, name in [("cancelled", "cancellation_rate"), ("refunded", "refund_rate")]:
            expected = sum(r["order_status"] == status for r in group) / len(group)
            if row[name] is None or not math.isfinite(row[name]) or not math.isclose(row[name], expected, abs_tol=1e-12, rel_tol=1e-12):
                raise ValueError("Channel status rate does not reconcile")
        if set(row) & set(VALUE_BRIDGE):
            if set(VALUE_BRIDGE) - set(row):
                raise ValueError("Channel value bridge is incomplete")
            for name, status in VALUE_BRIDGE.items():
                expected = sum((r["order_total"] for r in group if status is None or r["order_status"] == status), Decimal("0.00"))
                _money(row[name], expected)
    actual = _rows(marts["customer_360"], ("customer_id", "channel"))
    if set(actual) != set(customers):
        raise ValueError("Customer coverage does not reconcile")
    for key, group in customers.items():
        row = actual[key]
        _money(row["total_spend"], sum((r["order_total"] for r in group), Decimal("0.00")))
        categories = Counter()
        for record in group:
            categories[record["product_category"]] += record["quantity"]
        favorite = min(categories, key=lambda category: (-categories[category], category))
        if (row["total_orders"], row["first_order_date"], row["last_order_date"], row["favorite_category"]) != (
            len(group), min(r["order_date"] for r in group), max(r["order_date"] for r in group), favorite):
            raise ValueError("Customer history/category does not reconcile")
    actual = _rows(marts["category_unit_sales"], ("product_category", "channel"))
    if set(actual) != set(units) or any(actual[key]["units_sold"] != count for key, count in units.items()):
        raise ValueError("Completed category units do not reconcile")
    completed_dates = [row["order_date"] for row in rows if row["order_status"] == "completed"]
    if completed_dates:
        expected_period = f"{min(completed_dates)} to {max(completed_dates)}"
        if any(row.get("reporting_period") != expected_period for row in actual.values()):
            raise ValueError("Category sales reporting period does not reconcile")
    return dict(active_orders=len(rows), completed_orders=sum(len(group) for group in daily.values()),
                retained_completed_sales=str(sum((r["order_total"] for r in rows if r["order_status"] == "completed"), Decimal("0.00"))),
                order_values={name: str(sum((r["order_total"] for r in rows if status is None or r["order_status"] == status), Decimal("0.00"))) for name, status in VALUE_BRIDGE.items()},
                mart_rows={name: table.num_rows for name, table in marts.items()})


def build_marts(orders, *, currency="INR"):
    if currency != "INR":
        raise ValueError("Only the declared synthetic INR contract is supported")
    clean = validated_orders(orders)
    if "currency" in clean.column_names and set(clean["currency"].to_pylist()) - {currency}:
        raise ValueError("Mixed or unsupported source currencies block publication")
    marts = {name: module.transform(orders) for name, module in MARTS.items()}
    units = marts["category_unit_sales"]
    marts["category_unit_sales"] = units.rename_columns(["reporting_period" if name == "turnover_period" else name for name in units.column_names])
    # New release artifacts persist decimal money; legacy table schemas stay unchanged.
    for name, columns in MONEY_COLUMNS.items():
        table = marts[name]
        for column in columns:
            table = table.set_column(table.schema.get_field_index(column), column, pa.array([amount(v) for v in table[column].to_pylist()], MONEY))
        marts[name] = table.append_column("currency", pa.array([currency] * table.num_rows, pa.string()))
    groups = defaultdict(list)
    for row in clean.to_pylist():
        groups[row["channel"]].append(row)
    channel = marts["channel_performance"]
    for name, status in VALUE_BRIDGE.items():
        values = [sum((r["order_total"] for r in groups[key] if status is None or r["order_status"] == status), Decimal("0.00")) for key in channel["channel"].to_pylist()]
        channel = channel.append_column(name, pa.array(values, MONEY))
    marts["channel_performance"] = channel
    report = validate_marts(orders, marts)
    return marts, report
