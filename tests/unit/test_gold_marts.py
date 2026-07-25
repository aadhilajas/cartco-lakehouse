"""Unit tests for transforms.gold marts (daily_revenue, channel_performance, inventory_turnover, customer_360).

Exercises the pure transform() function in each module against small, synthetic
PyArrow tables. No live Iceberg catalog is required to run these tests.
"""

from __future__ import annotations

from datetime import date, datetime

import pyarrow as pa

from transforms.gold.channel_performance import transform as transform_channel_performance
from transforms.gold.customer_360 import transform as transform_customer_360
from transforms.gold.daily_revenue import transform as transform_daily_revenue
from transforms.gold.inventory_turnover import transform as transform_inventory_turnover


def _make_silver_orders_table(
    order_id: list[str],
    customer_id: list[str],
    channel: list[str],
    order_date: list[date],
    product_category: list[str],
    quantity: list[int],
    unit_price: list[float],
    order_status: list[str],
) -> pa.Table:
    n = len(order_id)
    order_total = [q * p for q, p in zip(quantity, unit_price)]
    ts = [datetime(2026, 1, 1)] * n
    return pa.table({
        "order_id": pa.array(order_id, type=pa.string()),
        "customer_id": pa.array(customer_id, type=pa.string()),
        "channel": pa.array(channel, type=pa.string()),
        "order_date": pa.array(order_date, type=pa.date32()),
        "product_id": pa.array([f"P{i}" for i in range(n)], type=pa.string()),
        "product_category": pa.array(product_category, type=pa.string()),
        "quantity": pa.array(quantity, type=pa.int32()),
        "unit_price": pa.array(unit_price, type=pa.float64()),
        "order_status": pa.array(order_status, type=pa.string()),
        "order_total": pa.array(order_total, type=pa.float64()),
        "transform_timestamp": pa.array(ts, type=pa.timestamp("us")),
    })


def test_daily_revenue_mart_aggregates_correctly():
    table = _make_silver_orders_table(
        order_id=["o1", "o2", "o3"],
        customer_id=["c1", "c2", "c3"],
        channel=["shopify", "shopify", "amazon"],
        order_date=[date(2026, 1, 1), date(2026, 1, 1), date(2026, 1, 2)],
        product_category=["Electronics", "Books", "Electronics"],
        quantity=[2, 1, 3],
        unit_price=[100.0, 50.0, 200.0],
        order_status=["completed", "completed", "completed"],
    )

    result = transform_daily_revenue(table)
    assert result.num_rows == 2

    rows = {(row["order_date"], row["channel"]): row for row in result.to_pylist()}

    shopify_row = rows[(date(2026, 1, 1), "shopify")]
    assert shopify_row["total_revenue"] == 250.0
    assert shopify_row["order_count"] == 2
    assert shopify_row["avg_order_value"] == 125.0

    amazon_row = rows[(date(2026, 1, 2), "amazon")]
    assert amazon_row["total_revenue"] == 600.0
    assert amazon_row["order_count"] == 1
    assert amazon_row["avg_order_value"] == 600.0


def test_channel_performance_mart_computes_cancellation_and_refund_rates():
    table = _make_silver_orders_table(
        order_id=["o1", "o2", "o3", "o4"],
        customer_id=["c1", "c2", "c3", "c4"],
        channel=["shopify", "shopify", "amazon", "amazon"],
        order_date=[date(2026, 1, 1)] * 4,
        product_category=["Electronics"] * 4,
        quantity=[1, 1, 1, 1],
        unit_price=[100.0, 100.0, 100.0, 100.0],
        order_status=["completed", "cancelled", "refunded", "completed"],
    )

    result = transform_channel_performance(table)
    assert result.num_rows == 2

    rows = {row["channel"]: row for row in result.to_pylist()}

    assert rows["shopify"]["total_orders"] == 2
    assert rows["shopify"]["cancellation_rate"] == 0.5
    assert rows["shopify"]["refund_rate"] == 0.0

    assert rows["amazon"]["total_orders"] == 2
    assert rows["amazon"]["cancellation_rate"] == 0.0
    assert rows["amazon"]["refund_rate"] == 0.5


def test_inventory_turnover_mart_groups_by_category_and_channel():
    table = _make_silver_orders_table(
        order_id=["o1", "o2", "o3"],
        customer_id=["c1", "c2", "c3"],
        channel=["shopify", "shopify", "amazon"],
        order_date=[date(2026, 1, 1), date(2026, 1, 5), date(2026, 1, 10)],
        product_category=["Electronics", "Books", "Electronics"],
        quantity=[10, 5, 20],
        unit_price=[10.0, 10.0, 10.0],
        order_status=["completed"] * 3,
    )

    result = transform_inventory_turnover(table)
    assert result.num_rows == 3

    rows = {(row["product_category"], row["channel"]): row for row in result.to_pylist()}

    assert rows[("Electronics", "shopify")]["units_sold"] == 10
    assert rows[("Books", "shopify")]["units_sold"] == 5
    assert rows[("Electronics", "amazon")]["units_sold"] == 20
    assert rows[("Electronics", "shopify")]["turnover_period"] == "2026-01-01 to 2026-01-10"


def test_customer_360_mart_identifies_favorite_category_and_lifetime_spend():
    table = _make_silver_orders_table(
        order_id=["o1", "o2", "o3", "o4"],
        customer_id=["c1", "c1", "c1", "c2"],
        channel=["shopify", "shopify", "shopify", "amazon"],
        order_date=[date(2026, 1, 1), date(2026, 1, 5), date(2026, 1, 10), date(2026, 1, 2)],
        product_category=["Electronics", "Electronics", "Books", "Clothing"],
        quantity=[2, 3, 1, 5],
        unit_price=[10.0, 20.0, 30.0, 40.0],
        order_status=["completed"] * 4,
    )

    result = transform_customer_360(table)
    assert result.num_rows == 2

    rows = {(row["customer_id"], row["channel"]): row for row in result.to_pylist()}

    c1_profile = rows[("c1", "shopify")]
    assert c1_profile["total_orders"] == 3
    assert c1_profile["total_spend"] == 110.0
    assert c1_profile["first_order_date"] == date(2026, 1, 1)
    assert c1_profile["last_order_date"] == date(2026, 1, 10)
    assert c1_profile["favorite_category"] == "Electronics"

    c2_profile = rows[("c2", "amazon")]
    assert c2_profile["total_orders"] == 1
    assert c2_profile["total_spend"] == 200.0
    assert c2_profile["favorite_category"] == "Clothing"
