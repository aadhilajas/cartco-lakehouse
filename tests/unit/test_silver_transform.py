"""Unit tests for transforms.silver_shopify_orders.transform().

Uses only small in-memory PyArrow tables built via tests.helpers -- no
Iceberg catalog and no Airflow are required to run these.
"""

from __future__ import annotations

from datetime import date, datetime

from helpers import make_bronze_table
from transforms.silver_shopify_orders import COLUMN_ORDER, transform


def test_transform_keeps_only_latest_row_per_order_id():
    # o1 appears twice (earlier + later order_date); o2 appears once.
    table = make_bronze_table(
        order_id=["o1", "o1", "o2"],
        customer_id=["c1", "c1", "c2"],
        order_date=[datetime(2026, 1, 1), datetime(2026, 1, 5), datetime(2026, 1, 2)],
        quantity=[1, 2, 3],
        unit_price=[10.0, 20.0, 30.0],
        status=["delivered", "delivered", "returned"],
    )

    result = transform(table, transform_date=date(2026, 1, 6))

    assert result.num_rows == 2
    rows = {row["order_id"]: row for row in result.to_pylist()}
    assert rows["o1"]["order_date"] == date(2026, 1, 5)
    assert rows["o1"]["quantity"] == 2  # the later (2026-01-05) row's quantity, not the earlier one's


def test_transform_drops_rows_with_null_order_id_or_customer_id():
    table = make_bronze_table(
        order_id=["o1", None, "o3"],
        customer_id=["c1", "c2", None],
        order_date=[datetime(2026, 1, 1)] * 3,
        quantity=[1, 2, 3],
        unit_price=[10.0, 20.0, 30.0],
        status=["delivered", "cancelled", "returned"],
    )

    result = transform(table, transform_date=date(2026, 1, 6))

    # Only the first row has both order_id and customer_id present.
    assert result.num_rows == 1
    assert result.column("order_id").to_pylist() == ["o1"]


def test_transform_computes_order_total_as_quantity_times_unit_price():
    table = make_bronze_table(
        order_id=["o1", "o2"],
        customer_id=["c1", "c2"],
        order_date=[datetime(2026, 1, 1), datetime(2026, 1, 2)],
        quantity=[3, 5],
        unit_price=[9.5, 2.0],
        status=["delivered", "cancelled"],
    )

    result = transform(table, transform_date=date(2026, 1, 6))

    totals = dict(zip(result.column("order_id").to_pylist(), result.column("order_total").to_pylist()))
    assert totals["o1"] == 3 * 9.5
    assert totals["o2"] == 5 * 2.0


def test_transform_output_has_expected_schema_and_column_order():
    table = make_bronze_table(
        order_id=["o1"],
        customer_id=["c1"],
        order_date=[datetime(2026, 1, 1)],
        quantity=[1],
        unit_price=[10.0],
        status=["delivered"],
    )

    result = transform(table, transform_date=date(2026, 1, 6))

    assert result.column_names == COLUMN_ORDER
    assert "status" not in result.column_names
    assert "order_status" in result.column_names
    assert "transform_timestamp" in result.column_names


def test_transform_fills_default_nulls_for_quantity_price_and_category():
    table = make_bronze_table(
        order_id=["o1"],
        customer_id=["c1"],
        order_date=[datetime(2026, 1, 1)],
        quantity=[None],
        unit_price=[None],
        status=["delivered"],
        product_category=[None],
    )

    result = transform(table, transform_date=date(2026, 1, 6))
    row = result.to_pylist()[0]

    assert row["quantity"] == 0
    assert row["unit_price"] == 0.0
    assert row["product_category"] == "UNKNOWN"
    assert row["order_total"] == 0.0
