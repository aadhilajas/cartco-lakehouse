"""Unit tests for transforms.amazon_silver — status mapping and transform pipeline.

Covers:
    - All 4 Amazon status transitions explicitly (shipped/cancelled/
      refund_initiated/pending -> completed/cancelled/refunded/pending)
    - Unknown status fallback to "unknown"
    - ASIN -> product_id rename
    - Dedup, null-drop, order_total, schema/column_order

Uses only small in-memory PyArrow tables — no Iceberg catalog required.
"""

from __future__ import annotations

from datetime import date, datetime

import pyarrow as pa
import pytest

from transforms.amazon_silver import (
    COLUMN_ORDER,
    STATUS_CANONICAL_MAP,
    UNKNOWN_STATUS_LABEL,
    _canonicalize_status_to_order_status,
    transform,
)


# ---------------------------------------------------------------------------
# Shared builder — shapes like a bronze.amazon_orders scan (no ingestion_date)
# ---------------------------------------------------------------------------

def _make_amazon_bronze_table(
    order_id: list[str | None],
    customer_id: list[str | None],
    order_date: list[datetime],
    quantity: list[int | None],
    unit_price: list[float | None],
    status: list[str | None],
    asin: list[str] | None = None,
    product_category: list[str | None] | None = None,
    marketplace: list[str] | None = None,
    fulfillment_channel: list[str] | None = None,
) -> pa.Table:
    n = len(order_id)
    asin = asin or [f"B0ASIN{i:05d}" for i in range(n)]
    product_category = product_category or ["Electronics"] * n
    marketplace = marketplace or ["amazon.in"] * n
    fulfillment_channel = fulfillment_channel or ["FBA"] * n
    return pa.table(
        {
            "order_id": pa.array(order_id, type=pa.string()),
            "customer_id": pa.array(customer_id, type=pa.string()),
            "channel": pa.array(["amazon"] * n, type=pa.string()),
            "order_date": pa.array(order_date, type=pa.timestamp("us")),
            "asin": pa.array(asin, type=pa.string()),
            "product_category": pa.array(product_category, type=pa.string()),
            "quantity": pa.array(quantity, type=pa.int32()),
            "unit_price": pa.array(unit_price, type=pa.float64()),
            "status": pa.array(status, type=pa.string()),
            "marketplace": pa.array(marketplace, type=pa.string()),
            "fulfillment_channel": pa.array(fulfillment_channel, type=pa.string()),
        }
    )


# ---------------------------------------------------------------------------
# Status mapping — one test per source value (explicit transition coverage)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("source_status, expected_canonical", [
    ("shipped", "completed"),
    ("cancelled", "cancelled"),
    ("refund_initiated", "refunded"),
    ("pending", "pending"),
])
def test_status_mapping_each_transition(source_status, expected_canonical):
    """Each of the 4 Amazon status values must map to its canonical counterpart."""
    table = _make_amazon_bronze_table(
        order_id=["o1"],
        customer_id=["c1"],
        order_date=[datetime(2026, 1, 1)],
        quantity=[1],
        unit_price=[100.0],
        status=[source_status],
    )
    result = _canonicalize_status_to_order_status(table)
    assert result.column("order_status").to_pylist() == [expected_canonical], (
        f"{source_status!r} should map to {expected_canonical!r}; "
        f"got {result.column('order_status').to_pylist()}"
    )


def test_status_mapping_unknown_value_falls_back_to_unknown_label():
    """A status outside the map should produce UNKNOWN_STATUS_LABEL, not raise."""
    table = _make_amazon_bronze_table(
        order_id=["o1"],
        customer_id=["c1"],
        order_date=[datetime(2026, 1, 1)],
        quantity=[1],
        unit_price=[100.0],
        status=["some_future_status"],
    )
    result = _canonicalize_status_to_order_status(table)
    assert result.column("order_status").to_pylist() == [UNKNOWN_STATUS_LABEL]


def test_status_canonical_map_covers_all_four_amazon_statuses():
    """STATUS_CANONICAL_MAP must contain exactly the 4 Amazon source values."""
    assert set(STATUS_CANONICAL_MAP.keys()) == {
        "shipped", "cancelled", "refund_initiated", "pending"
    }


# ---------------------------------------------------------------------------
# Full transform pipeline checks
# ---------------------------------------------------------------------------

def test_transform_renames_asin_to_product_id():
    """After transform(), the 'asin' column must be gone; 'product_id' must exist."""
    table = _make_amazon_bronze_table(
        order_id=["o1"],
        customer_id=["c1"],
        order_date=[datetime(2026, 1, 1)],
        quantity=[1],
        unit_price=[500.0],
        status=["shipped"],
        asin=["B0CX7K2JMP"],
    )
    result = transform(table, transform_date=date(2026, 1, 2))
    assert "product_id" in result.column_names
    assert "asin" not in result.column_names
    assert result.column("product_id").to_pylist() == ["B0CX7K2JMP"]


def test_transform_output_schema_and_column_order():
    """Output must have exactly COLUMN_ORDER columns and no 'status' / 'asin'."""
    table = _make_amazon_bronze_table(
        order_id=["o1"],
        customer_id=["c1"],
        order_date=[datetime(2026, 1, 1)],
        quantity=[2],
        unit_price=[1200.0],
        status=["pending"],
    )
    result = transform(table, transform_date=date(2026, 1, 2))
    assert result.column_names == COLUMN_ORDER
    assert "status" not in result.column_names
    assert "asin" not in result.column_names
    assert "order_status" in result.column_names
    assert "transform_timestamp" in result.column_names


def test_transform_computes_order_total():
    table = _make_amazon_bronze_table(
        order_id=["o1", "o2"],
        customer_id=["c1", "c2"],
        order_date=[datetime(2026, 1, 1), datetime(2026, 1, 2)],
        quantity=[4, 3],
        unit_price=[250.0, 1000.0],
        status=["shipped", "cancelled"],
    )
    result = transform(table, transform_date=date(2026, 1, 3))
    totals = dict(zip(result.column("order_id").to_pylist(), result.column("order_total").to_pylist()))
    assert totals["o1"] == pytest.approx(4 * 250.0)
    assert totals["o2"] == pytest.approx(3 * 1000.0)


def test_transform_drops_null_order_id_or_customer_id():
    table = _make_amazon_bronze_table(
        order_id=["o1", None, "o3"],
        customer_id=["c1", "c2", None],
        order_date=[datetime(2026, 1, 1)] * 3,
        quantity=[1, 1, 1],
        unit_price=[10.0, 10.0, 10.0],
        status=["shipped", "cancelled", "pending"],
    )
    result = transform(table, transform_date=date(2026, 1, 2))
    assert result.num_rows == 1
    assert result.column("order_id").to_pylist() == ["o1"]
