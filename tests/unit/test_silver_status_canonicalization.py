"""Unit tests for Bronze -> Silver status canonicalization.

Covers transforms.silver_shopify_orders._canonicalize_status_to_order_status,
the function responsible for the contract boundary described in that
module's docstring: Bronze stays source-faithful (delivered/cancelled/
returned) and only Silver maps those to the canonical vocabulary
(completed/cancelled/refunded/unknown).
"""

from __future__ import annotations

from datetime import datetime

from helpers import make_bronze_table
from transforms.silver_shopify_orders import (
    UNKNOWN_STATUS_LABEL,
    _canonicalize_status_to_order_status,
)


def _table_with_statuses(statuses: list[str]) -> "object":
    n = len(statuses)
    return make_bronze_table(
        order_id=[f"o{i}" for i in range(n)],
        customer_id=[f"c{i}" for i in range(n)],
        order_date=[datetime(2026, 1, 1)] * n,
        quantity=[1] * n,
        unit_price=[10.0] * n,
        status=statuses,
    )


def test_known_statuses_map_to_canonical_values():
    table = _table_with_statuses(["delivered", "cancelled", "returned"])

    result = _canonicalize_status_to_order_status(table)

    assert "status" not in result.column_names
    assert result.column("order_status").to_pylist() == ["completed", "cancelled", "refunded"]


def test_unknown_status_maps_to_unknown_label():
    table = _table_with_statuses(["delivered", "bogus_status", "returned"])

    result = _canonicalize_status_to_order_status(table)

    assert result.column("order_status").to_pylist() == [
        "completed",
        UNKNOWN_STATUS_LABEL,
        "refunded",
    ]


def test_rows_unknown_status_is_tracked_in_stats():
    table = _table_with_statuses(["delivered", "bogus_status", "another_bogus", "returned"])
    stats: dict[str, int] = {}

    _canonicalize_status_to_order_status(table, stats=stats)

    assert stats["rows_unknown_status"] == 2


def test_all_known_statuses_report_zero_unknown():
    table = _table_with_statuses(["delivered", "cancelled", "returned", "delivered"])
    stats: dict[str, int] = {}

    _canonicalize_status_to_order_status(table, stats=stats)

    assert stats["rows_unknown_status"] == 0
