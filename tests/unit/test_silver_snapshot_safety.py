"""Regression scenarios for legacy range-overwrite data loss."""
from datetime import date, datetime

import pyarrow as pa
import pytest

from pipeline.silver_safety import UnsafeSnapshotError, validate_snapshot_replacement


def table(*records, timestamp=datetime(2026, 1, 1)):
    schema = pa.schema([
        ("order_id", pa.string()), ("channel", pa.string()),
        ("order_date", pa.date32()), ("order_status", pa.string()),
        ("order_total", pa.float64()), ("transform_timestamp", pa.timestamp("us")),
    ])
    return pa.Table.from_pylist([
        {"order_id": key, "channel": "shopify", "order_date": date(2026, 1, day),
         "order_status": status, "order_total": amount, "transform_timestamp": timestamp}
        for key, day, status, amount in records
    ], schema=schema)


def test_sparse_batch_cannot_erase_order_between_minimum_and_maximum_dates():
    existing = table(("untouched", 2, "completed", 100))
    incoming = table(("new1", 1, "completed", 10), ("new3", 3, "completed", 30))
    with pytest.raises(UnsafeSnapshotError, match="would remove existing order"):
        validate_snapshot_replacement(existing, incoming, "shopify")


def test_identical_business_replay_allows_new_audit_timestamp():
    records = [("o1", 1, "completed", 100)]
    validate_snapshot_replacement(table(*records), table(*records, timestamp=datetime(2026, 1, 2)), "shopify")


def test_same_key_update_requires_source_version_instead_of_overwriting_silently():
    with pytest.raises(UnsafeSnapshotError, match="source versions"):
        validate_snapshot_replacement(table(("o1", 1, "completed", 100)), table(("o1", 1, "refunded", 100)), "shopify")


def test_changing_order_date_cannot_create_a_second_copy_in_a_new_partition():
    with pytest.raises(UnsafeSnapshotError, match="source versions"):
        validate_snapshot_replacement(table(("o1", 1, "completed", 100)), table(("o1", 4, "completed", 100)), "shopify")


def test_new_orders_outside_existing_range_are_allowed():
    validate_snapshot_replacement(table(("old", 1, "completed", 100)), table(("new", 3, "completed", 50)), "shopify")


def test_safe_coverage_can_add_orders_while_preserving_existing_keys():
    validate_snapshot_replacement(
        table(("old", 2, "completed", 100)),
        table(("new1", 1, "completed", 10), ("old", 2, "completed", 100), ("new3", 3, "completed", 30)),
        "shopify",
    )


def test_legacy_duplicate_keys_block_replacement_until_repaired():
    with pytest.raises(UnsafeSnapshotError, match="unique"):
        validate_snapshot_replacement(table(("old", 1, "completed", 100), ("old", 1, "completed", 100)), table(("new", 3, "completed", 50)), "shopify")
