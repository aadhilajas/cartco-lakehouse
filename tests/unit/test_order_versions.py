from datetime import date, datetime

import pyarrow as pa
import pytest

from order_state_helpers import APPLIED, events, order, tombstone
from pipeline.order_state import VersionConflict, active_orders, empty_state, initialize_legacy_state, merge_order_versions


def merge(existing, incoming):
    return merge_order_versions(existing, incoming, applied_at=APPLIED)


def test_sparse_incremental_orders_preserve_every_unrelated_key():
    old = events(order("middle", order_date=date(2026, 1, 2)))
    incoming = events(order("left"), order("right", order_date=date(2026, 1, 3)))
    result = merge(old, incoming)
    assert set(result.rows["order_id"].to_pylist()) == {"left", "middle", "right"}
    assert result.counts["inserted"] == 2


def test_new_source_version_updates_without_using_order_placement_time():
    result = merge(events(order()), events(order(version=2, order_status="cancelled")))
    assert result.rows["order_status"].to_pylist() == ["cancelled"]
    assert result.counts["updated"] == 1


def test_stale_replay_cannot_replace_newer_payload_or_audit_time():
    old = events(order(version=3, order_status="refunded"))
    result = merge(old, events(order(version=1)))
    assert result.rows.equals(old)
    assert result.counts["stale"] == 1


def test_exact_replay_preserves_existing_audit_timestamp():
    old = events(order())
    result = merge(old, old)
    assert result.rows.equals(old)
    assert result.counts["unchanged"] == 1


def test_equal_version_conflict_does_not_mutate_inputs():
    old = events(order())
    incoming = events(order(order_status="refunded"))
    before = old.to_pylist()
    with pytest.raises(VersionConflict):
        merge(old, incoming)
    assert old.to_pylist() == before


@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_versions_in_one_batch_are_rejected_in_any_order(reverse):
    rows = [order(), order(order_status="cancelled"), order(version=3)]
    with pytest.raises(VersionConflict):
        merge(empty_state(), events(*(rows[::-1] if reverse else rows)))


def test_conflict_with_stored_version_is_rejected_even_if_newer_input_exists():
    with pytest.raises(VersionConflict):
        merge(events(order()), events(order(order_status="cancelled"), order(version=3)))


def test_duplicate_events_choose_latest_source_version_deterministically():
    rows = [order(version=3, order_status="refunded"), order(), order(version=2)]
    left = merge(empty_state(), events(*rows))
    right = merge(empty_state(), events(*rows[::-1]))
    assert left.rows.equals(right.rows)
    assert left.counts["duplicate_keys"] == 2


def test_key_only_tombstone_retained_and_hidden_from_active_projection():
    result = merge(events(order()), tombstone())
    assert result.rows["is_deleted"].to_pylist() == [True]
    assert result.rows["customer_id"].to_pylist() == [None]
    assert active_orders(result.rows).num_rows == 0


def test_delete_unknown_key_prevents_later_stale_resurrection():
    deleted = merge(empty_state(), tombstone(version=3)).rows
    replay = merge(deleted, events(order(version=1)))
    assert replay.rows.equals(deleted)
    assert active_orders(replay.rows).num_rows == 0


def test_newer_explicit_live_version_can_restore_a_deleted_key():
    deleted = merge(empty_state(), tombstone(version=2)).rows
    result = merge(deleted, events(order(version=3)))
    assert active_orders(result.rows).num_rows == 1


def test_identical_order_ids_are_independent_across_channels():
    result = merge(events(order(channel="amazon")), events(order(channel="shopify")))
    assert result.rows.num_rows == 2


def test_partial_live_patch_is_rejected_instead_of_nulling_existing_fields():
    patch = pa.table({"channel": ["shopify"], "order_id": ["o1"], "source_updated_at": [datetime(2026, 1, 2)]})
    with pytest.raises(ValueError, match="complete canonical order"):
        merge(events(order()), patch)


@pytest.mark.parametrize("changes", [{"source_updated_at": None}, {"order_id": ""}, {"channel": " shopify"}, {"order_total": float("nan")}])
def test_invalid_canonical_events_are_rejected(changes):
    with pytest.raises(ValueError):
        merge(empty_state(), events(order(**changes)))


def test_legacy_migration_requires_complete_trusted_version_map():
    legacy = events(order()).select(events(order()).column_names[:11])
    versions = pa.table({"channel": ["shopify"], "order_id": ["o1"], "source_updated_at": [datetime(2026, 1, 8)]})
    state = initialize_legacy_state(legacy, versions, applied_at=APPLIED, currency="INR")
    assert state["source_updated_at"].to_pylist() == [datetime(2026, 1, 8)]
    assert state["order_total"].to_pylist() == [10.0]


@pytest.mark.parametrize("key", ["unknown", None])
def test_incomplete_or_unmatched_migration_baselines_fail(key):
    legacy = events(order()).select(events(order()).column_names[:11])
    versions = pa.table({"channel": ["shopify"], "order_id": [key], "source_updated_at": [datetime(2026, 1, 8)]})
    with pytest.raises(ValueError):
        initialize_legacy_state(legacy, versions, applied_at=APPLIED, currency="INR")
