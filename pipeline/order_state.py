"""Source-versioned order state, staged separately from legacy silver.orders.

Tombstones remain in state so an old replay cannot resurrect a deleted order.
This module never invents source versions or changes the legacy pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pipeline.money import exact_money

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import BooleanType, DateType, DecimalType, IntegerType, NestedField, StringType, TimestampType


STATE_SCHEMA = Schema(
    NestedField(1, "order_id", StringType(), required=True),
    NestedField(2, "customer_id", StringType(), required=False),
    NestedField(3, "channel", StringType(), required=True),
    NestedField(4, "order_date", DateType(), required=False),
    NestedField(5, "product_id", StringType(), required=False),
    NestedField(6, "product_category", StringType(), required=False),
    NestedField(7, "quantity", IntegerType(), required=False),
    NestedField(8, "unit_price", DecimalType(18, 2), required=False),
    NestedField(9, "order_status", StringType(), required=False),
    NestedField(10, "order_total", DecimalType(18, 2), required=False),
    NestedField(11, "transform_timestamp", TimestampType(), required=False),
    NestedField(12, "source_updated_at", TimestampType(), required=True),
    NestedField(13, "is_deleted", BooleanType(), required=True),
    NestedField(14, "currency", StringType(), required=False),
)
ARROW_SCHEMA = schema_to_pyarrow(STATE_SCHEMA)
KEYS = ("channel", "order_id")
BUSINESS = tuple(name for name in ARROW_SCHEMA.names if name not in {*KEYS, "source_updated_at", "transform_timestamp"})


class VersionConflict(ValueError):
    """Equal source versions disagree; no state may be committed."""


@dataclass(frozen=True)
class MergeResult:
    rows: pa.Table
    counts: dict[str, int]


@dataclass(frozen=True)
class WriteResult:
    counts: dict[str, int]
    snapshot_id: int | None
    committed: bool


def empty_state() -> pa.Table:
    return pa.Table.from_batches([], schema=ARROW_SCHEMA)


def _timestamp(value, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{label} must be an explicit source timestamp")
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _key(row) -> tuple[str, str]:
    values = tuple(row.get(name) for name in KEYS)
    if any(not isinstance(v, str) or not v or v.strip() != v for v in values):
        raise ValueError("channel and order_id must be nonempty canonical strings")
    return values


def _normalize(row, *, audit: datetime | None = None):
    key = _key(row)
    version = _timestamp(row.get("source_updated_at"), "source_updated_at")
    deleted = row.get("is_deleted", False)
    if not isinstance(deleted, bool):
        raise ValueError("is_deleted must be boolean")
    result = {name: row.get(name) for name in ARROW_SCHEMA.names}
    result.update(channel=key[0], order_id=key[1], source_updated_at=version, is_deleted=deleted)
    if deleted:
        for name in BUSINESS:
            if name != "is_deleted":
                result[name] = None
    else:
        if set(ARROW_SCHEMA.names[:10]) - set(row):
            raise ValueError("Live upserts must carry a complete canonical order, not a partial patch")
        if not isinstance(result["customer_id"], str) or not result["customer_id"].strip():
            raise ValueError("A live order needs customer_id")
        if not isinstance(result["order_date"], date) or isinstance(result["order_date"], datetime):
            raise ValueError("A live order needs a calendar order_date")
        if result["currency"] != "INR":
            raise ValueError("Live state requires explicit INR currency")
        for name in ("unit_price", "order_total"):
            result[name] = exact_money(result[name])
        quantity = result["quantity"]
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0:
            raise ValueError("Quantity must be a nonnegative integer")
        if result["order_total"] != exact_money(result["unit_price"] * quantity):
            raise ValueError("Order total differs from quantity times unit price")
    if audit is not None:
        result["transform_timestamp"] = audit
    elif result["transform_timestamp"] is not None:
        result["transform_timestamp"] = _timestamp(result["transform_timestamp"], "transform_timestamp")
    return result


def _payload(row):
    return tuple(row[name] for name in BUSINESS)


def merge_order_versions(existing: pa.Table, incoming: pa.Table, *, applied_at: datetime) -> MergeResult:
    """Merge canonical source events by (channel, order_id, source_updated_at).

    Older events are ignored; equal conflicting versions abort the entire batch.
    Exact replays retain the original audit timestamp. Incoming tombstones only
    need key/version/is_deleted. Every existing row must already have a trusted
    source version; legacy rows require explicit baseline migration first.
    """
    audit = _timestamp(applied_at, "applied_at")
    for table in (existing, incoming):
        unknown = set(table.column_names) - set(ARROW_SCHEMA.names)
        if unknown:
            raise ValueError(f"Unexpected canonical columns: {sorted(unknown)}")
    state = {}
    for raw in existing.to_pylist():
        row = _normalize(raw)
        key = _key(row)
        if key in state:
            raise ValueError("Existing state contains duplicate order keys")
        state[key] = row
    winners, seen = {}, {}
    for raw in incoming.to_pylist():
        row = _normalize(raw, audit=audit)
        key = _key(row)
        version_key = (*key, row["source_updated_at"])
        payload = _payload(row)
        stored = state.get(key)
        if stored is not None and stored["source_updated_at"] == row["source_updated_at"] and _payload(stored) != payload:
            raise VersionConflict(f"Input disagrees with stored source version for {key}")
        if version_key in seen and seen[version_key] != payload:
            raise VersionConflict(f"Conflicting input at the same source version for {key}")
        seen[version_key] = payload
        if key not in winners or row["source_updated_at"] > winners[key]["source_updated_at"]:
            winners[key] = row
    counts = dict(input_rows=incoming.num_rows, duplicate_keys=incoming.num_rows - len(winners), inserted=0, updated=0, deleted=0, stale=0, unchanged=0)
    for key, row in winners.items():
        old = state.get(key)
        if old is not None:
            if row["source_updated_at"] < old["source_updated_at"]:
                counts["stale"] += 1
                continue
            if row["source_updated_at"] == old["source_updated_at"]:
                if _payload(row) != _payload(old):
                    raise VersionConflict(f"Input disagrees with stored source version for {key}")
                counts["unchanged"] += 1
                continue
        counts["deleted" if row["is_deleted"] else "updated" if old is not None else "inserted"] += 1
        state[key] = row
    rows = pa.Table.from_pylist([state[key] for key in sorted(state)], schema=ARROW_SCHEMA)
    return MergeResult(rows, counts)


def active_orders(state: pa.Table) -> pa.Table:
    """Project live business fields and persisted currency for candidate publication."""
    return state.filter(pc.equal(state["is_deleted"], False)).select([*ARROW_SCHEMA.names[:11], "currency"])


def initialize_legacy_state(legacy: pa.Table, versions: pa.Table, *, applied_at: datetime, currency: str) -> pa.Table:
    """Prepare migration with a complete, externally trusted key/version map.

    Neither order_date, audit time nor load date is a source update version.
    This function does not alter the source table or publish the prepared state.
    """
    if set(versions.column_names) != {*KEYS, "source_updated_at"}:
        raise ValueError("Baseline map must contain exactly channel, order_id, source_updated_at")
    version_map = {}
    for row in versions.to_pylist():
        key = _key(row)
        if key in version_map:
            raise ValueError("Duplicate baseline version keys")
        version_map[key] = _timestamp(row["source_updated_at"], "source_updated_at")
    source_keys = [_key(row) for row in legacy.to_pylist()]
    if len(source_keys) != len(set(source_keys)):
        raise ValueError("Legacy table contains duplicate keys")
    if set(source_keys) != set(version_map):
        raise ValueError("Baseline versions must match legacy keys exactly")
    if set(legacy.column_names) != set(ARROW_SCHEMA.names[:11]):
        raise ValueError("Legacy export must contain exactly the eleven business/audit columns")
    if currency != "INR":
        raise ValueError("Legacy migration requires an explicit INR declaration")
    events = [dict(row, unit_price=exact_money(row["unit_price"], allow_legacy_float=True),
                   order_total=exact_money(row["order_total"], allow_legacy_float=True),
                   currency=currency, source_updated_at=version_map[_key(row)], is_deleted=False)
              for row in legacy.to_pylist()]
    incoming = pa.Table.from_pylist(events, schema=ARROW_SCHEMA)
    return merge_order_versions(empty_state(), incoming, applied_at=applied_at).rows


def write_order_state(catalog, identifier: str, incoming: pa.Table, *, channel: str, applied_at: datetime, max_attempts: int = 3, snapshot_properties: dict[str, str] | None = None) -> WriteResult:
    """Atomically replace one channel with merged state, retrying only conflicts.

    The caller must explicitly create a separate state table with STATE_SCHEMA
    and an identity channel partition. No legacy schema is migrated implicitly.
    Each retry reloads both metadata and data and recomputes the complete merge.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    if incoming.num_rows and set(incoming["channel"].to_pylist()) != {channel}:
        raise ValueError("Incoming events must belong to the requested channel")
    scope = EqualTo("channel", channel)
    for attempt in range(max_attempts):
        table = catalog.load_table(identifier)
        if table.properties.get("cartco.state.status") == "initializing":
            raise ValueError("State migration is still initializing")
        if not schema_to_pyarrow(table.schema()).equals(ARROW_SCHEMA, check_metadata=False):
            raise ValueError("State schema differs; an explicit migration is required")
        channel_id = table.schema().find_field("channel").field_id
        if not any(field.source_id == channel_id and isinstance(field.transform, IdentityTransform) for field in table.spec().fields):
            raise ValueError("State table must use an identity channel partition")
        current = table.scan(row_filter=scope).to_arrow()
        result = merge_order_versions(current, incoming, applied_at=applied_at)
        if not any(result.counts[name] for name in ("inserted", "updated", "deleted")):
            snapshot = table.current_snapshot()
            return WriteResult(result.counts, snapshot.snapshot_id if snapshot else None, False)
        try:
            table.overwrite(df=result.rows, overwrite_filter=scope, snapshot_properties={**(snapshot_properties or {}), "cartco.merge.channel": channel})
        except CommitFailedException:
            if attempt + 1 == max_attempts:
                raise
            continue
        snapshot_id = table.current_snapshot().snapshot_id
        verified = catalog.load_table(identifier).scan(row_filter=scope, snapshot_id=snapshot_id).to_arrow()
        if not verified.sort_by([("order_id", "ascending")]).equals(result.rows.sort_by([("order_id", "ascending")])):
            raise RuntimeError("Committed order state differs from the prepared merge")
        return WriteResult(result.counts, snapshot_id, True)
    raise AssertionError("Unreachable")
