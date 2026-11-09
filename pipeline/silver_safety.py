"""Fail closed while legacy Silver lacks a source-versioned merge contract."""
import pyarrow as pa


class UnsafeSnapshotError(ValueError):
    """A legacy range overwrite would lose or ambiguously update existing data."""


def validate_snapshot_replacement(existing: pa.Table, incoming: pa.Table, channel: str) -> None:
    """Allow new orders and exact business replays; reject deletes/updates.

    Call against a fresh, channel-scoped scan from the same table handle used
    for the overwrite. Compare every incoming business field except the new
    transform audit timestamp. This is a temporary safety gate, not an upsert.
    """
    columns = [name for name in incoming.column_names if name != "transform_timestamp"]
    new_rows = incoming.select(columns).to_pylist()
    old_rows = existing.select(columns).to_pylist()
    if not new_rows:
        raise UnsafeSnapshotError("An empty batch cannot replace Silver")
    if any(row["channel"] != channel or row["order_date"] is None for row in new_rows + old_rows):
        raise UnsafeSnapshotError("Snapshot rows must have valid dates and the expected channel")
    incoming_by_key = {row["order_id"]: row for row in new_rows}
    existing_by_key = {row["order_id"]: row for row in old_rows}
    if len(incoming_by_key) != len(new_rows) or len(existing_by_key) != len(old_rows):
        raise UnsafeSnapshotError("Silver order keys must be unique within a channel")
    if None in incoming_by_key or None in existing_by_key:
        raise UnsafeSnapshotError("Silver order keys cannot be null")
    lower = min(row["order_date"] for row in new_rows)
    upper = max(row["order_date"] for row in new_rows)
    for key, old_row in existing_by_key.items():
        replacement = incoming_by_key.get(key)
        if replacement is not None:
            if replacement != old_row:
                raise UnsafeSnapshotError(
                    f"Order {key!r} changes existing values; source versions and a versioned merge are required"
                )
        elif lower <= old_row["order_date"] <= upper:
            raise UnsafeSnapshotError(
                f"Batch would remove existing order {key!r}; refusing destructive range overwrite"
            )
