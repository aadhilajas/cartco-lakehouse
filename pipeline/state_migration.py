"""Install a verified candidate in a new state table without altering legacy data."""
from datetime import datetime
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.exceptions import NoSuchTableError

from pipeline.order_state import ARROW_SCHEMA, active_orders, empty_state, merge_order_versions
from pipeline.versioned_ingestion import STATE_TABLE, create_state_table


def install_candidate(catalog, candidate_path):
    candidate_path = Path(candidate_path)
    manifest = json.loads(candidate_path.with_suffix(candidate_path.suffix + ".manifest.json").read_text(encoding="utf-8"))
    content = candidate_path.read_bytes()
    if manifest.get("kind") != "prepared-order-state-migration" or manifest.get("published") is not False:
        raise ValueError("Not an unpublished migration candidate")
    if hashlib.sha256(content).hexdigest() != manifest.get("candidate_sha256"):
        raise ValueError("Migration candidate checksum differs")
    candidate = pq.read_table(pa.BufferReader(content))
    if not candidate.schema.equals(ARROW_SCHEMA, check_metadata=False):
        raise ValueError("Migration candidate schema differs")
    audit = datetime.fromisoformat(manifest["applied_at"])
    normalized = merge_order_versions(empty_state(), candidate, applied_at=audit).rows
    if not normalized.equals(candidate) or candidate.num_rows != manifest.get("state_rows"):
        raise ValueError("Migration candidate is not canonical")
    if candidate["is_deleted"].to_pylist().count(True):
        raise ValueError("Legacy baseline migration cannot introduce tombstones")
    try:
        catalog.load_table(STATE_TABLE)
    except NoSuchTableError:
        pass
    else:
        raise ValueError("State table already exists; refusing to replace it")
    legacy = catalog.load_table("silver.orders")
    baseline_snapshot = legacy.current_snapshot().snapshot_id if legacy.current_snapshot() else None
    baseline_metadata = legacy.metadata_location
    current = legacy.scan().to_arrow()
    expected = active_orders(candidate)
    if set(current.column_names) != set(expected.column_names) or any(
        current.schema.field(name).type != expected.schema.field(name).type for name in expected.column_names
    ):
        raise ValueError("Current legacy schema differs from the migration baseline")
    business = ARROW_SCHEMA.names[:10]
    expected_business = expected.select(business).sort_by([("channel", "ascending"), ("order_id", "ascending")])
    current_business = current.select(business).cast(expected_business.schema).sort_by([("channel", "ascending"), ("order_id", "ascending")])
    if not current_business.equals(expected_business):
        raise ValueError("Current legacy orders differ from the migration baseline")
    table = create_state_table(catalog, initializing=True)
    table.overwrite(candidate, snapshot_properties={"cartco.migration.sha256": manifest["candidate_sha256"]})
    snapshot_id = table.current_snapshot().snapshot_id
    verified = catalog.load_table(STATE_TABLE).scan(snapshot_id=snapshot_id).to_arrow()
    if not verified.sort_by([("channel", "ascending"), ("order_id", "ascending")]).equals(candidate):
        raise RuntimeError("Migration read-back differs; state remains initializing")
    legacy_now = catalog.load_table("silver.orders")
    current_snapshot = legacy_now.current_snapshot().snapshot_id if legacy_now.current_snapshot() else None
    if baseline_snapshot != current_snapshot or baseline_metadata != legacy_now.metadata_location:
        raise RuntimeError("Legacy changed during migration; state remains initializing")
    with table.transaction() as transaction:
        transaction.set_properties({"cartco.state.status": "ready", "cartco.migration.sha256": manifest["candidate_sha256"]})
    return dict(state_table=STATE_TABLE, state_snapshot_id=snapshot_id, rows=candidate.num_rows,
                legacy_snapshot_id=baseline_snapshot, published_to_gold=False)
