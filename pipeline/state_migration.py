"""Install a verified candidate in a new state table without altering legacy data."""
from datetime import datetime
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.exceptions import NoSuchTableError

from pipeline.source_contracts import zone
from pipeline.order_state import ARROW_SCHEMA, active_orders, empty_state, merge_order_versions
from pipeline.versioned_ingestion import STATE_TABLE, V1_STATE_TABLE, create_state_table


def migrate_money_state(catalog, *, currency, business_timezone):
    """Copy v1 into a separate v2 table; never replace or alter the v1 table.

    Caller must stop v1 writers before migrating. Metadata drift observed during
    the copy leaves v2 initializing. New source receipts are required for Gold.
    """
    from pipeline.money import exact_money
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    zone(business_timezone)
    if currency != "INR":
        raise ValueError("Money migration requires an explicit INR declaration")
    try:
        catalog.load_table(STATE_TABLE)
    except NoSuchTableError:
        pass
    else:
        raise ValueError("V2 state already exists; refusing to replace it")
    source = catalog.load_table(V1_STATE_TABLE)
    if source.properties.get("cartco.state.status") != "ready":
        raise ValueError("V1 state is not ready")
    schema = schema_to_pyarrow(source.schema())
    expected = pa.schema([pa.field(field.name, pa.float64() if field.name in {"unit_price", "order_total"} else field.type,
                                  nullable=field.nullable) for field in ARROW_SCHEMA if field.name != "currency"])
    if not schema.equals(expected, check_metadata=False):
        raise ValueError("V1 state schema differs")
    snapshot = source.current_snapshot()
    source_id = snapshot.snapshot_id if snapshot else None
    rows = source.scan(snapshot_id=source_id).to_arrow().to_pylist()
    for row in rows:
        # Deleted payload is canonicalized by the original v1 merger.
        row["currency"] = None if row["is_deleted"] else currency
        for name in ("unit_price", "order_total"):
            if row[name] is not None:
                row[name] = exact_money(row[name], allow_legacy_float=True)
    candidate = pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)
    # Preserve audit timestamps, versions and tombstones, including empty state.
    normalized = merge_order_versions(candidate, empty_state(), applied_at=datetime(2000, 1, 1)).rows
    candidate = candidate.sort_by([("channel", "ascending"), ("order_id", "ascending")])
    if not normalized.equals(candidate):
        raise ValueError("V1 state is not canonical")
    provenance = dict(source_table=V1_STATE_TABLE, source_uuid=str(source.metadata.table_uuid),
                      source_snapshot_id=source_id, source_metadata=source.metadata_location,
                      currency_declaration=currency, business_timezone=business_timezone, contract="exact-inr-decimal-v2")
    fingerprint = hashlib.sha256(json.dumps(provenance, sort_keys=True).encode()).hexdigest()
    target = create_state_table(catalog, initializing=True, business_timezone=business_timezone)
    if candidate.num_rows:
        target.overwrite(candidate, snapshot_properties={"cartco.migration.sha256": fingerprint})
    verified = catalog.load_table(STATE_TABLE).scan().to_arrow()
    if not verified.sort_by([("channel", "ascending"), ("order_id", "ascending")]).equals(candidate):
        raise RuntimeError("Money migration read-back differs; V2 remains initializing")
    if catalog.load_table(V1_STATE_TABLE).metadata_location != source.metadata_location:
        raise RuntimeError("V1 changed during migration; V2 remains initializing")
    with target.transaction() as transaction:
        transaction.set_properties({"cartco.state.status": "ready", "cartco.migration.sha256": fingerprint,
                                    "cartco.migration.provenance": json.dumps(provenance, sort_keys=True)})
    return dict(state_table=STATE_TABLE, rows=candidate.num_rows, provenance=provenance,
                published_to_gold=False, requires_new_source_receipts=True)


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
    try:
        catalog.load_table(V1_STATE_TABLE)
    except NoSuchTableError:
        pass
    else:
        raise ValueError("V1 state exists; use explicit money migration instead of a legacy baseline")
    if manifest.get("currency_declaration") != "INR":
        raise ValueError("Migration manifest requires an explicit INR declaration")
    legacy = catalog.load_table("silver.orders")
    baseline_snapshot = legacy.current_snapshot().snapshot_id if legacy.current_snapshot() else None
    baseline_metadata = legacy.metadata_location
    current = legacy.scan().to_arrow()
    expected = active_orders(candidate).select(ARROW_SCHEMA.names[:11])
    if set(current.column_names) != set(expected.column_names) or any(
        current.schema.field(name).type != expected.schema.field(name).type for name in expected.column_names if name not in {"unit_price", "order_total"}
    ):
        raise ValueError("Current legacy schema differs from the migration baseline")
    business = ARROW_SCHEMA.names[:10]
    expected_business = expected.select(business).sort_by([("channel", "ascending"), ("order_id", "ascending")])
    from pipeline.money import exact_money
    for name in ("unit_price", "order_total"):
        current = current.set_column(current.schema.get_field_index(name), name,
            pa.array([exact_money(v, allow_legacy_float=True) for v in current[name].to_pylist()], ARROW_SCHEMA.field(name).type))
    current_business = current.select(business).cast(expected_business.schema).sort_by([("channel", "ascending"), ("order_id", "ascending")])
    if not current_business.equals(expected_business):
        raise ValueError("Current legacy orders differ from the migration baseline")
    business_timezone = manifest.get("business_timezone")
    zone(business_timezone)
    table = create_state_table(catalog, initializing=True, business_timezone=business_timezone)
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
