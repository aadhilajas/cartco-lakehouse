"""Explicit source CSV -> immutable raw batch -> Bronze -> validated state path.

Legacy Bronze/Silver tables and Gold publication are never modified here.
"""
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import json

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException, NamespaceAlreadyExistsError, NoSuchTableError, TableAlreadyExistsError
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, IntegerType, NestedField, StringType

from pipeline.order_state import ARROW_SCHEMA, STATE_SCHEMA, empty_state, merge_order_versions, write_order_state
from pipeline.raw_batches import archive_csv, record_receipt

STATE_TABLE = "silver.order_state"
RAW_SCHEMA = Schema(
    NestedField(1, "batch_id", StringType(), required=True),
    NestedField(2, "source_row_number", IntegerType(), required=True),
    NestedField(3, "ingestion_date", DateType(), required=True),
    NestedField(4, "source", StringType(), required=True),
    NestedField(5, "raw_payload", StringType(), required=True),
)
STATUS = {"shopify": {"delivered": "completed", "cancelled": "cancelled", "returned": "refunded"},
          "amazon": {"shipped": "completed", "cancelled": "cancelled", "refund_initiated": "refunded", "pending": "pending"}}


def ensure_namespace(catalog, name):
    try:
        catalog.create_namespace(name)
    except NamespaceAlreadyExistsError:
        pass


def create_state_table(catalog, *, initializing=False):
    ensure_namespace(catalog, "silver")
    return catalog.create_table(STATE_TABLE, schema=STATE_SCHEMA,
        partition_spec=PartitionSpec(PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="channel")),
        properties={"cartco.state.status": "initializing" if initializing else "ready"})


def require_state(catalog, *, initialize_empty=False):
    try:
        state = catalog.load_table(STATE_TABLE)
    except NoSuchTableError:
        if not initialize_empty:
            raise ValueError("State table is absent: explicitly initialize a new catalog or migrate legacy orders")
        try:
            legacy = catalog.load_table("silver.orders")
        except NoSuchTableError:
            legacy = None
        if legacy is not None and legacy.scan(limit=1).to_arrow().num_rows:
            raise ValueError("Legacy orders exist; a trusted version-map migration is required")
        try:
            state = create_state_table(catalog)
        except TableAlreadyExistsError:
            state = catalog.load_table(STATE_TABLE)
    if state.properties.get("cartco.state.status") != "ready":
        raise ValueError("State is not ready; finish or repair the explicit migration first")
    return state


def land_raw_batch(catalog, batch):
    ensure_namespace(catalog, "bronze")
    identifier = f"bronze.{batch.source}_order_events"
    expected = pa.Table.from_pylist([
        dict(batch_id=batch.batch_id, source_row_number=index, ingestion_date=date.fromisoformat(batch.batch_date),
             source=batch.source, raw_payload=json.dumps(row, sort_keys=True))
        for index, row in enumerate(batch.rows, 1)
    ], schema=schema_to_pyarrow(RAW_SCHEMA))
    scope = EqualTo("batch_id", batch.batch_id)
    for attempt in range(3):
        try:
            table = catalog.load_table(identifier)
        except NoSuchTableError:
            try:
                table = catalog.create_table(identifier, schema=RAW_SCHEMA,
                    partition_spec=PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="batch_id")))
            except TableAlreadyExistsError:
                table = catalog.load_table(identifier)
        if not schema_to_pyarrow(table.schema()).equals(expected.schema, check_metadata=False):
            raise ValueError("Versioned Bronze schema differs; refusing implicit migration")
        if not any(field.source_id == 1 and isinstance(field.transform, IdentityTransform) for field in table.spec().fields):
            raise ValueError("Versioned Bronze needs an identity batch_id partition")
        old = table.scan(row_filter=scope).to_arrow()
        if old.num_rows:
            if not old.sort_by([("source_row_number", "ascending")]).equals(expected):
                raise ValueError("Existing Bronze batch differs from immutable input")
            return table.current_snapshot().snapshot_id
        if not expected.num_rows:
            return table.current_snapshot().snapshot_id if table.current_snapshot() else None
        try:
            table.overwrite(expected, overwrite_filter=scope, snapshot_properties={"cartco.batch_id": batch.batch_id})
        except CommitFailedException:
            if attempt == 2:
                raise
            continue
        snapshot_id = table.current_snapshot().snapshot_id
        verified = catalog.load_table(identifier).scan(row_filter=scope, snapshot_id=snapshot_id).to_arrow()
        if not verified.sort_by([("source_row_number", "ascending")]).equals(expected):
            raise RuntimeError("Versioned Bronze read-back differs")
        return snapshot_id
    raise AssertionError("Unreachable")


def canonical_events(batch):
    if {"order_id", "source_updated_at", "is_deleted"} - set(batch.columns):
        raise ValueError("Versioned input needs order_id, source_updated_at and is_deleted headers")
    rows = []
    for index, raw in enumerate(batch.rows, 1):
        try:
            key = raw.get("order_id", "")
            if not key or key.strip() != key or raw.get("channel", batch.source) != batch.source:
                raise ValueError("Invalid source key/channel")
            stamp = raw.get("source_updated_at", "")
            version = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            if version.tzinfo is None or version.utcoffset() is None:
                raise ValueError("Source version requires a timezone")
            version = version.astimezone(timezone.utc).replace(tzinfo=None)
            if raw.get("is_deleted") not in {"true", "false"}:
                raise ValueError("is_deleted must be true or false")
            row = dict(order_id=key, channel=batch.source, source_updated_at=version, is_deleted=raw["is_deleted"] == "true")
            if not row["is_deleted"]:
                product_key = "asin" if batch.source == "amazon" else "product_id"
                required = ["customer_id", "order_date", product_key, "product_category", "quantity", "unit_price", "status"]
                if any(not raw.get(name, "").strip() for name in required):
                    raise ValueError("A live event needs the complete source order")
                quantity = int(raw["quantity"])
                price = Decimal(raw["unit_price"])
                if not 0 <= quantity <= 10000 or not price.is_finite() or not 0 <= price <= 500000:
                    raise ValueError("Quantity/price outside source contract")
                status = STATUS[batch.source].get(raw["status"])
                if status is None:
                    raise ValueError("Unknown source status")
                row.update(customer_id=raw["customer_id"], order_date=datetime.fromisoformat(raw["order_date"]).date(),
                    product_id=raw[product_key], product_category=raw["product_category"], quantity=quantity,
                    unit_price=float(price), order_status=status, order_total=float(quantity * price))
            rows.append(row)
        except (ValueError, InvalidOperation, OverflowError) as exc:
            raise ValueError(f"Source row {index}: {exc}") from exc
    return pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)


def run_versioned_batch(catalog, csv_path, archive_root, *, source, batch_date, applied_at, initialize_empty=False):
    batch = archive_csv(csv_path, archive_root, source=source, batch_date=batch_date)
    report = {"status": "rejected"}
    try:
        report["bronze_snapshot_id"] = land_raw_batch(catalog, batch)
        incoming = canonical_events(batch)
        # Batch-level key/version conflicts are a quality failure before state access.
        merge_order_versions(empty_state(), incoming, applied_at=applied_at)
        require_state(catalog, initialize_empty=initialize_empty)
        result = write_order_state(catalog, STATE_TABLE, incoming, channel=source, applied_at=applied_at,
                                   snapshot_properties={"cartco.batch_id": batch.batch_id})
        report.update(status="complete", state_snapshot_id=result.snapshot_id, committed=result.committed,
                      counts=result.counts, batch_id=batch.batch_id,
                      state_table_uuid=str(catalog.load_table(STATE_TABLE).metadata.table_uuid))
    except Exception as exc:
        report.update(error_type=type(exc).__name__, error=str(exc))
        record_receipt(batch, report)
        raise
    receipt = record_receipt(batch, report)
    return dict(report, receipt=str(receipt))
