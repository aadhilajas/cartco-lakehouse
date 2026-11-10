"""Validated local Gold releases from a pinned state snapshot and source receipts.

Consumers read CURRENT.json. Independent legacy Gold tables are not modified.
"""
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.expressions import EqualTo

from pipeline.batch import resolve_batch_date
from pipeline.gold_quality import MARTS, build_marts
from pipeline.raw_batches import _write_once
from pipeline.versioned_ingestion import STATE_TABLE
from pipeline.order_state import ARROW_SCHEMA
from pyiceberg.io.pyarrow import schema_to_pyarrow

METRIC_VERSION = "retained-completed-sales-v2"


def _json(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _source_evidence(catalog, state, receipt_paths, required, day):
    snapshots = {snapshot.snapshot_id: snapshot for snapshot in state.metadata.snapshots}
    current = state.current_snapshot()
    current_id = current.snapshot_id if current else None
    chain, cursor = [], current_id
    while cursor is not None:
        if cursor not in snapshots or cursor in chain:
            raise ValueError("State snapshot ancestry is unavailable")
        chain.append(cursor)
        cursor = snapshots[cursor].parent_snapshot_id
    for snapshot_id in chain:
        props = snapshots[snapshot_id].summary.additional_properties
        channel = props.get("cartco.merge.channel")
        if channel and (channel not in required or not re.fullmatch(r"[0-9a-f]{64}", props.get("cartco.batch_id", ""))):
            raise ValueError("Untracked state commit blocks Gold publication")
        if not channel and not props.get("cartco.migration.sha256"):
            raise ValueError("Untracked state commit blocks Gold publication")
    receipts = {}
    for path in receipt_paths:
        receipt = json.loads(Path(path).read_text(encoding="utf-8"))
        source = receipt.get("source")
        if source not in required or source in receipts:
            raise ValueError("Exactly one completion receipt per required source is needed")
        if receipt.get("status") != "complete" or receipt.get("batch_date") != day:
            raise ValueError("Source receipt is failed or belongs to another batch date")
        if receipt.get("state_table_uuid") != str(state.metadata.table_uuid):
            raise ValueError("Source receipt belongs to another state table")
        batch_directory = Path(path).resolve().parent.parent
        raw_manifest = json.loads((batch_directory / "manifest.json").read_text(encoding="utf-8"))
        input_sha = hashlib.sha256((batch_directory / "source.csv").read_bytes()).hexdigest()
        expected_batch_id = hashlib.sha256(f"{source}\n{day}\n{input_sha}".encode()).hexdigest()
        if (raw_manifest.get("sha256"), raw_manifest.get("batch_id"), raw_manifest.get("source"), raw_manifest.get("batch_date"), raw_manifest.get("rows")) != (
            input_sha, expected_batch_id, source, day, receipt.get("counts", {}).get("input_rows")) or receipt.get("batch_id") != expected_batch_id:
            raise ValueError("Archived source manifest/checksum differs from the receipt")
        latest = next((index for index, snapshot_id in enumerate(chain)
            if snapshots[snapshot_id].summary.additional_properties.get("cartco.merge.channel") == source
            or snapshots[snapshot_id].summary.additional_properties.get("cartco.migration.sha256")), None)
        receipt_id = receipt.get("state_snapshot_id")
        if receipt_id is None:
            if receipt.get("counts", {}).get("input_rows") != 0 or latest is not None:
                raise ValueError("Empty source receipt does not cover current state")
        elif receipt_id not in chain or (latest is not None and chain.index(receipt_id) > latest):
            raise ValueError("Source receipt predates its latest state change")
        if receipt.get("committed"):
            props = snapshots[receipt_id].summary.additional_properties
            if props.get("cartco.batch_id") != receipt.get("batch_id") or props.get("cartco.merge.channel") != source:
                raise ValueError("Receipt does not identify its claimed state commit")
        raw = catalog.load_table(f"bronze.{source}_order_events")
        raw_id = receipt.get("bronze_snapshot_id")
        if raw_id is None and receipt.get("counts", {}).get("input_rows") != 0:
            raise ValueError("Nonempty source receipt needs a Bronze snapshot")
        records = raw.scan(row_filter=EqualTo("batch_id", receipt["batch_id"]), snapshot_id=raw_id).to_arrow()
        if records.num_rows != receipt.get("counts", {}).get("input_rows"):
            raise ValueError("Raw batch coverage differs from source receipt")
        if records.num_rows and (set(records["source"].to_pylist()) != {source}
            or set(records["ingestion_date"].to_pylist()) != {date.fromisoformat(day)}):
            raise ValueError("Raw batch source/date differs from completion receipt")
        receipts[source] = dict(batch_id=receipt["batch_id"], state_snapshot_id=receipt_id, bronze_snapshot_id=raw_id, input_sha256=input_sha)
    if set(receipts) != required:
        raise ValueError("A required source completion receipt is missing")
    if not any(receipt["state_snapshot_id"] == current_id for receipt in receipts.values()):
        raise ValueError("Receipts do not cover the latest state snapshot")
    return current_id, receipts


def publish_gold_release(catalog, output_root, receipt_paths, *, batch_date, expected_channels=("shopify", "amazon"), currency="INR"):
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".publication.lock"
    # Fail on an existing lock; do not steal one left by another process/crash.
    with lock.open("x", encoding="utf-8") as stream:
        stream.write(str(os.getpid()))
    try:
        required = set(expected_channels)
        if not required or required - {"shopify", "amazon"}:
            raise ValueError("Declare supported required sources explicitly")
        day = resolve_batch_date(batch_date).isoformat()
        state = catalog.load_table(STATE_TABLE)
        if state.properties.get("cartco.state.status") != "ready":
            raise ValueError("Order state is not ready")
        if not schema_to_pyarrow(state.schema()).equals(ARROW_SCHEMA, check_metadata=False):
            raise ValueError("Gold requires the explicit decimal/currency V2 state schema")
        snapshot_id, evidence = _source_evidence(catalog, state, receipt_paths, required, day)
        orders = state.scan(snapshot_id=snapshot_id).to_arrow()
        if set(orders["channel"].to_pylist()) - required:
            raise ValueError("State contains undeclared source channels")
        marts, quality = build_marts(orders, currency=currency)
        contract = dict(metric_version=METRIC_VERSION, state_table_uuid=str(state.metadata.table_uuid),
                        state_snapshot_id=snapshot_id, batch_date=day, required_sources=sorted(required),
                        source_evidence=evidence, currency=currency,
                        currency_basis="Persisted INR source contract or explicit migration declaration")
        release_id = hashlib.sha256(_json(contract)).hexdigest()
        directory = root / "releases" / release_id
        artifacts = {}
        for name, table in marts.items():
            buffer = pa.BufferOutputStream()
            pq.write_table(table, buffer)
            content = buffer.getvalue().to_pybytes()
            if not pq.read_table(pa.BufferReader(content)).equals(table):
                raise RuntimeError("Gold artifact serialization changed data")
            filename = name + ".parquet"
            _write_once(directory / filename, content)
            artifacts[filename] = dict(sha256=hashlib.sha256(content).hexdigest(), rows=table.num_rows)
        manifest = dict(contract, release_id=release_id, artifacts=artifacts, quality=quality)
        content = _json(manifest)
        _write_once(directory / "manifest.json", content)
        latest = catalog.load_table(STATE_TABLE)
        latest_id = latest.current_snapshot().snapshot_id if latest.current_snapshot() else None
        if latest_id != snapshot_id or latest.metadata.table_uuid != state.metadata.table_uuid or latest.properties.get("cartco.state.status") != "ready":
            raise ValueError("State changed during Gold preparation; previous release retained")
        pointer = _json(dict(release_id=release_id, manifest_sha256=hashlib.sha256(content).hexdigest()))
        with tempfile.NamedTemporaryFile(dir=root, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(pointer)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.replace(temporary, root / "CURRENT.json")
        finally:
            if temporary.exists():
                temporary.unlink()
        return manifest
    finally:
        lock.unlink()


def read_gold_release(output_root):
    root = Path(output_root).resolve()
    pointer = json.loads((root / "CURRENT.json").read_text(encoding="utf-8"))
    release_id = pointer["release_id"]
    if not re.fullmatch(r"[0-9a-f]{64}", release_id):
        raise ValueError("Invalid Gold release identifier")
    directory = root / "releases" / release_id
    content = (directory / "manifest.json").read_bytes()
    if hashlib.sha256(content).hexdigest() != pointer["manifest_sha256"]:
        raise ValueError("Gold manifest checksum differs")
    manifest = json.loads(content)
    if manifest.get("release_id") != release_id or set(manifest["artifacts"]) != {name + ".parquet" for name in MARTS}:
        raise ValueError("Gold release artifact inventory differs")
    tables = {}
    for filename, metadata in manifest["artifacts"].items():
        content = (directory / filename).read_bytes()
        if hashlib.sha256(content).hexdigest() != metadata["sha256"]:
            raise ValueError("Gold artifact checksum differs")
        table = pq.read_table(pa.BufferReader(content))
        if table.num_rows != metadata["rows"]:
            raise ValueError("Gold artifact row count differs")
        tables[filename.removesuffix(".parquet")] = table
    return tables, manifest
