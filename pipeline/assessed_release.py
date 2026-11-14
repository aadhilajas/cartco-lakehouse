"""Independent exact reconciliation + GE gate, then one atomic release pointer."""
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import tempfile

import pyarrow as pa
from pipeline.order_state import ARROW_SCHEMA
from pipeline.gold_quality import build_marts, validate_marts, MARTS
from pipeline.inventory import SCHEMA as INVENTORY_SCHEMA, build_turnover, validate_turnover
from pipeline.output_quality import validate_outputs
from pipeline.raw_batches import _write_once


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2, default=str) + "\n").encode()


def arrow_rows(rows, schema):
    converted = []
    for record in rows:
        if set(record) != set(schema.names):
            raise ValueError("Candidate columns differ from independently declared schema")
        row = {}
        for field in schema:
            value = record[field.name]
            if value is not None:
                if pa.types.is_decimal(field.type): value = Decimal(str(value))
                elif pa.types.is_date(field.type): value = date.fromisoformat(value)
                elif pa.types.is_timestamp(field.type): value = datetime.fromisoformat(value)
            row[field.name] = value
        converted.append(row)
    return pa.Table.from_pylist(converted, schema=schema)


def inventory_reference(orders, candidate):
    """Rebuild the old inventory input contract from declared SFTP observations.

    In this bounded fixture each source-qualified SKU declares one location and
    fixed unit cost. No matching across channels or inferred retail selling cost.
    """
    stocks = candidate["inventory_rows"]
    scope = {}
    for record in stocks:
        key = (record["sku"], record["location_id"])
        cost = record["unit_cost"]
        if key in scope and scope[key]["unit_cost"] != cost:
            raise ValueError("SFTP fixed standard cost changed")
        scope[key] = {"sku": key[0], "location_id": key[1], "unit_cost": cost}
    by_sku = {row["sku"]: row for row in scope.values()}
    if len(by_sku) != len(scope):
        raise ValueError("SFTP SKU has ambiguous location")
    products = []
    for sku in by_sku:
        channel, product_id = sku.split(":", 1)
        products.append(dict(channel=channel, product_id=product_id, sku=sku))
    period = candidate["inventory_period"]
    start, end = map(date.fromisoformat, [period["period_start"], period["period_end"]])
    allocations = []
    for row in orders.to_pylist():
        if row["is_deleted"] or row["order_status"] != "completed" or not start <= row["order_date"] < end:
            continue
        sku = row["channel"] + ":" + row["product_id"]
        item = by_sku[sku]
        allocations.append(dict(channel=row["channel"], order_id=row["order_id"], sku=sku, location_id=item["location_id"], quantity=str(row["quantity"]), unit_cost=item["unit_cost"], cogs=str(Decimal(item["unit_cost"]) * row["quantity"]), cogs_date=row["order_date"].isoformat()))
    snapshots = [dict(sku=row["sku"], location_id=row["location_id"], snapshot_date=row["snapshot_date"], on_hand=row["on_hand"], inventory_value=str(Decimal(row["unit_cost"]) * int(row["on_hand"]))) for row in stocks]
    inputs = {"scope.csv": list(scope.values()), "products.csv": products, "allocations.csv": allocations, "snapshots.csv": snapshots}
    manifest = {**period, "is_synthetic": True}
    reference, report = build_turnover(orders, manifest, inputs)
    return reference, report, manifest, inputs


def normalized_rows(table):
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in table.to_pylist())


def validate_candidate(candidate):
    if candidate.get("schema_version") != 1 or candidate.get("is_synthetic") is not True or candidate["compute_engine"]["spark"] != "3.5.6":
        raise ValueError("Unsupported assessed candidate")
    if set(candidate["marts"]) != {*MARTS, "inventory_turnover"}:
        raise ValueError("Assessed release requires all declared marts")
    orders = arrow_rows(candidate["silver_rows"], ARROW_SCHEMA)
    expected, quality = build_marts(orders)
    marts = {name: arrow_rows(candidate["marts"][name], table.schema) for name, table in expected.items()}
    for name in expected:
        if normalized_rows(marts[name]) != normalized_rows(expected[name]):
            raise ValueError(f"Spark {name} differs from independent reference")
    validate_marts(orders, marts)
    reference, report, manifest, inputs = inventory_reference(orders, candidate)
    inventory = arrow_rows(candidate["marts"]["inventory_turnover"], INVENTORY_SCHEMA)
    validate_turnover(inventory, inputs, manifest)
    if normalized_rows(inventory) != normalized_rows(reference):
        raise ValueError("Spark inventory differs from independent reference")
    marts["inventory_turnover"] = inventory
    quality["inventory"] = report
    quality["great_expectations"] = validate_outputs(orders, marts)
    return quality


def publish(candidate_path, root):
    """Serial local publisher. Failed gates leave the prior CURRENT unchanged."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    lock = root / "PUBLICATION.lock"
    with lock.open("x", encoding="utf-8") as stream:
        stream.write(str(os.getpid()))
    try:
        raw = Path(candidate_path).read_bytes()
        candidate = json.loads(raw)
        try:
            quality = validate_candidate(candidate)
        except Exception as exc:
            report = getattr(exc, "report", {"error_type": type(exc).__name__, "error": str(exc)})
            _write_once(root / "rejections" / (hashlib.sha256(raw).hexdigest() + ".json"), json_bytes(report))
            raise
        release_id = candidate["release_id"]
        import re
        if not re.fullmatch(r"r[0-9a-f]{32}", release_id):
            raise ValueError("Unsafe release identity")
        directory = root / "releases" / release_id
        _write_once(directory / "candidate.json", raw)
        _write_once(directory / "quality.json", json_bytes(quality))
        manifest = {"schema_version": 1, "release_id": release_id, "candidate_sha256": hashlib.sha256(raw).hexdigest(), "catalog": candidate["catalog"], "quality": quality,
            "batch_date": candidate["batch_date"], "computed_at": candidate["computed_at"], "is_synthetic": True}
        content = json_bytes(manifest)
        _write_once(directory / "manifest.json", content)
        pointer = json_bytes({"release_id": release_id, "manifest_sha256": hashlib.sha256(content).hexdigest()})
        with tempfile.NamedTemporaryFile(dir=root, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(pointer)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.replace(temporary, root / "CURRENT.json")
        finally:
            temporary.unlink(missing_ok=True)
        return manifest
    finally:
        lock.unlink()



# Public compatibility import; reading a release needs only the standard library.
from pipeline.published_release import read_release
