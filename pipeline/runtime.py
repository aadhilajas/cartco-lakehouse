"""Airflow-independent runtime callbacks; default catalog is REST, tests inject SQLite."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
from uuid import uuid4

from pipeline.batch import resolve_batch_date
from pipeline.gold_release import publish_gold_release, read_gold_release
from pipeline.inventory import read_bundle
from pipeline.raw_batches import _write_once
from pipeline.source_contracts import SOURCES, TIMEZONE_PROPERTY, daily_contract, timestamp, validate_contract
from pipeline.versioned_ingestion import STATE_TABLE, run_versioned_batch

logger = logging.getLogger(__name__)
SOURCE_ORDER = ("shopify", "amazon", "pos")


def _json(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _hash(content):
    return hashlib.sha256(content).hexdigest()


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _bool(value):
    if value not in {"true", "false"}:
        raise ValueError("Runtime boolean settings require literal true or false")
    return value == "true"


def run_path(work_root, batch_date, run_id):
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("An explicit scheduler run ID is required")
    if batch_date is None:
        raise ValueError("An explicit business batch date is required")
    return Path(work_root).resolve() / "runs" / resolve_batch_date(batch_date).isoformat() / _hash(run_id.encode())[:24]


def get_catalog():
    from pyiceberg.catalog import load_catalog
    return load_catalog("cartco-runtime", **{
        "type": "rest", "uri": os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"),
        "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "s3.region": os.environ.get("AWS_REGION", "us-east-1")})


def _load(work_root, batch_date, run_id):
    path = run_path(work_root, batch_date, run_id)
    manifest = _read(path / "run.json")
    if manifest.get("schema_version") != "runtime-run.v1" or manifest.get("run_id") != run_id or manifest.get("batch_date") != resolve_batch_date(batch_date).isoformat() or set(manifest.get("sources", {})) != SOURCES:
        raise ValueError("Frozen runtime manifest differs from run identity")
    for source, hashes in manifest["sources"].items():
        for suffix, name in ((".csv", "csv_sha256"), (".csv.contract.json", "contract_sha256")):
            if _hash((path / "inputs" / (source + suffix)).read_bytes()) != hashes[name]:
                raise ValueError("Frozen runtime input checksum differs")
    return path, manifest


def prepare_run(*, work_root, input_root, batch_date, run_id, mode="delivered", rows_per_source=None,
                allow_empty_init=False, inventory_mode="none"):
    path = run_path(work_root, batch_date, run_id)
    if (path / "run.json").exists():
        _load(work_root, batch_date, run_id)
        return dict(run_path=str(path), batch_date=resolve_batch_date(batch_date).isoformat())
    if mode not in {"delivered", "synthetic"} or inventory_mode not in {"none", "delivered", "synthetic"}:
        raise ValueError("Unsupported runtime source/inventory mode")
    if type(allow_empty_init) is not bool:
        raise ValueError("Empty-state initialization must be an explicit boolean")
    if inventory_mode == "synthetic" and mode != "synthetic":
        raise ValueError("Synthetic inventory requires explicit synthetic source mode")
    if rows_per_source is not None and (mode != "synthetic" or type(rows_per_source) is not int or not 1 <= rows_per_source <= 1000000):
        raise ValueError("Fixture row override requires synthetic mode and 1..1000000 rows")
    day = resolve_batch_date(batch_date).isoformat()
    inputs, sources, contracts = {}, {}, []
    for source in SOURCE_ORDER:
        if mode == "synthetic":
            import importlib
            module = importlib.import_module(f"ingestion.generate_synthetic_{source}_data")
            original_rows = module.N_ROWS
            try:
                if rows_per_source is not None:
                    module.N_ROWS = rows_per_source
                frame = module.generate(day) if source == "pos" else module.generate(day, include_versions=True)
            finally:
                module.N_ROWS = original_rows
            csv_bytes, contract_bytes = frame.to_csv(index=False).encode(), _json(daily_contract(source, day))
        else:
            incoming = Path(input_root) / day
            csv_bytes = (incoming / f"{source}.csv").read_bytes()
            contract_bytes = (incoming / f"{source}.csv.contract.json").read_bytes()
        contract = validate_contract(json.loads(contract_bytes), source=source, batch_date=day)
        contracts.append(contract)
        inputs[f"{source}.csv"], inputs[f"{source}.csv.contract.json"] = csv_bytes, contract_bytes
        sources[source] = dict(csv_sha256=_hash(csv_bytes), contract_sha256=_hash(contract_bytes))
    if len({(timestamp(c["extraction_start"]), timestamp(c["extraction_end"]), c["business_timezone"]) for c in contracts}) != 1:
        raise ValueError("Runtime source contracts have different windows/timezones")
    manifest = dict(schema_version="runtime-run.v1", run_id=run_id, batch_date=day, mode=mode,
        inventory_mode=inventory_mode, allow_empty_init=allow_empty_init, sources=sources,
        inventory_input=str((Path(input_root) / day / "inventory").resolve()) if inventory_mode == "delivered" else None)
    for filename, content in inputs.items():
        _write_once(path / "inputs" / filename, content)
    _write_once(path / "run.json", _json(manifest))
    return dict(run_path=str(path), batch_date=day)


def _receipt(path, source, manifest):
    result = _read(path / f"{source}.result.json")
    receipt_path = Path(result["receipt"])
    if _hash(receipt_path.read_bytes()) != result["receipt_sha256"]:
        raise ValueError("Runtime source receipt checksum differs")
    receipt = _read(receipt_path)
    if receipt.get("status") != "complete" or receipt.get("source") != source or receipt.get("batch_date") != manifest["batch_date"]:
        raise ValueError("Runtime source result is not a completion receipt for this batch")
    archived = receipt_path.parent.parent / "source.csv"
    if _hash(archived.read_bytes()) != manifest["sources"][source]["csv_sha256"]:
        raise ValueError("Runtime receipt belongs to different frozen input bytes")
    if receipt.get("delivery_contract") != _read(path / "inputs" / f"{source}.csv.contract.json"):
        raise ValueError("Runtime receipt belongs to another delivery contract")
    return receipt_path


def ingest_source(*, work_root, batch_date, run_id, source, catalog=None):
    if source not in SOURCES:
        raise ValueError("Unknown runtime source")
    path, manifest = _load(work_root, batch_date, run_id)
    if (path / f"{source}.result.json").exists():
        return dict(receipt=str(_receipt(path, source, manifest)))
    catalog = catalog if catalog is not None else get_catalog()
    result = run_versioned_batch(catalog, path / "inputs" / f"{source}.csv", Path(work_root) / "archive",
        source=source, batch_date=manifest["batch_date"], applied_at=datetime.now(timezone.utc),
        contract=_read(path / "inputs" / f"{source}.csv.contract.json"), initialize_empty=manifest["allow_empty_init"])
    reference = dict(receipt=result["receipt"], receipt_sha256=_hash(Path(result["receipt"]).read_bytes()))
    _write_once(path / f"{source}.result.json", _json(reference))
    return dict(receipt=str(_receipt(path, source, manifest)))


def _inventory(path, mode):
    result = _read(path / "inventory.result.json")
    if mode == "none":
        if result != {"enabled": False}:
            raise ValueError("Unexpected runtime inventory result")
        return None
    if result.get("enabled") is not True or set(result) != {"enabled", "files"} or set(result["files"]) != {"manifest.json", "scope.csv", "products.csv", "allocations.csv", "snapshots.csv"}:
        raise ValueError("Runtime inventory evidence is incomplete")
    bundle = path / "inventory"
    for filename, checksum in result["files"].items():
        if filename not in {"manifest.json", "scope.csv", "products.csv", "allocations.csv", "snapshots.csv"}:
            raise ValueError("Unexpected runtime inventory evidence file")
        if _hash((bundle / filename).read_bytes()) != checksum:
            raise ValueError("Runtime inventory evidence checksum differs")
    return bundle


def prepare_inventory(*, work_root, input_root, batch_date, run_id, catalog=None):
    path, manifest = _load(work_root, batch_date, run_id)
    for source in SOURCE_ORDER:
        _receipt(path, source, manifest)
    if (path / "inventory.result.json").exists():
        return dict(enabled=_inventory(path, manifest["inventory_mode"]) is not None)
    if manifest["inventory_mode"] == "none":
        _write_once(path / "inventory.result.json", _json(dict(enabled=False)))
        return dict(enabled=False)
    catalog = catalog if catalog is not None else get_catalog()
    table = catalog.load_table(STATE_TABLE)
    snapshot = table.current_snapshot()
    snapshot_id = snapshot.snapshot_id if snapshot else None
    kwargs = dict(state_uuid=str(table.metadata.table_uuid), snapshot_id=snapshot_id,
                  business_timezone=table.properties[TIMEZONE_PROPERTY])
    if manifest["inventory_mode"] == "synthetic":
        from scripts.generate_inventory_bundle import generate_bundle
        candidate = path / ("inventory-candidate-" + uuid4().hex[:12])
        day = resolve_batch_date(manifest["batch_date"])
        generate_bundle(table.scan(snapshot_id=snapshot_id).to_arrow(), candidate, **kwargs,
            period_start=(day - timedelta(days=7)).isoformat(), period_end=day.isoformat())
    else:
        candidate = Path(manifest["inventory_input"])
    _, _, contents = read_bundle(candidate, **kwargs, batch_date=manifest["batch_date"])
    for filename, content in contents.items():
        _write_once(path / "inventory" / filename, content)
    _write_once(path / "inventory.result.json", _json(dict(enabled=True, files={name: _hash(data) for name, data in contents.items()})))
    return dict(enabled=_inventory(path, manifest["inventory_mode"]) is not None)


def publish_run(*, work_root, batch_date, run_id, catalog=None):
    path, manifest = _load(work_root, batch_date, run_id)
    receipts = [_receipt(path, source, manifest) for source in SOURCE_ORDER]
    inventory = _inventory(path, manifest["inventory_mode"])
    root = Path(work_root) / "gold"
    identity = _hash((path / "run.json").read_bytes())
    pointer = root / "CURRENT.json"
    if pointer.exists():
        _, current = read_gold_release(root)
        if current.get("publication_id") == identity:
            result = dict(release_id=current["release_id"], state_snapshot_id=current["state_snapshot_id"], mart_rows=current["quality"]["mart_rows"])
            _write_once(path / "published.json", _json(result))
            return result
    if (path / "published.json").exists():
        raise ValueError("A later publication replaced this run; refusing to roll back the Gold pointer")
    intent = path / "publication-intent.json"
    if not intent.exists():
        _write_once(intent, _json(dict(pointer_sha256=_hash(pointer.read_bytes()) if pointer.exists() else None)))
    catalog = catalog if catalog is not None else get_catalog()
    released = publish_gold_release(catalog, root, receipts, batch_date=manifest["batch_date"], inventory_bundle=inventory,
        publication_id=identity, publication_precondition=_read(intent))
    result = dict(release_id=released["release_id"], state_snapshot_id=released["state_snapshot_id"], mart_rows=released["quality"]["mart_rows"])
    _write_once(path / "published.json", _json(result))
    return result


def _settings():
    return dict(work_root=os.environ.get("CARTCO_RUNTIME_WORK_ROOT", "/opt/airflow/data/versioned"),
                input_root=os.environ.get("CARTCO_RUNTIME_INPUT_ROOT", "/opt/airflow/data/deliveries"))


def _logged(stage, batch_date, run_id, action):
    try:
        result = action()
    except Exception:
        logger.exception(json.dumps(dict(event="runtime_stage_failed", stage=stage, batch_date=batch_date, run_id=run_id)))
        raise
    logger.info(json.dumps(dict(event="runtime_stage_complete", stage=stage, batch_date=batch_date, run_id=run_id)))
    return result


def prepare_task(batch_date, run_id):
    rows = os.environ.get("CARTCO_RUNTIME_ROWS_PER_SOURCE")
    return _logged("prepare", batch_date, run_id, lambda: prepare_run(**_settings(), batch_date=batch_date, run_id=run_id,
        mode=os.environ.get("CARTCO_RUNTIME_MODE", "delivered"), rows_per_source=int(rows) if rows else None,
        inventory_mode=os.environ.get("CARTCO_RUNTIME_INVENTORY_MODE", "none"),
        allow_empty_init=_bool(os.environ.get("CARTCO_RUNTIME_ALLOW_EMPTY_INIT", "false"))))


def ingest_task(batch_date, run_id, source):
    return _logged(source, batch_date, run_id, lambda: ingest_source(work_root=_settings()["work_root"], batch_date=batch_date, run_id=run_id, source=source))


def inventory_task(batch_date, run_id):
    return _logged("inventory", batch_date, run_id, lambda: prepare_inventory(**_settings(), batch_date=batch_date, run_id=run_id))


def publish_task(batch_date, run_id):
    return _logged("publish", batch_date, run_id, lambda: publish_run(work_root=_settings()["work_root"], batch_date=batch_date, run_id=run_id))
