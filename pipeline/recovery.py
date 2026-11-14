"""Quiesced local SQLite/file-warehouse backup and original-path, no-overwrite restore.

REST/object-store recovery requires its own live-stack procedure. This module
never guesses how to relocate Iceberg's absolute metadata/manifest paths.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
from urllib.parse import unquote, urlsplit

from pyiceberg.table import StaticTable

from pipeline.gold_release import read_gold_release
from pipeline.raw_batches import _write_once


def _json(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _hash(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def _layout(database, warehouse, runtime):
    paths = dict(database=Path(database).resolve(), warehouse=Path(warehouse).resolve(), runtime=Path(runtime).resolve())
    if any(path == Path(path.anchor) for path in paths.values()):
        raise ValueError("Recovery targets cannot be filesystem roots")
    if any(left == right or left.is_relative_to(right) or right.is_relative_to(left)
           for index, left in enumerate(paths.values()) for right in list(paths.values())[index + 1:]):
        raise ValueError("Catalog, warehouse and runtime targets must not overlap")
    return paths


def _local_file(uri, warehouse):
    parsed = urlsplit(uri)
    if parsed.scheme == "file":
        raw = unquote(parsed.netloc + parsed.path)
        if os.name == "nt" and re.match(r"^/[A-Za-z]:/", raw):
            raw = raw[1:]
    elif not parsed.scheme:
        raw = uri
    else:
        raise ValueError("Local recovery refuses remote/object-store Iceberg locations")
    if not Path(raw).is_absolute():
        raise ValueError("Iceberg recovery requires absolute local file locations")
    path = Path(raw).resolve()
    if not path.is_relative_to(warehouse) or not path.exists():
        raise ValueError("Iceberg references a missing file or data outside the backed-up warehouse")
    return path


def _inventory(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("Recovery refuses symlinks in source/payload trees")
        if path.name == ".publication.lock":
            raise ValueError("Publication lock exists; stop writers and investigate before backup")
        if path.is_file():
            result[path.relative_to(root).as_posix()] = dict(bytes=path.stat().st_size, sha256=_hash(path))
        elif not path.is_dir():
            raise ValueError("Recovery supports regular files/directories only")
    return result


def audit(database, warehouse, runtime, *, catalog_name):
    """Hash current keyed content and validate retained Iceberg reference coverage."""
    warehouse, runtime = Path(warehouse).resolve(), Path(runtime).resolve()
    connection = sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        names = {row[0] for row in connection.execute("SELECT DISTINCT catalog_name FROM iceberg_tables")}
        if names != {catalog_name}:
            raise ValueError("Backup requires the exact single populated SQLite catalog name")
        records = connection.execute("SELECT table_namespace, table_name, metadata_location FROM iceberg_tables WHERE catalog_name=? ORDER BY table_namespace, table_name", (catalog_name,)).fetchall()
    finally:
        connection.close()
    tables = {}
    for namespace, name, location in records:
        _local_file(location, warehouse)
        table = StaticTable.from_metadata(location)
        if not _local_file(table.location(), warehouse).is_dir():
            raise ValueError("Table location is not inside the backed-up warehouse")
        for old in table.metadata.metadata_log:
            _local_file(old.metadata_file, warehouse)
        for snapshot in table.metadata.snapshots:
            if snapshot.manifest_list:
                _local_file(snapshot.manifest_list, warehouse)
            for manifest in snapshot.manifests(table.io):
                _local_file(manifest.manifest_path, warehouse)
                for entry in manifest.fetch_manifest_entry(table.io):
                    _local_file(entry.data_file.file_path, warehouse)
        rows = table.scan().to_arrow().to_pylist()
        digests = sorted(hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).digest() for row in rows)
        snapshot = table.current_snapshot()
        tables[f"{namespace}.{name}"] = dict(rows=len(rows), row_multiset_sha256=hashlib.sha256(b"".join(digests)).hexdigest(),
            table_uuid=str(table.metadata.table_uuid), schema=json.loads(table.schema().model_dump_json()), current_snapshot_id=snapshot.snapshot_id if snapshot else None,
            snapshots=[snapshot.snapshot_id for snapshot in table.metadata.snapshots])
    gold = None
    if (runtime / "gold/CURRENT.json").exists():
        _, manifest = read_gold_release(runtime / "gold")
        gold = dict(release_id=manifest["release_id"], state_snapshot_id=manifest["state_snapshot_id"], mart_rows=manifest["quality"]["mart_rows"])
    return dict(catalog_name=catalog_name, tables=tables, gold=gold)


def create_backup(database, warehouse, runtime, destination, *, catalog_name, writers_stopped=False):
    if writers_stopped is not True:
        raise ValueError("Stop all catalog/runtime writers and explicitly acknowledge quiescence")
    paths = _layout(database, warehouse, runtime)
    destination = Path(destination).resolve()
    if destination.exists() or any(destination == path or destination.is_relative_to(path) for path in paths.values()):
        raise ValueError("Backup destination must be new and outside source targets")
    if not paths["database"].is_file() or not all(paths[key].is_dir() for key in ("warehouse", "runtime")):
        raise ValueError("Recovery source catalog/warehouse/runtime is missing")
    guard = sqlite3.connect(paths["database"], timeout=5)
    try:
        # Prevent cooperative SQLite catalog commits while copying data files.
        # The operator's quiescence precondition also covers filesystem writers.
        guard.execute("BEGIN IMMEDIATE")
        before = audit(**paths, catalog_name=catalog_name)
        source_files = {key: _inventory(paths[key]) for key in ("warehouse", "runtime")}
        destination.mkdir(parents=True, exist_ok=False)
        payload = destination / "payload"
        payload.mkdir()
        source = sqlite3.connect(paths["database"].as_uri() + "?mode=ro", uri=True)
        target = sqlite3.connect(payload / "catalog.sqlite")
        try:
            source.backup(target)
        finally:
            source.close()
            target.close()
        files = {"catalog.sqlite": dict(bytes=(payload / "catalog.sqlite").stat().st_size, sha256=_hash(payload / "catalog.sqlite"))}
        for component, inventory in source_files.items():
            for relative, expected in inventory.items():
                target_file = payload / component / relative
                target_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(paths[component] / relative, target_file)
                if _hash(target_file) != expected["sha256"]:
                    raise ValueError("Source changed during backup copy")
                files[f"{component}/{relative}"] = expected
        if source_files != {key: _inventory(paths[key]) for key in source_files} or before != audit(**paths, catalog_name=catalog_name):
            raise ValueError("Source changed during backup; incomplete candidate retained")
        manifest = dict(schema_version="cartco-local-backup.v1", created_at=datetime.now(timezone.utc).isoformat(),
            original_paths={key: str(path) for key, path in paths.items()}, catalog_name=catalog_name,
            files=files, audit=before, scope="SQLite catalog, local file warehouse and runtime tree; original-path restore only")
        _write_once(destination / "manifest.json", _json(manifest))
        verify_backup(destination)
        return manifest
    finally:
        guard.rollback()
        guard.close()


def verify_backup(backup):
    backup = Path(backup).resolve()
    if not (backup / "manifest.json").is_file():
        raise ValueError("Backup is incomplete: no final manifest")
    manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "cartco-local-backup.v1" or "catalog.sqlite" not in manifest.get("files", {}):
        raise ValueError("Unsupported or incomplete backup manifest")
    for name, expected in manifest["files"].items():
        relative = PurePosixPath(name)
        if relative.is_absolute() or relative.as_posix() != name or ".." in relative.parts or ":" in name or "\\" in name or (name != "catalog.sqlite" and (len(relative.parts) < 2 or relative.parts[0] not in {"warehouse", "runtime"})):
            raise ValueError("Unsafe backup payload path")
        path = backup / "payload" / name
        if path.is_symlink() or not path.resolve().is_relative_to((backup / "payload").resolve()) or not path.is_file() or path.stat().st_size != expected["bytes"] or _hash(path) != expected["sha256"]:
            raise ValueError("Backup payload checksum/size differs")
    if (backup / "payload").is_symlink():
        raise ValueError("Recovery refuses a symlink payload root")
    actual = _inventory(backup / "payload")
    if actual != manifest["files"]:
        raise ValueError("Backup has unlisted/missing payload files")
    return manifest


def restore_backup(backup, database, warehouse, runtime, *, writers_stopped=False):
    if writers_stopped is not True:
        raise ValueError("Stop all writers before restoring")
    manifest = verify_backup(backup)
    paths = _layout(database, warehouse, runtime)
    if paths != {key: Path(value).resolve() for key, value in manifest["original_paths"].items()}:
        raise ValueError("Restore requires the explicit original paths; automatic relocation is unsupported")
    if any(path.exists() for path in paths.values()):
        raise ValueError("Restore refuses existing targets; preserve them separately before recovery")
    for key in ("warehouse", "runtime"):
        paths[key].mkdir(parents=True, exist_ok=False)
    paths["database"].parent.mkdir(parents=True, exist_ok=True)
    backup = Path(backup).resolve()
    for name in manifest["files"]:
        if name == "catalog.sqlite":
            destination = paths["database"]
        else:
            component, relative = name.split("/", 1)
            destination = paths[component] / relative
        if destination.exists():
            raise ValueError("Restore encountered an existing destination; no overwrite permitted")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(backup / "payload" / name, destination)
    restored = audit(**paths, catalog_name=manifest["catalog_name"])
    if restored != manifest["audit"]:
        raise RuntimeError("Restored catalog/data audit differs; do not restart writers")
    return restored
