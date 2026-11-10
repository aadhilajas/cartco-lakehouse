"""Content-addressed CSV batches: retain original bytes and immutable manifests."""
from __future__ import annotations

from dataclasses import dataclass
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile

from pipeline.batch import resolve_batch_date

SOURCES = {"shopify", "amazon"}


@dataclass(frozen=True)
class RawBatch:
    batch_id: str
    source: str
    batch_date: str
    directory: Path
    columns: tuple[str, ...]
    rows: tuple[dict[str, str], ...]


def _write_once(path: Path, content: bytes):
    """Publish a complete file without replacing another writer's artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        try:
            if os.name == "nt":
                # Windows rename is atomic and refuses an existing destination.
                os.rename(temporary, path)
            else:
                os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise ValueError(f"Immutable batch artifact differs: {path.name}")
    finally:
        if temporary.exists():
            temporary.unlink()


def _rows(content: bytes):
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig"), newline=""), strict=True)
    if not reader.fieldnames or any(not name for name in reader.fieldnames) or len(set(reader.fieldnames)) != len(reader.fieldnames):
        raise ValueError("CSV needs nonempty unique column headers")
    rows = []
    for index, row in enumerate(reader, 2):
        if None in row or any(value is None for value in row.values()):
            raise ValueError(f"CSV record near line {index} has the wrong field count")
        rows.append(row)
    return tuple(reader.fieldnames), tuple(rows)


def archive_csv(csv_path, archive_root, *, source: str, batch_date) -> RawBatch:
    if source not in SOURCES:
        raise ValueError("Unsupported source")
    day = resolve_batch_date(batch_date).isoformat()
    content = Path(csv_path).read_bytes()
    checksum = hashlib.sha256(content).hexdigest()
    batch_id = hashlib.sha256(f"{source}\n{day}\n{checksum}".encode()).hexdigest()
    directory = Path(archive_root).resolve() / source / day / batch_id
    # Archive malformed input too; parsing never destroys original evidence.
    _write_once(directory / "source.csv", content)
    try:
        columns, rows = _rows(content)
    except (ValueError, UnicodeDecodeError, csv.Error) as exc:
        rejected = dict(schema_version=1, source=source, batch_date=day, batch_id=batch_id,
                        sha256=checksum, bytes=len(content), rows=None, parse_error=str(exc))
        _write_once(directory / "manifest.json", (json.dumps(rejected, sort_keys=True, indent=2) + "\n").encode())
        raise
    manifest = dict(schema_version=1, source=source, batch_date=day, batch_id=batch_id,
                    sha256=checksum, bytes=len(content), rows=len(rows), columns=columns)
    _write_once(directory / "manifest.json", (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode())
    return RawBatch(batch_id, source, day, directory, columns, rows)


def record_receipt(batch: RawBatch, report: dict):
    """Attempt receipts are append-only; the input manifest never changes."""
    from uuid import uuid4
    content = dict(report, batch_id=batch.batch_id, source=batch.source, batch_date=batch.batch_date)
    path = batch.directory / "receipts" / f"{uuid4()}.json"
    _write_once(path, (json.dumps(content, sort_keys=True, indent=2) + "\n").encode())
    return path
