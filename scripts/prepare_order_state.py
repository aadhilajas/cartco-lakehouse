"""Prepare a reviewable migration candidate; never modify a catalog or source file."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.order_state import active_orders, initialize_legacy_state


def prepare_candidate(legacy_path: Path, versions_path: Path, output_path: Path, *, applied_at: datetime):
    legacy_path, versions_path, output_path = (Path(p).resolve() for p in (legacy_path, versions_path, output_path))
    manifest_path = output_path.with_suffix(output_path.suffix + ".manifest.json")
    if output_path in {legacy_path, versions_path} or manifest_path in {legacy_path, versions_path}:
        raise ValueError("Migration output must differ from its inputs")
    if output_path.exists() or manifest_path.exists():
        raise FileExistsError("Migration output already exists; refusing to overwrite it")
    if applied_at.tzinfo is None or applied_at.utcoffset() is None:
        raise ValueError("Migration audit time must include a timezone")
    legacy_bytes, version_bytes = legacy_path.read_bytes(), versions_path.read_bytes()
    legacy = pq.read_table(pa.BufferReader(legacy_bytes))
    versions = pq.read_table(pa.BufferReader(version_bytes))
    candidate = initialize_legacy_state(legacy, versions, applied_at=applied_at)
    buffer = pa.BufferOutputStream()
    pq.write_table(candidate, buffer)
    output_bytes = buffer.getvalue().to_pybytes()
    # Validate serialization before creating either user-facing artifact.
    if not pq.read_table(pa.BufferReader(output_bytes)).equals(candidate):
        raise RuntimeError("Migration candidate failed serialization verification")
    report = {
        "kind": "prepared-order-state-migration", "published": False,
        "legacy_rows": legacy.num_rows, "state_rows": candidate.num_rows,
        "active_rows": active_orders(candidate).num_rows,
        "applied_at": applied_at.astimezone(timezone.utc).isoformat(),
        "legacy_sha256": hashlib.sha256(legacy_bytes).hexdigest(),
        "version_map_sha256": hashlib.sha256(version_bytes).hexdigest(),
        "candidate_sha256": hashlib.sha256(output_bytes).hexdigest(),
        "version_authority": "Caller-supplied baseline; source truth must be verified externally",
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("xb") as stream:
        stream.write(output_bytes)
    with manifest_path.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy", required=True, type=Path)
    parser.add_argument("--versions", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--applied-at", required=True, help="ISO timestamp with UTC offset; audit time only")
    args = parser.parse_args()
    try:
        audit = datetime.fromisoformat(args.applied_at.replace("Z", "+00:00"))
        report = prepare_candidate(args.legacy, args.versions, args.output, applied_at=audit)
    except (ValueError, FileExistsError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
