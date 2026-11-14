"""Privacy-safe dashboard export from one verified release, never mixed marts."""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import os
from pathlib import Path
import tempfile

from pipeline.assessed_release import read_release
from pipeline.assessed_lineage import mappings


def snapshot(root, *, lineage_report=None):
    manifest, candidate = read_release(root)
    lineage = []
    for name, facet in mappings(candidate).items():
        for column, field in facet.fields.items():
            lineage.append({"output": name + "." + column, "inputs": sorted({item.name.split(".")[-1] + "." + item.field for item in field.inputFields}), "description": field.transformationDescription})
    tables = {name: identity for name, identity in candidate["catalog"].items() if name != "gold"}
    tables.update(candidate["catalog"]["gold"])
    tables = {name: {**value, "snapshot_id": str(value["snapshot_id"]) if value["snapshot_id"] is not None else None} for name, value in tables.items()}
    # Public snapshot contains no raw payloads, customer IDs, auth headers or local paths.
    ge = manifest["quality"]["great_expectations"]
    transport = {"status": "unverified"}
    if lineage_report:
        report = json.loads(Path(lineage_report).read_text(encoding="utf-8"))
        if report.get("release_id") == candidate["release_id"] and report.get("status") == "verified":
            transport = {"status": "verified", "verified_at": report["verified_at"], "fields_verified": sum(row["fields_verified"] for row in report["tables"].values())}
    result = {"schema_version": 1, "is_synthetic": True, "release_id": manifest["release_id"], "batch_date": manifest["batch_date"], "computed_at": manifest["computed_at"],
        "exported_at": datetime.now(timezone.utc).isoformat(), "compute": candidate["compute_engine"],
        "execution": candidate.get("execution", {"mode": "Local Spark batch", "duration_seconds": None}),
        "daily_revenue": candidate["marts"]["daily_revenue"], "channels": candidate["marts"]["channel_performance"],
        "inventory": candidate["marts"]["inventory_turnover"], "inventory_period": candidate["inventory_period"],
        "quality": {"success": ge["success"], "engine": ge["engine"], "engine_version": ge["engine_version"], "suites": [{"name": name, "success": suite["success"], "rows": suite["rows"], "checks": len(suite["checks"])} for name, suite in ge["suites"].items()]},
        "catalog": tables, "lineage": lineage, "lineage_transport": transport}
    for row in result["inventory"]:
        for column in ["average_daily_inventory_value", "turnover"]:
            if row[column] is not None:
                row[column] = format(Decimal(row[column]), "f")
    result["source_bundle_sha256"] = candidate.get("source_bundle_sha256")
    # Keep all exact decimal input strings. UI rounds display values without
    # changing computation or turning missing/zero values into one another.
    return result


def export(root, destination, *, lineage_report=None):
    result = snapshot(root, lineage_report=lineage_report)
    if not result["quality"]["success"]:
        raise ValueError("Refusing failed-quality dashboard snapshot")
    content = (json.dumps(result, indent=2, default=str) + "\n").encode()
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target
