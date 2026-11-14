"""Summarize sampled Docker usage; never add unrelated per-container peaks."""

# Support direct CLI execution and imports from any working directory.
import sys as _sys
from pathlib import Path as _Path
_SCRIPT_PATH = _Path(__file__).resolve()
_REPOSITORY_ROOT = next(
    (parent for parent in _SCRIPT_PATH.parents
     if (parent / "pipeline/source_identity.py").is_file()
     and (parent / "scripts/layout.json").is_file()),
    _SCRIPT_PATH.parent,  # Individually copied stdlib tools do not need a repo.
)
if str(_REPOSITORY_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPOSITORY_ROOT))
import argparse
from collections import defaultdict
import json
from pathlib import Path
import re


def memory_bytes(value):
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(B|kB|KB|MB|GB|TB|KiB|MiB|GiB|TiB)\s*", value)
    if not match:
        raise ValueError(f"Invalid Docker memory quantity: {value}")
    units = {"B": 1, "kB": 1000, "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4,
             "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    return round(float(match[1]) * units[match[2]])


def summarize(lines):
    snapshots = defaultdict(dict)
    per_container = {}
    count = 0
    for line in lines:
        if not line.strip():
            continue
        record = json.loads(line)
        stats = record["stats"]
        name = stats["Name"]
        if not name.startswith("cartco-infra-p1-"):
            raise ValueError("Unexpected container in infrastructure sample")
        used = memory_bytes(stats["MemUsage"].split("/")[0])
        snapshots[record["observed_at"]][name] = used
        per_container[name] = max(per_container.get(name, 0), used)
        count += 1
    if not snapshots or not any("spark-smoke" in name for name in per_container):
        raise ValueError("Samples must include the running Spark acceptance job")
    return {
        "scope": "Observed Docker CLI memory samples; excludes host OS and unsampled peaks, not guaranteed VM sizing",
        "records": count,
        "sample_times": len(snapshots),
        "observed_peak_stack_bytes": max(sum(containers.values()) for containers in snapshots.values()),
        "observed_peak_per_container_bytes": per_container,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = summarize(args.input.read_text(encoding="utf-8-sig").splitlines())
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
