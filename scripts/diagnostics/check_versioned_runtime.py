"""Live CI acceptance for the versioned DAG; run inside its actual Airflow stack."""

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
from collections import Counter
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(_REPOSITORY_ROOT))
from pipeline.gold_release import read_gold_release
from pipeline.runtime import get_catalog, SOURCE_ORDER
from pipeline.versioned_ingestion import STATE_TABLE


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows-per-source", required=True, type=int)
    args = parser.parse_args()
    root = Path(os.environ.get("CARTCO_RUNTIME_WORK_ROOT", "/opt/airflow/data/versioned"))
    tables, manifest = read_gold_release(root / "gold")
    catalog = get_catalog()
    state = catalog.load_table(STATE_TABLE)
    data = state.scan().to_arrow()
    expected = {source: args.rows_per_source for source in SOURCE_ORDER}
    if dict(Counter(data["channel"].to_pylist())) != expected or manifest["required_sources"] != sorted(SOURCE_ORDER):
        raise RuntimeError("Versioned runtime source coverage differs")
    if len(state.metadata.snapshots) != 3 or state.current_snapshot().snapshot_id != manifest["state_snapshot_id"]:
        raise RuntimeError("Replay changed state or publication did not pin the current state")
    if "inventory_turnover" not in tables or not manifest["inventory"]["contract"]["is_synthetic"]:
        raise RuntimeError("CI runtime did not publish its explicitly synthetic inventory evidence")
    bronze = {}
    for source in SOURCE_ORDER:
        table = catalog.load_table(f"bronze.{source}_order_events")
        if table.scan().to_arrow().num_rows != args.rows_per_source or len(table.metadata.snapshots) != 1:
            raise RuntimeError("Raw source replay duplicated input or changed its snapshot")
        bronze[source] = dict(rows=args.rows_per_source, snapshot_id=table.current_snapshot().snapshot_id)
    print(json.dumps(dict(channels=expected, state_snapshot_id=manifest["state_snapshot_id"], state_snapshots=3,
        bronze=bronze, release_id=manifest["release_id"], mart_rows=manifest["quality"]["mart_rows"],
        evidence="Live REST/MinIO catalog and shared filesystem, after actual Airflow DAG execution"), indent=2))


if __name__ == "__main__":
    main()
