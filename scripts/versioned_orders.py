"""Run or migrate versioned orders using an explicitly selected local catalog."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.state_migration import install_candidate
from pipeline.versioned_ingestion import run_versioned_batch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-db", required=True, type=Path)
    parser.add_argument("--warehouse", required=True, type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--source", choices=["shopify", "amazon"], required=True)
    run.add_argument("--csv", type=Path, required=True)
    run.add_argument("--archive-root", type=Path, required=True)
    run.add_argument("--batch-date", required=True)
    run.add_argument("--initialize-empty-state", action="store_true")
    migrate = commands.add_parser("migrate")
    migrate.add_argument("--candidate", type=Path, required=True)
    args = parser.parse_args()
    try:
        from pyiceberg.catalog.sql import SqlCatalog
    except ImportError:
        parser.error("Use the isolated requirements/file-integration.txt environment")
    database = args.catalog_db.resolve()
    database.parent.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog("cartco-local", uri="sqlite:///" + database.as_posix(),
                         warehouse="file://" + args.warehouse.resolve().as_posix())
    try:
        if args.command == "migrate":
            result = install_candidate(catalog, args.candidate)
        else:
            result = run_versioned_batch(catalog, args.csv, args.archive_root, source=args.source,
                batch_date=args.batch_date, applied_at=datetime.now(timezone.utc),
                initialize_empty=args.initialize_empty_state)
        print(json.dumps(result, indent=2))
    finally:
        catalog.engine.dispose()


if __name__ == "__main__":
    main()
