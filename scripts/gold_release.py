"""Publish or inspect a validated local Gold release; legacy tables are untouched."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.gold_release import publish_gold_release, read_gold_release


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--catalog-db", required=True, type=Path)
    publish.add_argument("--warehouse", required=True, type=Path)
    publish.add_argument("--output-root", required=True, type=Path)
    publish.add_argument("--receipt", required=True, action="append", type=Path)
    publish.add_argument("--batch-date", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "inspect":
        _, manifest = read_gold_release(args.output_root)
    else:
        try:
            from pyiceberg.catalog.sql import SqlCatalog
        except ImportError:
            parser.error("Use the isolated file-integration dependency environment")
        if not args.catalog_db.is_file():
            parser.error("The source catalog database does not exist")
        catalog = SqlCatalog("cartco-local", uri="sqlite:///" + args.catalog_db.resolve().as_posix(),
                             warehouse="file://" + args.warehouse.resolve().as_posix())
        try:
            manifest = publish_gold_release(catalog, args.output_root, args.receipt, batch_date=args.batch_date)
        finally:
            catalog.engine.dispose()
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
