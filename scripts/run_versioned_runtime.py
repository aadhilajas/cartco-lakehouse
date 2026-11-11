"""Run the exact DAG callbacks sequentially; SQLite is an isolated local test option."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline import runtime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--input-root", required=True, type=Path)
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--mode", choices=["delivered", "synthetic"], default="delivered")
    parser.add_argument("--inventory-mode", choices=["none", "delivered", "synthetic"], default="none")
    parser.add_argument("--rows-per-source", type=int)
    parser.add_argument("--allow-empty-init", action="store_true")
    parser.add_argument("--local-catalog-db", type=Path)
    parser.add_argument("--local-warehouse", type=Path)
    args = parser.parse_args()
    if bool(args.local_catalog_db) != bool(args.local_warehouse):
        parser.error("Local catalog database and warehouse must be supplied together")
    if args.local_catalog_db:
        from pyiceberg.catalog.sql import SqlCatalog
        args.local_catalog_db.parent.mkdir(parents=True, exist_ok=True)
        catalog = SqlCatalog("cartco-local", uri="sqlite:///" + args.local_catalog_db.resolve().as_posix(),
                             warehouse="file://" + args.local_warehouse.resolve().as_posix())
    else:
        catalog = runtime.get_catalog()
    context = dict(work_root=args.work_root, batch_date=args.batch_date, run_id=args.run_id)
    try:
        runtime.prepare_run(**context, input_root=args.input_root, mode=args.mode, inventory_mode=args.inventory_mode,
                            rows_per_source=args.rows_per_source, allow_empty_init=args.allow_empty_init)
        for source in runtime.SOURCE_ORDER:
            runtime.ingest_source(**context, source=source, catalog=catalog)
        runtime.prepare_inventory(**context, input_root=args.input_root, catalog=catalog)
        result = runtime.publish_run(**context, catalog=catalog)
        print(json.dumps(result, indent=2))
    finally:
        if hasattr(catalog, "engine"):
            catalog.engine.dispose()


if __name__ == "__main__":
    main()
