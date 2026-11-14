"""Create explicitly SYNTHETIC standard-cost inventory inputs for a pinned state."""

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
import csv
from datetime import timedelta
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import sys
from uuid import NAMESPACE_URL, uuid5

sys.path.insert(0, str(_REPOSITORY_ROOT))
from pipeline.batch import resolve_batch_date
from pipeline.commerce_metrics import validated_orders
from pipeline.inventory import COST_BASIS, PERIOD_BASIS, FILES, inventory_date
from pipeline.raw_batches import _write_once
from pipeline.source_contracts import TIMEZONE_PROPERTY
from pipeline.versioned_ingestion import STATE_TABLE


def generate_bundle(orders, output, *, state_uuid, snapshot_id, business_timezone, period_start, period_end):
    start, end = (inventory_date(value) for value in (period_start, period_end))
    if not 1 <= (end - start).days <= 3660:
        raise ValueError("Invalid inventory fixture period")
    rows = [row for row in validated_orders(orders).to_pylist()
            if row["order_status"] == "completed" and start <= row["order_date"] < end]
    mappings, scope, allocations = {}, {}, []
    for row in rows:
        key = (row["channel"], row["product_id"])
        sku = "SKU-" + str(uuid5(NAMESPACE_URL, json.dumps(key)))
        pool = (sku, "SYNTHETIC-DC")
        unit_cost = Decimal(100 + int(hashlib.sha256(sku.encode()).hexdigest()[:8], 16) % 100000) / 100
        mappings[key] = dict(channel=key[0], product_id=key[1], sku=sku)
        scope[pool] = dict(sku=sku, location_id=pool[1], unit_cost=f"{unit_cost:.2f}")
        allocations.append(dict(channel=row["channel"], order_id=row["order_id"], sku=sku, location_id=pool[1],
            quantity=str(row["quantity"]), unit_cost=f"{unit_cost:.2f}", cogs=f"{unit_cost * row['quantity']:.2f}",
            cogs_date=row["order_date"].isoformat()))
    scope[("DEMO-UNSOLD", "SYNTHETIC-DC")] = dict(sku="DEMO-UNSOLD", location_id="SYNTHETIC-DC", unit_cost="10.00")
    snapshots = []
    for offset in range((end - start).days):
        day = start + timedelta(days=offset)
        for pool, cost in sorted(scope.items()):
            units = 20 + int(hashlib.sha256(f"{pool[0]}/{day}".encode()).hexdigest()[:8], 16) % 80
            snapshots.append(dict(sku=pool[0], location_id=pool[1], snapshot_date=day.isoformat(), on_hand=str(units),
                                  inventory_value=f"{Decimal(cost['unit_cost']) * units:.2f}"))
    inputs = {"scope.csv": [scope[key] for key in sorted(scope)], "products.csv": [mappings[key] for key in sorted(mappings)],
              "allocations.csv": sorted(allocations, key=lambda row: (row["channel"], row["order_id"])), "snapshots.csv": snapshots}
    contents = {}
    for filename, records in inputs.items():
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=sorted(FILES[filename]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(records)
        contents[filename] = stream.getvalue().encode()
    manifest = dict(schema_version="inventory.v1", cost_basis=COST_BASIS, period_basis=PERIOD_BASIS, currency="INR",
        state_table_uuid=state_uuid, state_snapshot_id=snapshot_id, business_timezone=business_timezone,
        period_start=start.isoformat(), period_end=end.isoformat(), is_synthetic=True,
        input_provenance="Deterministic CartCo fixture; invented costs, stock and product/location mappings",
        files={name: hashlib.sha256(content).hexdigest() for name, content in contents.items()})
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    for filename, content in contents.items():
        _write_once(output / filename, content)
    _write_once(output / "manifest.json", (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode())
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-db", required=True, type=Path)
    parser.add_argument("--warehouse", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path, help="Must not exist")
    parser.add_argument("--period-start", required=True)
    parser.add_argument("--period-end", required=True, help="Exclusive business calendar date")
    args = parser.parse_args()
    if not args.catalog_db.is_file():
        parser.error("Source catalog does not exist")
    from pyiceberg.catalog.sql import SqlCatalog
    catalog = SqlCatalog("cartco-local", uri="sqlite:///" + args.catalog_db.resolve().as_posix(),
                         warehouse="file://" + args.warehouse.resolve().as_posix())
    try:
        table = catalog.load_table(STATE_TABLE)
        if table.properties.get("cartco.state.status") != "ready" or TIMEZONE_PROPERTY not in table.properties:
            raise ValueError("State must be ready and declare its business timezone")
        snapshot = table.current_snapshot()
        snapshot_id = snapshot.snapshot_id if snapshot else None
        manifest = generate_bundle(table.scan(snapshot_id=snapshot_id).to_arrow(), args.output,
            state_uuid=str(table.metadata.table_uuid), snapshot_id=snapshot_id, business_timezone=table.properties[TIMEZONE_PROPERTY],
            period_start=args.period_start, period_end=args.period_end)
        print(json.dumps(manifest, indent=2))
    finally:
        catalog.engine.dispose()


if __name__ == "__main__":
    main()
