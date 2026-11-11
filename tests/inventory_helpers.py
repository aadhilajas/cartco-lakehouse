import csv
import hashlib
import io
import json

from order_state_helpers import events, order
from pipeline.inventory import COST_BASIS, PERIOD_BASIS, FILES


def orders():
    return events(order(quantity=2, order_total=20), order(channel="amazon"))


def inputs():
    return {"scope.csv": [dict(sku="S1", location_id="L1", unit_cost="2.00")],
        "products.csv": [dict(channel=source, product_id="p1", sku="S1") for source in ("shopify", "amazon")],
        "allocations.csv": [dict(channel=source, order_id="o1", sku="S1", location_id="L1", quantity=str(quantity),
            unit_cost="2.00", cogs=f"{quantity * 2}.00", cogs_date="2026-01-01") for source, quantity in (("shopify", 2), ("amazon", 1))],
        "snapshots.csv": [dict(sku="S1", location_id="L1", snapshot_date=f"2026-01-0{day}", on_hand=str(units),
            inventory_value=f"{units * 2}.00") for day, units in ((1, 10), (2, 20))]}


def manifest(**changes):
    return dict(schema_version="inventory.v1", cost_basis=COST_BASIS, period_basis=PERIOD_BASIS, currency="INR",
        state_table_uuid="test-uuid", state_snapshot_id=1, business_timezone="UTC", period_start="2026-01-01",
        period_end="2026-01-03", is_synthetic=False, input_provenance="Declared test cost/stock ledger", files={}) | changes


def write_bundle(path, contract, data):
    path.mkdir(parents=True, exist_ok=False)
    hashes = {}
    for filename, fields in FILES.items():
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=sorted(fields), lineterminator="\n")
        writer.writeheader()
        writer.writerows(data[filename])
        content = stream.getvalue().encode()
        (path / filename).write_bytes(content)
        hashes[filename] = hashlib.sha256(content).hexdigest()
    (path / "manifest.json").write_text(json.dumps(contract | {"files": hashes}), encoding="utf-8")
    return path
