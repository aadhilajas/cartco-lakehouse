"""Fixed-standard-cost turnover from explicitly scoped, reconciled inventory inputs."""
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP, localcontext
import hashlib
import json
from pathlib import Path
import re

import pyarrow as pa

from pipeline.batch import resolve_batch_date
from pipeline.commerce_metrics import validated_orders
from pipeline.money import exact_money
from pipeline.raw_batches import _rows
from pipeline.source_contracts import zone

COST_BASIS = "fixed-standard-cost-v1"
PERIOD_BASIS = "order_date_latest_state"
FILES = {"scope.csv": {"sku", "location_id", "unit_cost"},
         "products.csv": {"channel", "product_id", "sku"},
         "allocations.csv": {"channel", "order_id", "sku", "location_id", "quantity", "unit_cost", "cogs", "cogs_date"},
         "snapshots.csv": {"sku", "location_id", "snapshot_date", "on_hand", "inventory_value"}}
MANIFEST_FIELDS = {"schema_version", "cost_basis", "period_basis", "currency", "business_timezone",
                   "state_table_uuid", "state_snapshot_id", "period_start", "period_end", "files", "is_synthetic", "input_provenance"}
SCHEMA = pa.schema([
    ("sku", pa.string()), ("location_id", pa.string()), ("period_start", pa.date32()), ("period_end", pa.date32()),
    ("currency", pa.string()), ("cost_basis", pa.string()), ("period_basis", pa.string()),
    ("is_synthetic", pa.bool_()),
    ("cogs", pa.decimal128(18, 2)), ("inventory_value_days", pa.decimal128(24, 2)),
    ("observed_days", pa.int32()), ("expected_days", pa.int32()),
    ("average_daily_inventory_value", pa.decimal128(28, 10)), ("turnover", pa.decimal128(38, 12)),
    ("quality_status", pa.string())])


def inventory_date(value):
    if not isinstance(value, str):
        raise ValueError("Inventory dates must explicitly use YYYY-MM-DD")
    return resolve_batch_date(value)


def _key(row, fields):
    result = tuple(row[name] for name in fields)
    if any(not isinstance(value, str) or not value or value.strip() != value for value in result):
        raise ValueError("Inventory keys must be nonempty canonical strings")
    return result


def _count(value):
    if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]*", value) or int(value) > 2147483647:
        raise ValueError("Inventory quantities must be canonical nonnegative integers")
    return int(value)


def _unique(rows, fields):
    result = {}
    for row in rows:
        key = _key(row, fields)
        if key in result:
            raise ValueError("Duplicate inventory input keys")
        result[key] = row
    return result


def read_bundle(path, *, state_uuid, snapshot_id, business_timezone, batch_date):
    """Read source bytes once, verify checksums and pin the exact state/period."""
    path = Path(path)
    content = {"manifest.json": (path / "manifest.json").read_bytes()}
    manifest = json.loads(content["manifest.json"])
    if not isinstance(manifest, dict) or set(manifest) != MANIFEST_FIELDS:
        raise ValueError("Inventory manifest has missing or unexpected fields")
    if manifest["schema_version"] != "inventory.v1" or manifest["cost_basis"] != COST_BASIS or manifest["period_basis"] != PERIOD_BASIS or manifest["currency"] != "INR":
        raise ValueError("Unsupported inventory schema, currency or cost/period basis")
    if type(manifest["is_synthetic"]) is not bool or not isinstance(manifest["input_provenance"], str) or not manifest["input_provenance"].strip():
        raise ValueError("Inventory must declare synthetic/external provenance")
    if manifest["state_snapshot_id"] is not None and (type(manifest["state_snapshot_id"]) is not int or manifest["state_snapshot_id"] <= 0):
        raise ValueError("Inventory snapshot ID must be an integer or null")
    zone(manifest["business_timezone"])
    if (manifest["state_table_uuid"], manifest["state_snapshot_id"], manifest["business_timezone"]) != (state_uuid, snapshot_id, business_timezone):
        raise ValueError("Inventory bundle belongs to another state snapshot or business timezone")
    start, end = (inventory_date(manifest[name]) for name in ("period_start", "period_end"))
    if not 1 <= (end - start).days <= 3660 or end > resolve_batch_date(batch_date) + timedelta(days=1):
        raise ValueError("Inventory reporting period is invalid or extends past the business batch day")
    if not isinstance(manifest["files"], dict) or set(manifest["files"]) != set(FILES):
        raise ValueError("Inventory manifest file inventory differs")
    rows = {}
    for filename, columns in FILES.items():
        data = content[filename] = (path / filename).read_bytes()
        if hashlib.sha256(data).hexdigest() != manifest["files"][filename]:
            raise ValueError("Inventory input checksum differs")
        headers, records = _rows(data)
        if set(headers) != columns:
            raise ValueError(f"Inventory schema differs: {filename}")
        rows[filename] = records
    return manifest, rows, content


def build_turnover(orders, manifest, inputs):
    """Reconcile all completed orders in the declared order-date reporting period.

    One allocation per single-product order; inventory pools are not duplicated
    across channels. Unsold, explicitly scoped pools still enter the output.
    """
    start, end = (inventory_date(manifest[name]) for name in ("period_start", "period_end"))
    days = (end - start).days
    if not 1 <= days <= 3660:
        raise ValueError("Invalid inventory reporting period")
    scope = _unique(inputs["scope.csv"], ("sku", "location_id"))
    if not scope:
        raise ValueError("Inventory scope must explicitly declare at least one SKU/location")
    costs = {key: exact_money(row["unit_cost"]) for key, row in scope.items()}
    products = _unique(inputs["products.csv"], ("channel", "product_id"))
    declared_skus = {key[0] for key in scope}
    for row in products.values():
        _key(row, ("sku",))
        if row["sku"] not in declared_skus:
            raise ValueError("Product mapping refers to an undeclared inventory SKU")
    active = validated_orders(orders).to_pylist()
    if any(row["order_status"] == "unknown" for row in active):
        raise ValueError("Unknown status blocks inventory reconciliation")
    expected = {(row["channel"], row["order_id"]): row for row in active
                if row["order_status"] == "completed" and start <= row["order_date"] < end}
    allocations = _unique(inputs["allocations.csv"], ("channel", "order_id"))
    if set(allocations) != set(expected):
        raise ValueError("COGS allocations must exactly cover completed orders in the period")
    cogs = defaultdict(lambda: Decimal("0.00"))
    for key, row in allocations.items():
        order = expected[key]
        pool = _key(row, ("sku", "location_id"))
        mapping = products.get((order["channel"], order["product_id"]))
        if pool not in scope or mapping is None or mapping["sku"] != row["sku"]:
            raise ValueError("COGS SKU/location differs from product mapping or scope")
        if inventory_date(row["cogs_date"]) != order["order_date"] or _count(row["quantity"]) != order["quantity"]:
            raise ValueError("COGS date/quantity differs from the pinned order")
        if exact_money(row["unit_cost"]) != costs[pool]:
            raise ValueError("COGS cost differs from the fixed standard-cost basis")
        value = exact_money(row["cogs"])
        if value != exact_money(costs[pool] * order["quantity"]):
            raise ValueError("COGS amount does not reconcile")
        cogs[pool] += value
    snapshots = _unique(inputs["snapshots.csv"], ("sku", "location_id", "snapshot_date"))
    observed = defaultdict(dict)
    for row in snapshots.values():
        pool = _key(row, ("sku", "location_id"))
        day = inventory_date(row["snapshot_date"])
        if pool not in scope or not start <= day < end:
            raise ValueError("Inventory snapshot lies outside declared pool/date coverage")
        value = exact_money(row["inventory_value"])
        if value != exact_money(_count(row["on_hand"]) * costs[pool]):
            raise ValueError("Inventory valuation differs from the fixed standard-cost basis")
        observed[pool][day] = value
    output = []
    for pool in sorted(scope):
        total = sum(observed[pool].values(), Decimal("0.00"))
        numerator = exact_money(cogs[pool])
        status, average, ratio = "complete", None, None
        if len(observed[pool]) != days:
            status = "incomplete_daily_coverage"
        else:
            with localcontext() as context:
                context.prec = 50
                average = (total / days).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
                if total == 0:
                    status = "zero_inventory_value"
                else:
                    ratio = (numerator * days / total).quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP)
        output.append(dict(sku=pool[0], location_id=pool[1], period_start=start, period_end=end,
            currency="INR", cost_basis=COST_BASIS, period_basis=PERIOD_BASIS, is_synthetic=manifest["is_synthetic"], cogs=numerator, inventory_value_days=total,
            observed_days=len(observed[pool]), expected_days=days, average_daily_inventory_value=average,
            turnover=ratio, quality_status=status))
    table = pa.Table.from_pylist(output, schema=SCHEMA)
    report = dict(pools=len(output), allocated_orders=len(allocations), cogs=str(sum(cogs.values(), Decimal("0.00"))),
                  incomplete_pools=sum(row["quality_status"] == "incomplete_daily_coverage" for row in output),
                  zero_inventory_pools=sum(row["quality_status"] == "zero_inventory_value" for row in output),
                  period_start=start.isoformat(), period_end=end.isoformat(), cost_basis=COST_BASIS, period_basis=PERIOD_BASIS, is_synthetic=manifest["is_synthetic"])
    return table, report


def validate_turnover(table, inputs, manifest):
    """Independently reconcile pool totals, calendar coverage and rational ratios."""
    start, end = (inventory_date(manifest[name]) for name in ("period_start", "period_end"))
    days = (end - start).days
    actual = _unique(table.to_pylist(), ("sku", "location_id"))
    scope = _unique(inputs["scope.csv"], ("sku", "location_id"))
    if set(actual) != set(scope) or not table.schema.equals(SCHEMA):
        raise ValueError("Inventory output pool/schema coverage differs")
    values, amounts = defaultdict(list), defaultdict(list)
    for row in inputs["snapshots.csv"]:
        values[(row["sku"], row["location_id"])].append(Decimal(row["inventory_value"]))
    for row in inputs["allocations.csv"]:
        amounts[(row["sku"], row["location_id"])].append(Decimal(row["cogs"]))
    for pool, row in actual.items():
        total, numerator = sum(values[pool], Decimal("0.00")), sum(amounts[pool], Decimal("0.00"))
        status = "incomplete_daily_coverage" if len(values[pool]) != days else "zero_inventory_value" if total == 0 else "complete"
        with localcontext() as context:
            context.prec = 50
            average = None if len(values[pool]) != days else (total / days).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
            ratio = (numerator * days / total).quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP) if status == "complete" else None
        expected = dict(period_start=start, period_end=end, currency="INR", cost_basis=COST_BASIS, period_basis=PERIOD_BASIS, is_synthetic=manifest["is_synthetic"],
            cogs=numerator, inventory_value_days=total, observed_days=len(values[pool]), expected_days=days,
            average_daily_inventory_value=average, turnover=ratio, quality_status=status)
        if any(row[name] != value for name, value in expected.items()):
            raise ValueError("Inventory turnover output does not reconcile")
