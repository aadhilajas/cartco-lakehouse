"""Assert table coverage and emit snapshot/count evidence, not financial acceptance."""

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
import json
import sys
from pathlib import Path

sys.path.insert(0, str(_REPOSITORY_ROOT))
from transforms.silver_shopify_orders import get_catalog


def main():
    catalog = get_catalog()
    names = ["bronze.shopify_orders", "bronze.amazon_orders", "silver.orders", "gold.daily_revenue", "gold.channel_performance", "gold.inventory_turnover", "gold.customer_360"]
    report = {}
    for name in names:
        table = catalog.load_table(name)
        data = table.scan().to_arrow()
        if data.num_rows == 0:
            raise RuntimeError(f"{name} is empty")
        channels = set(data.column("channel").to_pylist())
        expected = {"shopify"} if name == "bronze.shopify_orders" else {"amazon"} if name == "bronze.amazon_orders" else {"shopify", "amazon"}
        if channels != expected:
            raise RuntimeError(f"{name} channels: {channels}, expected {expected}")
        snapshot = table.current_snapshot()
        report[name] = {"rows": data.num_rows, "channels": sorted(channels), "snapshot_id": snapshot.snapshot_id}
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
