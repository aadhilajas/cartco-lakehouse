"""Generate deterministic single-product POS receipts and a delivery sidecar."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import random
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.batch import resolve_batch_date
from pipeline.pos import pos_order_id
from pipeline.source_contracts import daily_contract

N_ROWS = 30_000
SEED = 44
OUTPUT_PATH = os.path.join(os.environ.get("DATA_DIR", "data"), "raw", "pos_orders.csv")
CATEGORIES = ("Apparel", "Footwear", "Electronics", "Home & Kitchen", "Beauty", "Sports & Outdoors", "Toys & Games", "Grocery")
COLUMNS = ("order_id", "store_id", "terminal_id", "transaction_id", "channel", "customer_id", "order_date",
           "sku", "product_category", "quantity", "unit_price", "order_total", "line_count", "tax_amount",
           "discount_amount", "status", "currency", "source_updated_at", "is_deleted")


def generate(as_of_date=None):
    day = resolve_batch_date(as_of_date)
    end = datetime.combine(day, datetime.min.time(), timezone.utc)
    start = end - timedelta(days=730)
    rng = random.Random(SEED)
    rows = []
    for index in range(N_ROWS):
        store, terminal = f"STORE-{index % 20:03d}", f"TILL-{(index // 20) % 4:02d}"
        transaction = f"{day.isoformat()}-{index // 20:06d}"
        key = pos_order_id(store, terminal, transaction)
        price = Decimal(rng.randrange(5000, 5000001)) / 100
        quantity = rng.choices((1, 2, 3, 4, 5), (50, 25, 13, 8, 4))[0]
        product = rng.randrange(600)
        rows.append(dict(order_id=key, store_id=store, terminal_id=terminal, transaction_id=transaction, channel="pos",
            customer_id="" if rng.random() < 0.35 else f"LOYALTY-{rng.randrange(4000):05d}",
            order_date=(start + timedelta(seconds=rng.randrange(730 * 86400))).isoformat(), sku=f"POS-{product:05d}",
            product_category=CATEGORIES[product % len(CATEGORIES)], quantity=quantity, unit_price=f"{price:.2f}",
            order_total=f"{price * quantity:.2f}", line_count=1, tax_amount="0.00", discount_amount="0.00",
            status=rng.choices(("paid", "voided", "refunded", "open"), (86, 6, 6, 2))[0], currency="INR",
            source_updated_at=end.isoformat(), is_deleted="false"))
    return pd.DataFrame(rows, columns=COLUMNS)


def main():
    path = Path(OUTPUT_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = generate()
    frame.to_csv(path, index=False)
    path.with_suffix(path.suffix + ".contract.json").write_text(
        json.dumps(daily_contract("pos", resolve_batch_date()), indent=2) + "\n", encoding="utf-8")
    print(f"Generated {len(frame):,} synthetic POS receipts -> {path}")


if __name__ == "__main__":
    main()
