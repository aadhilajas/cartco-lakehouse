"""Generate a realistic synthetic Shopify orders dataset for the CartCo Bronze layer.

Produces 50,000 rows spanning the last two years and writes them to
``data/raw/shopify_orders.csv``. The data is synthetic (Faker + NumPy) and is
git-ignored — it is regenerated on demand and never source-controlled.

Run inside the Airflow container (deps are pre-installed there):
    docker exec cartco-airflow-scheduler \\
        python /opt/airflow/ingestion/generate_synthetic_shopify_data.py
"""

from __future__ import annotations

import json
import os
import uuid
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

REPO_ROOT = str(Path(__file__).resolve().parents[1])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
from pipeline.batch import resolve_batch_date
from pipeline.source_contracts import daily_contract

N_ROWS = 50_000
N_CUSTOMERS = 8_000
N_PRODUCTS = 1_200
SEED = 42

# DATA_DIR defaults to "data" relative to the Airflow working dir (/opt/airflow),
# which is bind-mounted to ./data on the host.
DATA_DIR = os.environ.get("DATA_DIR", "data")
OUTPUT_PATH = os.path.join(DATA_DIR, "raw", "shopify_orders.csv")

PRODUCT_CATEGORIES = [
    "Apparel",
    "Footwear",
    "Electronics",
    "Home & Kitchen",
    "Beauty",
    "Sports & Outdoors",
    "Toys & Games",
    "Grocery",
]

# Rough average unit price (INR) per category, used to sample realistic prices.
CATEGORY_BASE_PRICE = {
    "Apparel": 1500.0,
    "Footwear": 2800.0,
    "Electronics": 9500.0,
    "Home & Kitchen": 2200.0,
    "Beauty": 900.0,
    "Sports & Outdoors": 3200.0,
    "Toys & Games": 1100.0,
    "Grocery": 550.0,
}

# Order status distribution — most orders are delivered.
ORDER_STATUSES = ["delivered", "cancelled", "returned"]
ORDER_STATUS_WEIGHTS = [0.82, 0.10, 0.08]


def generate(as_of_date=None, *, include_versions=False) -> pd.DataFrame:
    fake = Faker()
    fake.seed_instance(SEED)
    rng = np.random.default_rng(SEED)

    customer_ids = [fake.uuid4() for _ in range(N_CUSTOMERS)]
    product_ids = [f"SHOP-{i:05d}" for i in range(N_PRODUCTS)]
    product_to_category = {
        pid: PRODUCT_CATEGORIES[rng.integers(0, len(PRODUCT_CATEGORIES))]
        for pid in product_ids
    }

    end = pd.Timestamp(resolve_batch_date(as_of_date))
    start = end - pd.DateOffset(years=2)
    start_epoch = int(start.timestamp())
    end_epoch = int(end.timestamp())
    order_epochs = rng.integers(start_epoch, end_epoch, size=N_ROWS)
    order_dates = pd.to_datetime(order_epochs, unit="s")

    chosen_products = rng.choice(product_ids, size=N_ROWS)
    categories = [product_to_category[pid] for pid in chosen_products]

    base_prices = np.array([CATEGORY_BASE_PRICE[c] for c in categories])
    unit_prices = np.round(
        np.maximum(50.0, rng.normal(base_prices, base_prices * 0.25)), 2
    )

    quantities = rng.choice([1, 2, 3, 4, 5], size=N_ROWS, p=[0.5, 0.25, 0.13, 0.08, 0.04])
    statuses = rng.choice(ORDER_STATUSES, size=N_ROWS, p=ORDER_STATUS_WEIGHTS)

    frame = pd.DataFrame(
        {
            "order_id": [str(uuid.uuid5(uuid.NAMESPACE_URL, f"cartco/shopify/{SEED}/{end.date()}/{i}")) for i in range(N_ROWS)],
            "customer_id": rng.choice(customer_ids, size=N_ROWS),
            "channel": "shopify",
            "order_date": order_dates,
            "product_id": chosen_products,
            "product_category": categories,
            "quantity": quantities.astype("int64"),
            "unit_price": unit_prices,
            "status": statuses,
        }
    )
    if include_versions:
        frame["currency"] = "INR"
        # The synthetic source declares its snapshot version; never infer legacy versions.
        frame["source_updated_at"] = end.isoformat() + "Z"
        frame["is_deleted"] = "false"
    return frame


def main() -> None:
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df = generate(include_versions=os.environ.get("CARTCO_SOURCE_VERSIONS", "0") == "1")
    df.to_csv(OUTPUT_PATH, index=False)
    if os.environ.get("CARTCO_SOURCE_VERSIONS", "0") == "1":
        contract = daily_contract("shopify", resolve_batch_date())
        Path(OUTPUT_PATH + ".contract.json").write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")
    print(f"Generated {len(df):,} synthetic Shopify orders -> {OUTPUT_PATH}")
    print(f"Date range: {df['order_date'].min()} .. {df['order_date'].max()}")
    print("Status distribution:")
    print(df["status"].value_counts().to_string())


if __name__ == "__main__":
    main()
