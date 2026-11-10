"""Generate a realistic synthetic Amazon marketplace orders dataset for the CartCo Bronze layer.

Produces 40,000 rows spanning the last two years and writes them to
``data/raw/amazon_orders.csv``. The data is synthetic (Faker + NumPy) and is
git-ignored — it is regenerated on demand and never source-controlled.

Mirrors the structure and conventions of
``ingestion/generate_synthetic_shopify_data.py`` (the Shopify equivalent),
with Amazon-specific fields: ``asin`` (Amazon Standard Identification Number),
``marketplace`` (``amazon.in`` / ``amazon.com`` / ``amazon.co.uk``), and
``fulfillment_channel`` (``FBA`` / ``FBM``). The status vocabulary uses
Amazon-realistic values (``shipped`` / ``cancelled`` / ``refund_initiated`` /
``pending``), deliberately different from Shopify's source vocabulary — Bronze
stays source-faithful and Silver (when built) will own the canonical mapping.

Run inside the Airflow container (deps are pre-installed there):
    docker exec cartco-airflow-scheduler \\
        python /opt/airflow/ingestion/generate_synthetic_amazon_data.py
"""

from __future__ import annotations

import os
import string
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

N_ROWS = 40_000
N_CUSTOMERS = 6_000
N_PRODUCTS = 800
SEED = 42

# DATA_DIR defaults to "data" relative to the Airflow working dir (/opt/airflow),
# which is bind-mounted to ./data on the host.
DATA_DIR = os.environ.get("DATA_DIR", "data")
OUTPUT_PATH = os.path.join(DATA_DIR, "raw", "amazon_orders.csv")

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
# Same base prices as Shopify for downstream Gold-layer compatibility.
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

# Amazon-specific order status distribution. These are deliberately different
# from Shopify's source vocabulary (delivered/cancelled/returned) — Bronze is
# source-faithful and does not harmonise status values across channels.
ORDER_STATUSES = ["shipped", "cancelled", "refund_initiated", "pending"]
ORDER_STATUS_WEIGHTS = [0.75, 0.12, 0.08, 0.05]

# Amazon marketplace distribution — India-heavy, reflecting CartCo's primary market.
MARKETPLACES = ["amazon.in", "amazon.com", "amazon.co.uk"]
MARKETPLACE_WEIGHTS = [0.80, 0.12, 0.08]

# Fulfillment channel distribution — FBA dominant.
FULFILLMENT_CHANNELS = ["FBA", "FBM"]
FULFILLMENT_WEIGHTS = [0.70, 0.30]

# Characters used in the random portion of an ASIN (uppercase letters + digits).
_ASIN_CHARS = string.ascii_uppercase + string.digits


def _generate_asins(rng: np.random.Generator, n: int) -> list[str]:
    """Generate ``n`` unique ASINs in the format ``B0`` + 8 alphanumeric chars.

    Real Amazon ASINs for products (non-books) always start with ``B0``
    followed by 8 uppercase alphanumeric characters (e.g. ``B0CX7K2JMP``).
    """
    charset = np.array(list(_ASIN_CHARS))
    suffixes = rng.choice(charset, size=(n, 8))
    return ["B0" + "".join(row) for row in suffixes]


def generate(as_of_date=None, *, include_versions=False) -> pd.DataFrame:
    fake = Faker()
    fake.seed_instance(SEED)
    rng = np.random.default_rng(SEED)

    customer_ids = [fake.uuid4() for _ in range(N_CUSTOMERS)]
    asins = _generate_asins(rng, N_PRODUCTS)
    asin_to_category = {
        asin: PRODUCT_CATEGORIES[rng.integers(0, len(PRODUCT_CATEGORIES))]
        for asin in asins
    }

    end = pd.Timestamp(resolve_batch_date(as_of_date))
    start = end - pd.DateOffset(years=2)
    start_epoch = int(start.timestamp())
    end_epoch = int(end.timestamp())
    order_epochs = rng.integers(start_epoch, end_epoch, size=N_ROWS)
    order_dates = pd.to_datetime(order_epochs, unit="s")

    chosen_asins = rng.choice(asins, size=N_ROWS)
    categories = [asin_to_category[asin] for asin in chosen_asins]

    base_prices = np.array([CATEGORY_BASE_PRICE[c] for c in categories])
    unit_prices = np.round(
        np.maximum(50.0, rng.normal(base_prices, base_prices * 0.25)), 2
    )

    quantities = rng.choice([1, 2, 3, 4, 5], size=N_ROWS, p=[0.55, 0.25, 0.10, 0.07, 0.03])
    statuses = rng.choice(ORDER_STATUSES, size=N_ROWS, p=ORDER_STATUS_WEIGHTS)
    marketplaces = rng.choice(MARKETPLACES, size=N_ROWS, p=MARKETPLACE_WEIGHTS)
    fulfillment_channels = rng.choice(FULFILLMENT_CHANNELS, size=N_ROWS, p=FULFILLMENT_WEIGHTS)

    frame = pd.DataFrame(
        {
            "order_id": [str(uuid.uuid5(uuid.NAMESPACE_URL, f"cartco/amazon/{SEED}/{end.date()}/{i}")) for i in range(N_ROWS)],
            "customer_id": rng.choice(customer_ids, size=N_ROWS),
            "channel": "amazon",
            "order_date": order_dates,
            "asin": chosen_asins,
            "product_category": categories,
            "quantity": quantities.astype("int64"),
            "unit_price": unit_prices,
            "status": statuses,
            "marketplace": marketplaces,
            "fulfillment_channel": fulfillment_channels,
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
    print(f"Generated {len(df):,} synthetic Amazon orders -> {OUTPUT_PATH}")
    print(f"Date range: {df['order_date'].min()} .. {df['order_date'].max()}")
    print("Status distribution:")
    print(df["status"].value_counts().to_string())
    print("Marketplace distribution:")
    print(df["marketplace"].value_counts().to_string())
    print("Fulfillment channel distribution:")
    print(df["fulfillment_channel"].value_counts().to_string())


if __name__ == "__main__":
    main()
