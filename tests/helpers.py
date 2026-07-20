"""Small, shared test builders. Not a test module itself (no test_ functions)."""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

# Column order matches what transforms.silver_shopify_orders.transform() expects
# on its input: the Bronze-side columns (status, not order_status) minus
# ingestion_date, which the transform never reads.
BRONZE_SHAPED_COLUMNS = [
    "order_id",
    "customer_id",
    "channel",
    "order_date",
    "product_id",
    "product_category",
    "quantity",
    "unit_price",
    "status",
]


def make_bronze_table(
    order_id: list[str | None],
    customer_id: list[str | None],
    order_date: list[datetime],
    quantity: list[int | None],
    unit_price: list[float | None],
    status: list[str | None],
    channel: list[str] | None = None,
    product_id: list[str] | None = None,
    product_category: list[str | None] | None = None,
) -> pa.Table:
    """Build a small PyArrow table shaped like a bronze.shopify_orders scan.

    Every list argument must be the same length. Defaults fill in plausible
    values for columns a given test doesn't care about, so call sites only
    need to specify the columns relevant to what they're asserting.
    """
    n = len(order_id)
    channel = channel or ["shopify"] * n
    product_id = product_id or [f"SHOP-{i:05d}" for i in range(n)]
    product_category = product_category or ["Electronics"] * n

    return pa.table(
        {
            "order_id": pa.array(order_id, type=pa.string()),
            "customer_id": pa.array(customer_id, type=pa.string()),
            "channel": pa.array(channel, type=pa.string()),
            "order_date": pa.array(order_date, type=pa.timestamp("us")),
            "product_id": pa.array(product_id, type=pa.string()),
            "product_category": pa.array(product_category, type=pa.string()),
            "quantity": pa.array(quantity, type=pa.int32()),
            "unit_price": pa.array(unit_price, type=pa.float64()),
            "status": pa.array(status, type=pa.string()),
        }
    )
