"""Explicit POS v1 deliveries for adapter and real-file tests."""
import csv
from datetime import datetime

from pipeline.pos import pos_order_id

DAY = "2026-01-05"
APPLIED = datetime(2026, 1, 5)


def receipt(*, store="S1", terminal="T1", transaction="R1", version=2, status="paid", customer=""):
    return dict(order_id=pos_order_id(store, terminal, transaction), store_id=store, terminal_id=terminal,
        transaction_id=transaction, channel="pos", customer_id=customer, order_date="2026-01-01T23:30:00Z",
        sku="SKU1", product_category="Apparel", quantity="2", unit_price="0.10", order_total="0.20",
        line_count="1", tax_amount="0.00", discount_amount="0.00", status=status, currency="INR",
        source_updated_at=f"2026-01-{version:02d}T00:00:00Z", is_deleted="false")


def deletion(*, store="S1", terminal="T1", transaction="R1", version=3):
    return {key: value for key, value in receipt(store=store, terminal=terminal, transaction=transaction, version=version).items()
            if key in {"order_id", "store_id", "terminal_id", "transaction_id", "source_updated_at"}} | {"is_deleted": "true"}


def write_csv(path, *rows):
    fields = list(dict.fromkeys(key for row in rows for key in row)) if rows else list(receipt())
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path
