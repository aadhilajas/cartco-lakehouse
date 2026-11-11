"""POS v1 single-product receipt identity and source-specific validation."""
import json
from uuid import NAMESPACE_URL, uuid5

from pipeline.money import exact_money


def pos_order_id(store_id, terminal_id, transaction_id):
    parts = (store_id, terminal_id, transaction_id)
    if any(not isinstance(value, str) or not value or value.strip() != value for value in parts):
        raise ValueError("POS requires canonical store, terminal and transaction IDs")
    # JSON encodes components unambiguously; delimiters in native IDs cannot collide.
    return str(uuid5(NAMESPACE_URL, "cartco/pos/v1/" + json.dumps(parts, ensure_ascii=True, separators=(",", ":"))))


def validate_pos_key(raw):
    expected = pos_order_id(*(raw.get(name) for name in ("store_id", "terminal_id", "transaction_id")))
    if raw.get("order_id") != expected:
        raise ValueError("POS order_id differs from its store/terminal/transaction identity")


def pos_customer(raw):
    value = raw.get("customer_id")
    if not isinstance(value, str) or (value and value.strip() != value):
        raise ValueError("POS requires a customer_id field; empty means walk-in")
    return "pos:customer:" + value if value else "pos:guest:" + raw["order_id"]


def validate_pos_amounts(raw, total):
    if raw.get("line_count") != "1":
        raise ValueError("POS v1 supports exactly one product line per receipt")
    for name in ("tax_amount", "discount_amount"):
        if name in raw and exact_money(raw[name]) != 0:
            raise ValueError("POS v1 requires zero separate tax/discount amounts")
    if "order_total" in raw and exact_money(raw["order_total"]) != total:
        raise ValueError("POS receipt total differs from quantity times price")
