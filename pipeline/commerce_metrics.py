"""Shared completed-sales contract and exact-cent calculations for Gold."""
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
import math

import pyarrow as pa
import pyarrow.compute as pc

CENT = Decimal("0.01")
MONEY = pa.decimal128(18, 2)
STATUSES = {"completed", "cancelled", "refunded", "pending", "unknown"}


def amount(value) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError("Money must be a finite nonnegative value")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Money must be numeric") from exc
    if not number.is_finite() or number < 0:
        raise ValueError("Money must be a finite nonnegative value")
    with localcontext() as context:
        context.prec = 50
        return number.quantize(CENT, rounding=ROUND_HALF_UP)


def legacy_money(values) -> pa.Array:
    """Keep legacy double schemas only when their string round-trip retains cents."""
    result = []
    for value in values:
        expected = amount(value)
        converted = float(expected)
        if not math.isfinite(converted) or Decimal(str(converted)) != expected:
            raise OverflowError("Legacy double output cannot preserve cents; use a decimal schema")
        result.append(converted)
    return pa.array(result, pa.float64())


def validated_orders(table: pa.Table) -> pa.Table:
    """Exclude tombstones, validate business keys and derive exact-cent order values."""
    if "is_deleted" in table.column_names:
        if table["is_deleted"].type != pa.bool_() or table["is_deleted"].null_count:
            raise ValueError("Deletion flags must be nonnull booleans")
        table = table.filter(pc.equal(table["is_deleted"], False))
    if "currency" in table.column_names and set(table["currency"].to_pylist()) - {"INR"}:
        raise ValueError("Mixed or unsupported source currencies block Gold")
    required = {"order_id", "customer_id", "channel", "order_date", "quantity", "unit_price", "order_total", "order_status", "product_category"}
    if required - set(table.column_names):
        raise ValueError("Gold input is missing canonical order columns")
    seen, amounts = set(), []
    for row in table.to_pylist():
        key = (row["channel"], row["order_id"])
        if any(not isinstance(v, str) or not v.strip() for v in (*key, row["customer_id"], row["product_category"])) or not isinstance(row["order_date"], date) or isinstance(row["order_date"], datetime):
            raise ValueError("Gold input needs valid order/customer/channel/date keys")
        if key in seen:
            raise ValueError("Gold input contains duplicate channel/order keys")
        seen.add(key)
        if row["order_status"] not in STATUSES:
            raise ValueError("Gold input has an invalid canonical status")
        quantity = row["quantity"]
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0:
            raise ValueError("Gold quantity must be a nonnegative integer")
        try:
            price = Decimal(str(row["unit_price"]))
        except InvalidOperation as exc:
            raise ValueError("Gold unit_price must be numeric") from exc
        if not price.is_finite() or price < 0:
            raise ValueError("Gold unit_price must be finite and nonnegative")
        total = amount(row["order_total"])
        if total != amount(price * quantity):
            raise ValueError("Gold order_total differs from quantity times unit_price")
        amounts.append(total)
    return table.set_column(table.schema.get_field_index("order_total"), "order_total", pa.array(amounts, MONEY))


def completed_orders(table: pa.Table) -> pa.Table:
    table = validated_orders(table)
    return table.filter(pc.equal(table["order_status"], "completed"))
