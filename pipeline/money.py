"""Exact persisted INR money: reject rounding, overflow and binary floats."""
from decimal import Decimal, InvalidOperation, localcontext

CENT = Decimal("0.01")
LIMIT = Decimal("10000000000000000")


def exact_money(value, *, allow_legacy_float=False):
    if value is None or isinstance(value, bool) or (isinstance(value, float) and not allow_legacy_float):
        raise ValueError("Money requires an exact decimal value")
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number < 0 or number >= LIMIT:
            raise ValueError("Money outside decimal(18,2) range")
        with localcontext() as context:
            context.prec = 50
            rounded = number.quantize(CENT)
        if number != rounded:
            raise ValueError("Money has fractional cents; refusing implicit rounding")
        return rounded
    except InvalidOperation as exc:
        raise ValueError("Money must be a finite exact decimal") from exc
