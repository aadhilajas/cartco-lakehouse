"""Resolve one explicit load partition date, independent of source order dates."""
from __future__ import annotations

from datetime import date, datetime, timezone
import os
import re
from typing import Mapping


def resolve_batch_date(
    value: date | str | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> date:
    """Argument > CARTCO_BATCH_DATE > UTC date for standalone compatibility.

    Airflow and the local runner pass one date to every stage, so a run crossing
    midnight keeps its partition. This is a load date, not a currency/business
    timezone policy or a complete immutable batch manifest.
    """
    if value is None:
        value = (os.environ if environ is None else environ).get("CARTCO_BATCH_DATE")
    if value is None:
        timestamp = now if now is not None else datetime.now(timezone.utc)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("Batch clock must be timezone-aware")
        return timestamp.astimezone(timezone.utc).date()
    if isinstance(value, datetime):
        raise ValueError("Batch date must be a date, not a timestamp")
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Batch date must use YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid batch date: {value}") from exc
