"""Versioned delivery metadata and unambiguous source/business time semantics."""
from datetime import datetime, time, timedelta, timezone
import hashlib
import json
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pipeline.batch import resolve_batch_date

FIELDS = {"contract_version", "source_schema_version", "business_timezone", "source_timezone",
          "extraction_start", "extraction_end", "delivery_mode"}
SOURCE_SCHEMAS = {"shopify": "shopify.orders.v2", "amazon": "amazon.orders.v2", "pos": "pos.orders.v1"}
SOURCES = frozenset(SOURCE_SCHEMAS)
TIMEZONE_PROPERTY = "cartco.business.timezone"


def zone(name):
    try:
        if not isinstance(name, str):
            raise ValueError("Timezone must be an IANA name")
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, TypeError) as exc:
        raise ValueError("Timezone must be a known IANA name") from exc


def timestamp(value):
    if not isinstance(value, str) or "T" not in value:
        raise ValueError("Timestamp must include date and time")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Timestamp requires an explicit UTC offset")
    return result.astimezone(timezone.utc)


def validate_contract(contract, *, source, batch_date):
    if not isinstance(contract, dict) or set(contract) != FIELDS:
        raise ValueError("Delivery contract has missing or unexpected fields")
    if type(contract["contract_version"]) is not int or contract["contract_version"] != 1:
        raise ValueError("Unsupported delivery contract version")
    if source not in SOURCES or contract["source_schema_version"] != SOURCE_SCHEMAS[source]:
        raise ValueError("Unsupported source schema version")
    business = zone(contract["business_timezone"])
    zone(contract["source_timezone"])
    start, end = (timestamp(contract[key]) for key in ("extraction_start", "extraction_end"))
    if start >= end or start.astimezone(business).date() != resolve_batch_date(batch_date):
        raise ValueError("Extraction window must be increasing and start on the business batch date")
    if contract["delivery_mode"] not in {"snapshot", "incremental"}:
        raise ValueError("Unsupported delivery mode")
    return dict(contract)


def contract_bytes(contract):
    return json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()


def batch_identity(source, day, checksum, contract=None):
    identity = f"{source}\n{day}\n{checksum}".encode()
    if contract is not None:
        identity += b"\n" + contract_bytes(contract)
    return hashlib.sha256(identity).hexdigest()


def business_date(value, contract):
    if not isinstance(value, str) or ("T" not in value and " " not in value):
        raise ValueError("Order timestamp must include date and time")
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        source = zone(contract["source_timezone"])
        # Round-trip both folds: reject nonexistent and ambiguous wall times.
        candidates = {instant.replace(tzinfo=source, fold=fold).astimezone(timezone.utc)
                      for fold in (0, 1)
                      if instant.replace(tzinfo=source, fold=fold).astimezone(timezone.utc).astimezone(source).replace(tzinfo=None) == instant}
        if len(candidates) != 1:
            raise ValueError("Ambiguous or nonexistent source wall time requires an explicit offset")
        instant = candidates.pop()
    return instant.astimezone(zone(contract["business_timezone"])).date()


def check_source_version(version, contract):
    start, end = (timestamp(contract[key]) for key in ("extraction_start", "extraction_end"))
    if version >= end or (contract["delivery_mode"] == "incremental" and version < start):
        raise ValueError("Source version falls outside the declared extraction window")


def daily_contract(source, batch_date, *, business_timezone="Asia/Kolkata", source_timezone="UTC", delivery_mode="snapshot"):
    day = resolve_batch_date(batch_date)
    business = zone(business_timezone)
    return validate_contract(dict(contract_version=1, source_schema_version=SOURCE_SCHEMAS.get(source),
        business_timezone=business_timezone, source_timezone=source_timezone, delivery_mode=delivery_mode,
        extraction_start=datetime.combine(day, time(), business).isoformat(),
        extraction_end=datetime.combine(day + timedelta(days=1), time(), business).isoformat()), source=source, batch_date=day)
