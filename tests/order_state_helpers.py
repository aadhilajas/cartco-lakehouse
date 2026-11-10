"""Small explicit source-version fixtures shared by pure and local-file tests."""
from datetime import date, datetime

import pyarrow as pa

from pipeline.order_state import ARROW_SCHEMA

APPLIED = datetime(2026, 1, 20)


def order(key="o1", version=1, **changes):
    row = dict(order_id=key, customer_id="c1", channel="shopify", order_date=date(2026, 1, 1),
               product_id="p1", product_category="Apparel", quantity=1, unit_price=10.0,
               order_status="completed", order_total=10.0, transform_timestamp=datetime(2026, 1, 1),
               source_updated_at=datetime(2026, 1, version), is_deleted=False)
    row.update(changes)
    return row


def events(*rows):
    return pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)


def tombstone(key="o1", version=2, channel="shopify"):
    return pa.table({"order_id": [key], "channel": [channel],
                     "source_updated_at": pa.array([datetime(2026, 1, version)], pa.timestamp("us")),
                     "is_deleted": [True]})
