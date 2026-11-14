import json

from pipeline.source_contracts import daily_contract
from pos_helpers import DAY, receipt, write_csv


def deliveries(root):
    incoming = root / DAY
    incoming.mkdir(parents=True, exist_ok=True)
    for source, status in (("shopify", "delivered"), ("amazon", "shipped"), ("pos", "paid")):
        row = receipt() if source == "pos" else dict(order_id="o1", customer_id="c1", channel=source,
            order_date="2026-01-01T23:30:00Z", product_category="Apparel", quantity="1", unit_price="10.00",
            status=status, currency="INR", source_updated_at="2026-01-02T00:00:00Z", is_deleted="false",
            **{"asin" if source == "amazon" else "product_id": "p1"})
        write_csv(incoming / f"{source}.csv", row)
        (incoming / f"{source}.csv.contract.json").write_text(json.dumps(daily_contract(source, DAY)), encoding="utf-8")
    return root
