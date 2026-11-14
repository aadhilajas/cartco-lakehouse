"""Orders v0/OrderItems shaped local SP-API fixture; real LWA is out of scope."""
from decimal import Decimal
from urllib.parse import quote, urlencode

from ingestion.protocols.common import publish_orders
from pipeline.money import exact_money


def normalize(order, items):
    if len(items) != 1:
        raise ValueError("Amazon fixture contract requires one product line")
    item = items[0]
    quantity = item["QuantityOrdered"]
    if type(quantity) is not int or quantity <= 0:
        raise ValueError("Amazon quantity must be a positive integer")
    money = item["ItemPrice"]
    price = exact_money(money["Amount"]) / quantity
    if price != price.quantize(Decimal("0.01")) or money["CurrencyCode"] != "INR":
        raise ValueError("Amazon line total cannot map to exact INR unit cents")
    if order["OrderTotal"] != money:
        raise ValueError("Amazon order/line totals differ; shipping/tax not in fixture contract")
    statuses = {"Shipped": "shipped", "Canceled": "cancelled", "Pending": "pending"}
    if order["OrderStatus"] not in statuses:
        raise ValueError("Unsupported Amazon status")
    return dict(order_id=order["AmazonOrderId"], customer_id=order["BuyerInfo"]["BuyerEmail"], channel="amazon",
        source_updated_at=order["LastUpdateDate"], is_deleted="false", order_date=order["PurchaseDate"], asin=item["ASIN"],
        product_category=item["CartcoSyntheticCategory"], quantity=str(quantity), unit_price=f"{price:.2f}", status=statuses[order["OrderStatus"]], currency="INR")


def pages(http, path, params, key, max_pages):
    seen = set()
    for _ in range(max_pages):
        result = http.request(path + "?" + urlencode(params))
        if result.get("errors"):
            raise ValueError("Amazon API errors; delivery not published")
        payload = result["payload"]
        yield payload.get(key, [])
        token = payload.get("NextToken")
        if not token:
            return
        if not isinstance(token, str) or token in seen:
            raise ValueError("Amazon pagination token did not advance")
        seen.add(token)
        params = {**params, "NextToken": token}
    raise ValueError("Amazon page limit exceeded")


def extract(http, *, contract, batch_date, marketplace="A21TJRUUN4KGV", max_pages=100):
    params = {"MarketplaceIds": marketplace, "LastUpdatedBefore": contract["extraction_end"]}
    params["LastUpdatedAfter" if contract["delivery_mode"] == "incremental" else "CreatedAfter"] = contract["extraction_start"] if contract["delivery_mode"] == "incremental" else "2020-01-01T00:00:00Z"
    rows = []
    for group in pages(http, "/orders/v0/orders", params, "Orders", max_pages):
        for order in group:
            items = []
            for chunk in pages(http, "/orders/v0/orders/" + quote(order["AmazonOrderId"], safe="") + "/orderItems", {}, "OrderItems", max_pages):
                items.extend(chunk)
            rows.append(normalize(order, items))
    return publish_orders(rows, root=http.root, source="amazon", contract=contract, batch_date=batch_date, evidence=http.evidence)
