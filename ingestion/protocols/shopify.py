"""Cursor-paginated GraphQL orders, bounded to one product line per order."""
from ingestion.protocols.common import publish_orders

QUERY = """query Orders($after: String, $query: String!) {
  orders(first: 50, after: $after, query: $query, sortKey: UPDATED_AT) {
    nodes { id updatedAt createdAt cancelledAt displayFulfillmentStatus displayFinancialStatus
      customer { id } currentTotalPriceSet { shopMoney { amount currencyCode } }
      lineItems(first: 2) { nodes { quantity originalUnitPriceSet { shopMoney { amount currencyCode } }
        product { id productType } } pageInfo { hasNextPage } }
    } pageInfo { hasNextPage endCursor }
  }
}"""


def normalize(node):
    items = node["lineItems"]
    if items["pageInfo"]["hasNextPage"] or len(items["nodes"]) != 1:
        raise ValueError("Shopify fixture contract requires exactly one product line")
    item = items["nodes"][0]
    money = item["originalUnitPriceSet"]["shopMoney"]
    from pipeline.money import exact_money
    if money["currencyCode"] != "INR" or node["currentTotalPriceSet"]["shopMoney"]["currencyCode"] != "INR":
        raise ValueError("Shopify requires INR")
    if exact_money(money["amount"]) * item["quantity"] != exact_money(node["currentTotalPriceSet"]["shopMoney"]["amount"]):
        raise ValueError("Discount/tax/shipping-adjusted Shopify totals need a wider contract")
    status = "cancelled" if node.get("cancelledAt") else "returned" if node["displayFinancialStatus"] == "REFUNDED" else "delivered" if node["displayFulfillmentStatus"] == "FULFILLED" and node["displayFinancialStatus"] == "PAID" else None
    if status is None:
        raise ValueError("Unsupported Shopify status in bounded fixture")
    return dict(order_id=node["id"], source_updated_at=node["updatedAt"], is_deleted="false", channel="shopify",
        customer_id=(node.get("customer") or {}).get("id", ""), order_date=node["createdAt"], product_id=item["product"]["id"],
        product_category=item["product"]["productType"], quantity=str(item["quantity"]), unit_price=money["amount"], currency="INR", status=status)


def extract(http, *, contract, batch_date, max_pages=100):
    cursor, seen, rows = None, set(), []
    query = f"updated_at:<{contract['extraction_end']}"
    if contract["delivery_mode"] == "incremental":
        query += f" updated_at:>={contract['extraction_start']}"
    for _ in range(max_pages):
        result = http.request(body={"query": QUERY, "variables": {"after": cursor, "query": query}})
        if result.get("errors"):
            raise ValueError("Shopify GraphQL errors; no complete delivery published")
        connection = result["data"]["orders"]
        rows.extend(normalize(node) for node in connection["nodes"])
        info = connection["pageInfo"]
        if not info["hasNextPage"]:
            return publish_orders(rows, root=http.root, source="shopify", contract=contract, batch_date=batch_date, evidence=http.evidence)
        cursor = info["endCursor"]
        if not isinstance(cursor, str) or not cursor or cursor in seen:
            raise ValueError("Shopify pagination cursor did not advance")
        seen.add(cursor)
    raise ValueError("Shopify page limit exceeded")
