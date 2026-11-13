"""Explicitly synthetic HTTP fixtures with real wire protocols and pagination."""
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from urllib.parse import parse_qs, urlsplit

from pipeline.source_contracts import timestamp


def shopify_order(index, updated="2026-11-12T06:00:00Z"):
    return {"id": f"gid://shopify/Order/{index}", "updatedAt": updated, "createdAt": "2026-11-11T08:00:00Z",
        "cancelledAt": None, "displayFulfillmentStatus": "FULFILLED", "displayFinancialStatus": "PAID",
        "customer": {"id": f"gid://shopify/Customer/{index % 2}"},
        "currentTotalPriceSet": {"shopMoney": {"amount": "100.50", "currencyCode": "INR"}},
        "lineItems": {"nodes": [{"quantity": 1, "originalUnitPriceSet": {"shopMoney": {"amount": "100.50", "currencyCode": "INR"}},
            "product": {"id": f"gid://shopify/Product/{index}", "productType": "Apparel"}}], "pageInfo": {"hasNextPage": False}}}


def amazon_order(index, updated="2026-11-12T06:00:00Z"):
    return {"AmazonOrderId": f"SYNTHETIC-AMZ-{index}", "LastUpdateDate": updated, "PurchaseDate": "2026-11-11T08:00:00Z",
        "OrderStatus": "Shipped", "OrderTotal": {"Amount": "200.00", "CurrencyCode": "INR"},
        "BuyerInfo": {"BuyerEmail": f"synthetic-buyer-{index}@example.invalid"}}


def amazon_item(index):
    return {"ASIN": f"SYNTH{index:05d}", "QuantityOrdered": 2,
        "ItemPrice": {"Amount": "200.00", "CurrencyCode": "INR"}, "CartcoSyntheticCategory": "Electronics"}


def http_server(host="127.0.0.1", port=0, *, throttle_once=False, repeat_cursor=False):
    """Return an unstarted server so tests/services control its lifetime."""
    class Handler(BaseHTTPRequestHandler):
        throttled = False

        def log_message(self, *args):
            pass

        def reply(self, payload, code=200):
            content = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            if code == 429:
                self.send_header("Retry-After", "0")
            self.end_headers()
            self.wfile.write(content)

        def throttle(self):
            if throttle_once and not Handler.throttled:
                Handler.throttled = True
                self.reply({"errors": [{"code": "Throttled", "message": "Synthetic throttle"}]}, 429)
                return True
            return False

        def do_POST(self):
            if self.path != "/admin/api/2026-10/graphql.json":
                return self.reply({"error": "unknown synthetic endpoint"}, 404)
            if self.throttle():
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size < 16384:
                    raise ValueError()
                body = json.loads(self.rfile.read(size))
                variables = body["variables"]
                if "orders(" not in body["query"] or "updated_at:<" not in variables["query"]:
                    raise ValueError()
                # Only the documented bounded search expression is accepted.
                terms = variables["query"].split()
                end = timestamp(terms[0].removeprefix("updated_at:<"))
                start = timestamp(terms[1].removeprefix("updated_at:>=")) if len(terms) == 2 else datetime.min.replace(tzinfo=timezone.utc)
                rows = [shopify_order(i) for i in range(1, 4) if start <= timestamp(shopify_order(i)["updatedAt"]) < end]
                offset = int(variables["after"] or 0)
                page = rows[offset:offset + 2]
                more = offset + 2 < len(rows)
                self.reply({"data": {"orders": {"nodes": page, "pageInfo": {"hasNextPage": more, "endCursor": "2" if repeat_cursor else str(offset + len(page))}}}})
            except (ValueError, KeyError, TypeError):
                self.reply({"errors": [{"message": "Invalid synthetic GraphQL request"}]}, 400)

        def do_GET(self):
            if self.path == "/health":
                return self.reply({"status": "healthy", "synthetic": True})
            if self.throttle():
                return
            try:
                url = urlsplit(self.path)
                params = parse_qs(url.query)
                if url.path == "/orders/v0/orders":
                    if params["MarketplaceIds"] != ["A21TJRUUN4KGV"]:
                        raise ValueError()
                    end = timestamp(params["LastUpdatedBefore"][0])
                    start = timestamp(params.get("LastUpdatedAfter", params.get("CreatedAfter"))[0])
                    rows = [amazon_order(i) for i in range(1, 4) if start <= timestamp(amazon_order(i)["LastUpdateDate"]) < end]
                    offset = int(params.get("NextToken", ["0"])[0])
                    payload = {"Orders": rows[offset:offset + 2]}
                    if offset + 2 < len(rows):
                        payload["NextToken"] = "2" if repeat_cursor else str(offset + 2)
                    self.reply({"payload": payload})
                elif url.path.startswith("/orders/v0/orders/SYNTHETIC-AMZ-") and url.path.endswith("/orderItems"):
                    index = int(url.path.split("/")[-2].split("-")[-1])
                    self.reply({"payload": {"OrderItems": [amazon_item(index)]}})
                else:
                    self.reply({"error": "unknown synthetic endpoint"}, 404)
            except (ValueError, KeyError, TypeError):
                self.reply({"errors": [{"message": "Invalid synthetic Orders request"}]}, 400)

    return ThreadingHTTPServer((host, port), Handler)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18081)
    args = parser.parse_args()
    http_server(args.host, args.port).serve_forever()
