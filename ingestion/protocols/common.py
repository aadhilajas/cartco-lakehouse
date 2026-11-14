"""Immutable protocol evidence and bounded HTTP transport."""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pipeline.raw_batches import _write_once

MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode()


def retain(root, source, content):
    digest = hashlib.sha256(content).hexdigest()
    path = Path(root) / source / "objects" / digest
    _write_once(path, content)
    return {"sha256": digest, "bytes": len(content), "path": str(path)}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never forward source credentials to a redirect destination.


class JsonHTTP:
    def __init__(self, endpoint, *, root, source, headers=None, timeout=10, attempts=4, sleep=time.sleep):
        parsed = urlsplit(endpoint)
        if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.scheme not in {"http", "https"}:
            raise ValueError("Endpoint must be an HTTP(S) URL without embedded credentials/query")
        if parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "source-api"}:
            raise ValueError("Plain HTTP is allowed only for the local fixture")
        if not 0 < timeout <= 60 or not 1 <= attempts <= 6:
            raise ValueError("Invalid bounded HTTP policy")
        self.endpoint, self.root, self.source = endpoint.rstrip("/"), root, source
        self.headers, self.timeout, self.attempts, self.sleep = headers or {}, timeout, attempts, sleep
        self.evidence = []
        self.opener = build_opener(NoRedirect())

    def request(self, path="", *, body=None):
        if not path.startswith(("/", "?")) and path:
            raise ValueError("Request path must be relative to configured source")
        data = None if body is None else encoded(body)
        req = Request(self.endpoint + path, data=data, headers={"Accept": "application/json", "Content-Type": "application/json", **self.headers})
        for attempt in range(self.attempts):
            try:
                with self.opener.open(req, timeout=self.timeout) as response:
                    content = response.read(MAX_RESPONSE_BYTES + 1)
                    if len(content) > MAX_RESPONSE_BYTES:
                        raise ValueError("Source response exceeds byte limit")
                    self.evidence.append(retain(self.root, self.source, content))
                result = json.loads(content)
                if not isinstance(result, dict):
                    raise ValueError("Source response must be a JSON object")
                return result
            except HTTPError as exc:
                content = exc.read(MAX_RESPONSE_BYTES + 1)
                self.evidence.append({**retain(self.root, self.source, content), "http_status": exc.code})
                if exc.code not in {429, 500, 502, 503, 504} or attempt + 1 == self.attempts:
                    raise RuntimeError(f"{self.source} HTTP {exc.code}; raw evidence retained") from None
                retry = exc.headers.get("Retry-After", "")
                delay = min(10, float(retry)) if retry.isdigit() else min(10, 2 ** attempt)
            except (URLError, TimeoutError):
                if attempt + 1 == self.attempts:
                    raise RuntimeError(f"{self.source} transport retry limit reached") from None
                delay = min(10, 2 ** attempt)
            self.sleep(delay)


def publish_orders(rows, *, root, source, contract, batch_date, evidence):
    """Validate before exposing a complete delivery; replay returns the same bytes."""
    from pipeline.raw_batches import RawBatch
    from pipeline.versioned_ingestion import canonical_events
    from pipeline.source_contracts import validate_contract

    contract = validate_contract(contract, source=source, batch_date=batch_date)
    columns = sorted(set().union(*(row.keys() for row in rows))) if rows else ["order_id", "source_updated_at", "is_deleted"]
    if source == "pos":
        columns = sorted(set(columns) | {"store_id", "terminal_id", "transaction_id"})
    normalized = [{key: str(row.get(key, "")) for key in columns} for row in rows]
    normalized.sort(key=encoded)  # Broker partition interleaving must not change replay identity.
    # Existing exact-money, source-version and POS identity rules remain authoritative.
    canonical_events(RawBatch("validation", source, str(batch_date), Path(root), tuple(columns), tuple(normalized), contract))
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(normalized)
    content = stream.getvalue().encode()
    identity = hashlib.sha256(encoded({"source": source, "contract": contract, "csv_sha256": hashlib.sha256(content).hexdigest()})).hexdigest()
    directory = Path(root) / source / "deliveries" / identity
    _write_once(directory / "orders.csv", content)
    _write_once(directory / "orders.csv.contract.json", encoded(contract))
    # Timing is in attempt evidence, not immutable delivery identity.
    manifest = {"schema_version": 1, "source": source, "delivery_id": identity, "rows": len(rows), "csv_sha256": hashlib.sha256(content).hexdigest(), "contract": contract}
    _write_once(directory / "COMPLETE.json", encoded(manifest))
    attempt = {"completed_at": datetime.now(timezone.utc).isoformat(), "delivery_id": identity, "responses": evidence}
    retain(root, source + "/attempts", encoded(attempt))
    return directory / "orders.csv"
