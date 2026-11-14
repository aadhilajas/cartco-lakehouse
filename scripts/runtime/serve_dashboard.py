"""Read-only same-origin snapshot API and built React assets, stdlib only."""

# Support direct CLI execution and imports from any working directory.
import sys as _sys
from pathlib import Path as _Path
_SCRIPT_PATH = _Path(__file__).resolve()
_REPOSITORY_ROOT = next(
    (parent for parent in _SCRIPT_PATH.parents
     if (parent / "pipeline/source_identity.py").is_file()
     and (parent / "scripts/layout.json").is_file()),
    _SCRIPT_PATH.parent,  # Individually copied stdlib tools do not need a repo.
)
if str(_REPOSITORY_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPOSITORY_ROOT))
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit


def read_snapshot(path):
    raw = Path(path).read_bytes()
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("Snapshot exceeds serving limit")
    data = json.loads(raw)
    if data.get("schema_version") != 1 or data.get("is_synthetic") is not True or not re.fullmatch(r"r[0-9a-f]{32}", data.get("release_id", "")) or not data.get("quality", {}).get("success"):
        raise ValueError("No valid published snapshot")
    return raw, data


def server(dist, snapshot, host="127.0.0.1", port=18100):
    dist = Path(dist).resolve()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send(self, code, content, content_type="application/json"):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store" if content_type == "application/json" else "no-cache")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self'; style-src-attr 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; script-src 'self'; frame-ancestors 'none'; base-uri 'self'")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(content)

        def do_GET(self):
            path = unquote(urlsplit(self.path).path)
            if path in {"/api/snapshot", "/health"}:
                try:
                    raw, data = read_snapshot(snapshot)
                    return self.send(200, raw if path == "/api/snapshot" else json.dumps({"status": "healthy", "snapshot_release": data["release_id"]}).encode())
                except (OSError, ValueError):
                    return self.send(503, b'{"error":"Published snapshot unavailable"}')
            if path.startswith("/api/"):
                return self.send(404, b'{"error":"Unknown read-only endpoint"}')
            target = (dist / ("index.html" if path == "/" else path.lstrip("/"))).resolve()
            if not target.is_relative_to(dist) or not target.is_file():
                return self.send(404, b'{"error":"Not found"}')
            return self.send(200, target.read_bytes(), mimetypes.guess_type(target.name)[0] or "application/octet-stream")

        do_HEAD = do_GET

        def do_POST(self):
            self.send(405, b'{"error":"Read-only service"}')
        do_PUT = do_POST
        do_DELETE = do_POST
        do_PATCH = do_POST
    return ThreadingHTTPServer((host, port), Handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist", type=Path, default=Path("dashboard/dist"))
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18100)
    args = parser.parse_args()
    print(f"Read-only dashboard: http://{args.host}:{args.port}", flush=True)
    server(args.dist, args.snapshot, args.host, args.port).serve_forever()
