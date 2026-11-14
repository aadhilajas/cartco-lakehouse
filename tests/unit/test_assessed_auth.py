"""Exercise real cookie redirects and CSRF form exchange without Airflow imports."""
import http.cookiejar
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
from pathlib import Path
import threading
from urllib.parse import parse_qs
from urllib.request import build_opener, HTTPCookieProcessor

spec = importlib.util.spec_from_file_location("assessed_runner", Path(__file__).resolve().parents[2] / "scripts/run_assessed_stack.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def test_login_works_with_valid_and_expired_prior_sessions():
    forms, logins = [], []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_GET(self):
            if self.path == "/login/" and "session=valid" in self.headers.get("Cookie", ""):
                self.send_response(302); self.send_header("Location", "/home"); self.end_headers(); return
            self.send_response(200)
            if self.path == "/login/":
                forms.append(True)
                self.send_header("Set-Cookie", "csrf=token; Path=/")
                self.end_headers()
                self.wfile.write(b'<input name="csrf_token" value="token">')
            else:
                self.end_headers(); self.wfile.write(b"authenticated home")
        def do_POST(self):
            body = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
            assert body["csrf_token"] == ["token"] and "csrf=token" in self.headers.get("Cookie", "")
            logins.append(True)
            self.send_response(303); self.send_header("Set-Cookie", "session=valid; Path=/")
            self.send_header("Location", "/home"); self.end_headers()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True); worker.start()
    try:
        cookies = http.cookiejar.CookieJar()
        opener = build_opener(HTTPCookieProcessor(cookies))
        api = "http://127.0.0.1:" + str(server.server_port)
        runner.airflow_login(opener, cookies, api)
        assert any(cookie.name == "session" for cookie in cookies)
        # Existing authenticated /login would redirect without the cookie reset.
        runner.airflow_login(opener, cookies, api)
        cookies.clear()  # Expired session after a webserver restart.
        runner.airflow_login(opener, cookies, api)
        assert len(forms) == len(logins) == 3
    finally:
        server.shutdown(); server.server_close(); worker.join(timeout=3)
