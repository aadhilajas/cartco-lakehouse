"""Host-side build, real scheduled DAGs, Grafana and publication acceptance.

Stdlib only. Run with the user's Docker access. It stops only the earlier new
Phase 1 stacks to release their overlapping ports; original baseline is retained.
"""

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
import base64
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import http.cookiejar
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, build_opener, HTTPCookieProcessor


class CSRF(HTMLParser):
    token = None
    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "input" and values.get("name") == "csrf_token": self.token = values.get("value")


def airflow_login(opener, cookies, api):
    # An authenticated GET /login redirects home; start with an empty session
    # so both valid and expired prior cookies receive a real CSRF login form.
    cookies.clear()
    with opener.open(api + "/login/", timeout=15) as response: html = response.read().decode()
    csrf = CSRF(); csrf.feed(html)
    if not csrf.token: raise RuntimeError("Airflow login CSRF token missing")
    body = urlencode({"username": "airflow", "password": "airflow", "csrf_token": csrf.token}).encode()
    with opener.open(Request(api + "/login/", data=body), timeout=15) as response: response.read()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--result-root", type=Path)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--terraform", help="Terraform executable; deploy into its isolated assessed workspace")
    parser.add_argument("--resume-after-recreation", action="store_true")
    args = parser.parse_args()
    repo = _REPOSITORY_ROOT
    sys.path.insert(0, str(repo))
    from pipeline.source_identity import source_identity
    evidence = args.result_root or repo / "test-results/assessed" / datetime.now().strftime("%Y%m%d-%H%M%S")
    evidence = evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    if args.resume_after_recreation and (not args.terraform or not args.result_root or (evidence / "SUCCESS.txt").exists()):
        raise ValueError("Resume requires an unfinished explicit Terraform result directory")
    (evidence / "SUCCESS.txt").unlink(missing_ok=True)
    compose = [args.docker, "compose", "-p", "cartco-assessed", "-f", str(repo / "infra/local/stack.json")]

    def run(command, name):
        if args.resume_after_recreation and name.startswith("terraform-"): name = "resume-" + name
        print("Running:", name, flush=True)
        with (evidence / name).open("wb") as log:
            result = subprocess.run(command, cwd=repo, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode:
            print((evidence / name).read_text(encoding="utf-8", errors="replace")[-5000:], flush=True)
            raise RuntimeError(f"Stage failed ({result.returncode}): {name}; evidence {evidence}")
        return (evidence / name).read_text(encoding="utf-8", errors="replace")

    from scripts.deployment.assessed_deployment import Deployment
    deployment = Deployment(repo=repo, evidence=evidence, docker=args.docker, terraform=args.terraform, run=run)

    def inventory(name):
        return [json.loads(row) for row in run([args.docker, "ps", "-a", "--format", "{{json .}}"], name).splitlines()]

    def baseline(rows):
        return sorted((row["Names"], row["ID"]) for row in rows if row["Names"].startswith("cartco-") and not row["Names"].startswith(("cartco-infra-p1-", "cartco-assessed-")))

    cookies = http.cookiejar.CookieJar()
    opener = build_opener(HTTPCookieProcessor(cookies))
    def request(url, data=None, method=None, headers=None):
        body = None if data is None else json.dumps(data).encode()
        req = Request(url, data=body, method=method, headers={"Content-Type": "application/json", **(headers or {})})
        with opener.open(req, timeout=15) as response: return json.load(response)

    def wait(url, *, airflow=False, seconds=300):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            try:
                value = request(url)
                if not airflow or all(value[key]["status"] == "healthy" for key in ["metadatabase", "scheduler"]): return value
            except (OSError, ValueError): pass
            time.sleep(2)
        raise TimeoutError("Readiness timeout: " + url)

    api = "http://127.0.0.1:18080"
    def login():
        airflow_login(opener, cookies, api)

    def dag_run(name):
        url = api + "/api/v1/dags/" + name
        was_paused = request(url)["is_paused"]
        run_id = "local-acceptance-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        try:
            request(url, {"is_paused": False}, "PATCH")
            request(url + "/dagRuns", {"dag_run_id": run_id}, "POST")
            deadline = time.monotonic() + 1500
            while time.monotonic() < deadline:
                result = request(url + "/dagRuns/" + quote(run_id, safe=""))
                if result["state"] in {"success", "failed"}:
                    tasks = request(url + "/dagRuns/" + quote(run_id, safe="") + "/taskInstances")
                    content = json.dumps({"run": result, "tasks": tasks}, indent=2)
                    (evidence / (name + "-" + run_id + ".json")).write_text(content, encoding="utf-8")
                    (evidence / (name + ".json")).write_text(content, encoding="utf-8")
                    if result["state"] != "success": raise RuntimeError("Scheduled DAG failed: " + name)
                    print("Scheduled DAG passed:", name, flush=True)
                    return result
                time.sleep(3)
            raise TimeoutError("DAG did not finish: " + name)
        finally:
            request(url, {"is_paused": was_paused}, "PATCH")

    before = None
    try:
        if args.resume_after_recreation:
            for name in ["terraform-apply.log", "public-snapshot.json", "catalog-after-recreation.log", "containers-before.jsonl"]:
                if not (evidence / name).is_file(): raise ValueError("Missing completed resume prerequisite: " + name)
            for dag in ["assessed_orders_ingestion", "assessed_inventory_ingestion", "assessed_commerce_release", "assessed_system_health"]:
                if json.loads((evidence / (dag + ".json")).read_text())["run"]["state"] != "success":
                    raise ValueError("Cannot resume an unfinished initial DAG: " + dag)
            before = baseline([json.loads(row) for row in (evidence / "containers-before.jsonl").read_text().splitlines()])
            snapshot = json.loads((evidence / "public-snapshot.json").read_text())
            if snapshot.get("source_bundle_sha256") != source_identity(repo): raise ValueError("Resume compute implementation differs from accepted source")
            deployment.evidence = evidence / ("resume-" + datetime.now().strftime("%Y%m%d-%H%M%S"))
            deployment.evidence.mkdir(exist_ok=False)
            # Reconcile the small current image layer without rerunning ingestion,
            # the four completed DAGs, or the completed storage recreation.
            deployment.start()
            wait("http://127.0.0.1:19120/api/v2/config")
            verifier = deployment.execute("airflow-scheduler", "/opt/cartco-spark/bin/python", "/opt/airflow/scripts/diagnostics/verify_assessed_catalog.py")
            run(verifier, "resume-catalog-verification.log")
        else:
            run([args.docker, "version"], "docker-version.log")
            if shutil.disk_usage(repo).free < (2 if args.skip_build else 8) * 1024**3:
                raise RuntimeError("Insufficient free build space; preserve all existing data and volumes")
            rows = inventory("containers-before.jsonl")
            before = baseline(rows)
            run(compose + ["config", "--quiet"], "compose-config.log")
            if not args.skip_build and not args.terraform:
                run([args.docker, "build", "-t", "cartco/airflow:infra-p1-base", "-f", str(repo / "infra/shared/Dockerfile.airflow-base"), str(repo)], "base-build.log")
                run(compose + ["build"], "assessed-build.log")
            earlier = [row["ID"] for row in rows if row["Names"].startswith(("cartco-infra-p1-", "cartco-assessed-")) and not row["Names"].startswith(deployment.project + "-") and row.get("State") != "exited"]
            if earlier: run([args.docker, "stop", *earlier], "stop-earlier-phase1.log")
            deployment.start()
            wait(api + "/health", airflow=True)
            run(deployment.execute("airflow-scheduler", "python", "/opt/airflow/scripts/diagnostics/check_assessed_dags.py"), "dag-import-serialization.log")
            run(deployment.execute("airflow-scheduler", "/opt/cartco-spark/bin/python", "-c", "import pyspark,importlib.util; assert pyspark.__version__=='3.5.6'; assert importlib.util.find_spec('airflow') is None; print('Isolated Spark environment verified')"), "compute-isolation.log")
            login()
            for name in ["assessed_orders_ingestion", "assessed_inventory_ingestion", "assessed_commerce_release", "assessed_system_health"]: dag_run(name)
            snapshot = wait("http://127.0.0.1:18100/api/snapshot")
            if not snapshot["quality"]["success"] or snapshot["lineage_transport"]["status"] != "verified": raise ValueError("Published snapshot lacks verified quality/lineage")
            if snapshot.get("source_bundle_sha256") != source_identity(repo):
                raise ValueError("Built image differs from the current assessed source; rebuild before accepting it")
            # Preserve raw JSON snapshot-number strings as exported, not JS numbers.
            (evidence / "public-snapshot.json").write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
            run(deployment.execute("airflow-scheduler", "python", "/opt/airflow/scripts/diagnostics/check_grafana_access.py"), "grafana-role-proof.log")
            auth = {"Authorization": "Basic " + base64.b64encode(b"cartco:cartco-grafana-local-only").decode()}
            wait("http://127.0.0.1:13030/api/health")
            dashboard = request("http://127.0.0.1:13030/api/dashboards/uid/cartco-health", headers=auth)
            if len(dashboard["dashboard"]["panels"]) != 5: raise ValueError("Grafana dashboard panels missing")
            query = {"queries": [{"refId": "A", "datasource": {"type": "postgres", "uid": "cartco-airflow-health"}, "rawSql": "SELECT count(*) AS runs FROM cartco_monitoring.dag_health WHERE dag_id LIKE 'assessed_%'", "format": "table"}], "from": "0", "to": str(int(time.time()*1000))}
            result = request("http://127.0.0.1:13030/api/ds/query", query, "POST", auth)
            if result["results"]["A"].get("error") or not result["results"]["A"].get("frames"): raise ValueError("Grafana SQL datasource query failed")
            (evidence / "grafana-query.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
            verifier = deployment.execute("airflow-scheduler", "/opt/cartco-spark/bin/python", "/opt/airflow/scripts/diagnostics/verify_assessed_catalog.py")
            run(verifier, "catalog-before-recreation.log")
            # Recreate only this stack's catalog/storage containers, keeping volumes.
            # No ingestion or Spark write task runs during this checkpoint.
            deployment.recreate()
            wait("http://127.0.0.1:19120/api/v2/config")
            run(verifier, "catalog-after-recreation.log")
        after_snapshot = request("http://127.0.0.1:18100/api/snapshot")
        if snapshot != after_snapshot:
            raise ValueError("Published snapshot changed during storage recreation")
        # Replay the same immutable deliveries through a real scheduled release.
        # A provider reconciliation may restart the webserver too. Its signed
        # session cookie is ephemeral, so reacquire it rather than reusing it.
        wait(api + "/health", airflow=True)
        login()
        dag_run("assessed_commerce_release")
        replay = request("http://127.0.0.1:18100/api/snapshot")
        for key in ["daily_revenue", "channels", "inventory", "source_bundle_sha256"]:
            if replay[key] != snapshot[key]: raise ValueError("Replay changed business output: " + key)
        for key in ["bronze_orders", "bronze_wire_objects", "bronze_inventory", "silver"]:
            if replay["catalog"][key] != snapshot["catalog"][key]:
                raise ValueError("Replay rewrote retained source/state: " + key)
        run(verifier, "catalog-after-replay.log")
        (evidence / "replay-snapshot.json").write_text(json.dumps(replay, indent=2), encoding="utf-8")
        behavior_path = "/opt/airflow/data/assessed/table-behaviors.json"
        run(deployment.execute("airflow-scheduler", "/opt/cartco-spark/bin/python", "/opt/airflow/scripts/acceptance/prove_assessed_table_behaviors.py", "--output", behavior_path), "table-behaviors.log")
        behavior = json.loads(run(deployment.execute("airflow-scheduler", "cat", behavior_path), "table-behaviors.json"))
        if behavior["before"]["rows_sha256"] != behavior["after"]["rows_sha256"] or behavior["after"]["files"] >= behavior["before"]["files"]:
            raise ValueError("Compaction did not retain exact rows and reduce files")
        deployment.convergence()
        run([args.docker, "ps", "-a", "--filter", "name=" + deployment.project + "-"], "status.log")
        if baseline(inventory("containers-after.jsonl")) != before: raise ValueError("Original baseline container identities changed")
    except Exception:
        try:
            if args.terraform:
                for row in inventory("failure-containers.jsonl"):
                    if row["Names"].startswith(deployment.project + "-"):
                        run([args.docker, "logs", "--tail", "100", row["ID"]], "failure-" + row["Names"] + ".log")
            else:
                run(compose + ["logs", "--tail", "100"], "failure-logs.log")
        except Exception: pass
        raise
    finally:
        deployment.finish()
    (evidence / "SUCCESS.txt").write_text("Assessed scheduled ingestion, Spark medallion, GE, Marquez, Grafana, recreation, replay and dashboard acceptance passed.\n", encoding="utf-8")
    print("Assessed stack acceptance passed:", evidence, flush=True)


if __name__ == "__main__": main()
