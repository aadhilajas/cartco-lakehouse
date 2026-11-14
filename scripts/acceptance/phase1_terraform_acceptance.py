"""Live checks for an already-applied isolated Terraform foundation.

Uses Terraform itself to replace catalog/storage containers. Named volumes are
retained. This does not provision a VM or operate Compose-owned resources.
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
import json
from pathlib import Path
import re
import subprocess
import time
from urllib.request import urlopen

from scripts.acceptance.phase1_spark_smoke import wait_http


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--terraform", default="terraform")
    parser.add_argument("--project", default="cartco-infra-p1-tf")
    parser.add_argument("--docker-host")
    parser.add_argument("--buildx-builder", default="default")
    parser.add_argument("--evidence", type=Path, default=Path("test-results/terraform-live"))
    args = parser.parse_args()
    if not re.fullmatch(r"cartco-infra-p1-tf(?:-[a-z0-9]+)?", args.project):
        parser.error("Refusing non-isolated project name")
    repo = _REPOSITORY_ROOT
    args.evidence = (repo / args.evidence).resolve()
    args.evidence.mkdir(parents=True, exist_ok=True)
    (args.evidence / "SUCCESS.txt").unlink(missing_ok=True)
    spec = json.loads((repo / "infra/compatibility/stack.json").read_text(encoding="utf-8"))

    def run(command, name):
        result = subprocess.run(command, cwd=repo, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
        (args.evidence / name).write_bytes(result.stdout)
        if result.returncode:
            raise RuntimeError(f"Command failed ({result.returncode}); inspect {args.evidence / name}")
        return result.stdout.decode("utf-8")

    def container_id(service):
        result = run([args.docker, "inspect", "--format", "{{.Id}}", args.project + "-" + service], "identity-" + service + ".log")
        return result.strip()

    def baseline_identity(name):
        raw = run([args.docker, "ps", "-a", "--format", "{{json .}}"], name)
        return sorted((row["Names"], row["ID"]) for row in map(json.loads, raw.splitlines())
                      if row["Names"].startswith("cartco-") and not row["Names"].startswith("cartco-infra-p1-"))

    def services_ready():
        for url in ["http://127.0.0.1:19000/minio/health/ready", "http://127.0.0.1:19120/api/v2/config", "http://127.0.0.1:15000/api/v1/namespaces"]:
            wait_http(url, seconds=300)
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            try:
                with urlopen("http://127.0.0.1:18080/health", timeout=5) as response:
                    health = json.load(response)
                if all(health[key]["status"] == "healthy" for key in ["metadatabase", "scheduler"]):
                    return
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError("Terraform Airflow health did not settle")

    smoke = spec["services"]["spark-smoke"]
    probe = [args.docker, "run", "--rm", "--network", args.project + "-network", "--memory", smoke["mem_limit"]]
    for key, value in smoke["environment"].items():
        probe += ["-e", key + "=" + value]
    probe += ["-v", args.project + "-evidence-data:/evidence"]
    try:
        baseline = baseline_identity("baseline-before.jsonl")
        services_ready()
        run(probe + [smoke["image"]], "spark-create.log")
        services = ["nessie", "catalog-db", "minio"]
        before = {service: container_id(service) for service in services}
        # The smoke writer exited; quiesce the catalog before its database restarts.
        run([args.docker, "stop", args.project + "-nessie"], "quiesce-catalog.log")
        replace = [args.terraform, "-chdir=" + str(repo / "infra/terraform"), "apply", "-input=false", "-auto-approve",
                   "-var=project_name=" + args.project, "-var=buildx_builder=" + args.buildx_builder]
        if args.docker_host:
            replace += ["-var=docker_host=" + args.docker_host]
        replace += ['-replace=docker_container.service["' + service + '"]' for service in services]
        run(replace, "terraform-recreation.log")
        services_ready()
        after = {service: container_id(service) for service in services}
        if any(before[service] == after[service] for service in services):
            raise RuntimeError("Terraform recreation not proven")
        run(probe + [smoke["image"], "--verify"], "spark-restart-verify.log")
        run([args.docker, "exec", args.project + "-airflow-scheduler", "python", "/opt/airflow/scripts/diagnostics/check_dags.py"], "dag-imports.log")
        for name in ["compatibility.json", "restart-verification.json"]:
            raw = run(probe + ["--entrypoint", "cat", smoke["image"], "/evidence/" + name], "copy-" + name + ".log")
            (args.evidence / name).write_text(json.dumps(json.loads(raw), indent=2) + "\n", encoding="utf-8")
        if baseline_identity("baseline-after.jsonl") != baseline:
            raise RuntimeError("Baseline container identities changed")
        (args.evidence / "recreated-containers.json").write_text(json.dumps({"before": before, "after": after}, indent=2), encoding="utf-8")
        (args.evidence / "SUCCESS.txt").write_text("Terraform-controlled recreation and Spark compatibility passed. ARM/cloud hosting remain separate.\n", encoding="utf-8")
        print(f"Terraform live acceptance passed: {args.evidence}")
    except Exception:
        try:
            run([args.docker, "ps", "-a"], "failure-status.log")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
