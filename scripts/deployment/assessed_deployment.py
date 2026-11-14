"""Host deployment commands shared by real assessed acceptance modes."""

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
import os
import json
import subprocess
import time


class Deployment:
    def __init__(self, *, repo, evidence, docker, terraform, run):
        self.repo, self.evidence, self.docker = repo, evidence, docker
        self.terraform, self.run = terraform, run
        self.previous_workspace = None
        self.compose = [docker, "compose", "-p", "cartco-assessed", "-f", str(repo / "infra/assessed/stack.json")]
        self.project = "cartco-infra-p1-tf-assessed" if terraform else "cartco-assessed"

    def execute(self, service, *command):
        if self.terraform:
            return [self.docker, "exec", self.project + "-" + service, *command]
        return self.compose + ["exec", "-T", service, *command]

    def tf(self, *command):
        return [self.terraform, "-chdir=" + str(self.repo / "infra/terraform"), *command]

    def variables(self):
        result = ["-var=stack_spec=../assessed/stack.json", "-var=project_name=" + self.project]
        if os.name == "nt":
            result += ["-var=docker_host=npipe:////./pipe/dockerDesktopLinuxEngine", "-var=buildx_builder=default"]
        return result

    def start(self):
        if not self.terraform:
            self.run(self.compose + ["up", "-d"], "up.log")
            return
        self.run(self.tf("init", "-backend=false"), "terraform-init.log")
        self.previous_workspace = self.run(self.tf("workspace", "show"), "terraform-previous-workspace.log").strip()
        self.run(self.tf("workspace", "select", "-or-create", "assessed"), "terraform-workspace.log")
        self.run(self.tf("validate"), "terraform-validate.log")
        self.run(self.tf("test"), "terraform-tests.log")
        plan = str(self.evidence / "assessed.tfplan")
        self.run(self.tf("plan", *self.variables(), "-out=" + plan), "terraform-plan.log")
        self.run(self.tf("apply", "-auto-approve", plan), "terraform-apply.log")
        # Terraform creates services concurrently; wait for the same finite
        # initializers that Compose gates with completed-successfully dependencies.
        deadline = time.monotonic() + 300
        pending = {name for name in ["runtime-init", "fixture-seed", "airflow-init", "grafana-init"]}
        states = {}
        while pending and time.monotonic() < deadline:
            for name in list(pending):
                result = subprocess.run([self.docker, "inspect", "--format", "{{json .State}}", self.project + "-" + name],
                                        check=True, capture_output=True, text=True)
                states[name] = json.loads(result.stdout)
                if states[name]["Status"] == "exited" and states[name]["ExitCode"] == 0: pending.remove(name)
            if pending: time.sleep(2)
        (self.evidence / "terraform-initializer-states.json").write_text(json.dumps(states, indent=2), encoding="utf-8")
        if pending: raise TimeoutError("Terraform initializers did not complete: " + ", ".join(sorted(pending)))

    def finish(self):
        if self.terraform and self.previous_workspace and self.previous_workspace != "assessed":
            self.run(self.tf("workspace", "select", self.previous_workspace), "terraform-restore-workspace.log")

    def convergence(self):
        if self.terraform:
            # Exit 2 means unresolved drift and must not receive SUCCESS.txt.
            self.run(self.tf("plan", "-detailed-exitcode", *self.variables()), "terraform-convergence.log")

    def recreate(self):
        if self.terraform:
            self.run([self.docker, "stop", self.project + "-nessie"], "stop-catalog.log")
            plan = str(self.evidence / "recreation.tfplan")
            replacements = ['-replace=docker_container.service["' + name + '"]' for name in ["catalog-db", "minio", "nessie"]]
            self.run(self.tf("plan", *self.variables(), *replacements, "-out=" + plan), "terraform-recreation-plan.log")
            self.run(self.tf("apply", "-auto-approve", plan), "terraform-recreation-apply.log")
        else:
            self.run(self.compose + ["stop", "nessie"], "stop-catalog.log")
            self.run(self.compose + ["up", "-d", "--no-deps", "--force-recreate", "catalog-db", "minio"], "recreate-storage.log")
            self.run(self.compose + ["up", "-d", "--force-recreate", "nessie"], "recreate-catalog.log")
