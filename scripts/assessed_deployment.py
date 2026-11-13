"""Host deployment commands shared by real assessed acceptance modes."""
import os


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

    def finish(self):
        if self.terraform and self.previous_workspace and self.previous_workspace != "assessed":
            self.run(self.tf("workspace", "select", self.previous_workspace), "terraform-restore-workspace.log")

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
