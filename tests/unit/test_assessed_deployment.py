"""Guard isolated Terraform state selection and fail-closed initializer readiness."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location("assessed_deployment", Path(__file__).resolve().parents[2] / "scripts/assessed_deployment.py")
deployment_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deployment_module)


def deployment(tmp_path, monkeypatch, state):
    commands = []
    def run(command, name):
        commands.append((command, name))
        return "default\n" if name == "terraform-previous-workspace.log" else ""
    monkeypatch.setattr(deployment_module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps(state)))
    value = deployment_module.Deployment(repo=tmp_path, evidence=tmp_path, docker="docker", terraform="terraform", run=run)
    return value, commands


def test_real_apply_commands_use_separate_workspace_saved_plan_and_restore_selection(tmp_path, monkeypatch):
    value, commands = deployment(tmp_path, monkeypatch, {"Status": "exited", "ExitCode": 0})
    value.start()
    value.finish()
    plan = next(command for command, name in commands if name == "terraform-plan.log")
    assert "-var=stack_spec=../local/stack.json" in plan
    assert "-var=project_name=cartco-infra-p1-tf-assessed" in plan
    applied = next(command for command, name in commands if name == "terraform-apply.log")
    assert applied[-1] == str(tmp_path / "assessed.tfplan")
    assert commands[-1][0][-3:] == ["workspace", "select", "default"]
    states = json.loads((tmp_path / "terraform-initializer-states.json").read_text())
    assert set(states) == {"runtime-init", "fixture-seed", "airflow-init", "grafana-init"}


def test_failed_initializers_cannot_count_as_ready(tmp_path, monkeypatch):
    value, _ = deployment(tmp_path, monkeypatch, {"Status": "exited", "ExitCode": 1})
    ticks = iter([0, 301])
    monkeypatch.setattr(deployment_module.time, "monotonic", lambda: next(ticks))
    with pytest.raises(TimeoutError, match="initializers did not complete"):
        value.start()
