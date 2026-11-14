"""Exercise old/new CLI paths, module aliases and individually copied tools."""
import importlib
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
LAYOUT = json.loads((ROOT / "scripts/layout.json").read_text())["compatibility_entrypoints"]


@pytest.mark.parametrize("name", [
    "run_assessed_stack.py", "phase1_spark_smoke.py", "prove_assessed_recovery.py",
    "serve_dashboard.py", "dev.py",
])
def test_original_and_grouped_help_work_outside_repository_without_docker(name, tmp_path):
    outputs = []
    for relative in [name, LAYOUT[name]]:
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / relative), "--help"],
            cwd=tmp_path, env={**os.environ, "PATH": ""}, capture_output=True,
            text=True, encoding="utf-8", timeout=30,
        )
        assert result.returncode == 0, result.stderr
        assert "usage:" in result.stdout
        outputs.append(result.stdout)
    assert outputs[0] == outputs[1]


@pytest.mark.parametrize("name", ["volume_inventory", "recovery_worker_scenario", "assessed_deployment"])
def test_original_module_is_an_alias_so_function_patches_reach_implementation(name):
    original = importlib.import_module("scripts." + name)
    canonical = importlib.import_module("scripts." + LAYOUT[name + ".py"][:-3].replace("/", "."))
    assert original is canonical


@pytest.mark.parametrize("name", ["volume_inventory.py", "serve_dashboard.py", "phase1_spark_smoke.py"])
def test_standalone_container_tool_needs_no_repository_layout(name, tmp_path):
    copied = tmp_path / name
    shutil.copyfile(ROOT / "scripts" / LAYOUT[name], copied)
    result = subprocess.run(
        [sys.executable, str(copied), "--help"], cwd=tmp_path,
        env={**os.environ, "PATH": ""}, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_standalone_airflow_initializer_can_be_imported_without_running_airflow(tmp_path):
    copied = tmp_path / "initialize_airflow.py"
    shutil.copyfile(ROOT / "scripts/deployment/initialize_airflow.py", copied)
    assert callable(runpy.run_path(str(copied))["initialize"])
