"""Initialization checks included in the unit-test CI job."""
from types import SimpleNamespace

import pytest

from scripts.initialize_airflow import initialize


def runner_for(results):
    calls = []
    def runner(args, **kwargs):
        calls.append(args)
        code, output = results.pop(0)
        return SimpleNamespace(returncode=code, stdout=output)
    return runner, calls


@pytest.mark.parametrize("results,expected_calls", [([(1, "secret")], 1), ([(0, ""), (1, "")], 2), ([(0, ""), (0, "invalid")], 2), ([(0, ""), (0, "{}")], 2)])
def test_failed_setup_stops_before_creation(results, expected_calls):
    runner, calls = runner_for(results)
    with pytest.raises(RuntimeError):
        initialize(runner, {})
    assert len(calls) == expected_calls


def test_existing_user_is_preserved():
    runner, calls = runner_for([(0, ""), (0, '[{"username":"airflow"}]')])
    initialize(runner, {})
    assert len(calls) == 2


def test_creation_failure_is_not_hidden_or_disclosed():
    runner, calls = runner_for([(0, ""), (0, "[]"), (2, "sensitive-password")])
    with pytest.raises(RuntimeError, match="administrator creation failed") as caught:
        initialize(runner, {"CARTCO_AIRFLOW_ADMIN_PASSWORD": "sensitive-password"})
    assert "sensitive-password" not in str(caught.value)
    assert len(calls) == 3


def test_empty_database_creates_requested_user():
    runner, calls = runner_for([(0, ""), (0, "[]"), (0, "")])
    initialize(runner, {"CARTCO_AIRFLOW_ADMIN_USERNAME": "operator"})
    assert calls[-1][calls[-1].index("--username") + 1] == "operator"
