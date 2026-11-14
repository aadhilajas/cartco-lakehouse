"""Failure boundaries for the new compatibility harness, without a fake Spark pass."""
import hashlib
from io import BytesIO
import json

import pytest

from infra.phase1 import fetch_jars
from scripts.phase1_spark_smoke import checkpoint, require, wait_http
from scripts.summarize_infrastructure_resources import summarize


def artifact(tmp_path, **overrides):
    item = {"name": "test.jar", "url": "https://repo.maven.apache.org/maven2/test.jar", "sha512": hashlib.sha512(b"verified").hexdigest()}
    item.update(overrides)
    path = tmp_path / "artifacts.json"
    path.write_text(json.dumps({"jars": [item]}), encoding="utf-8")
    return path


def test_locked_artifact_preserves_verified_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch_jars, "urlopen", lambda *a, **kw: BytesIO(b"verified"))
    fetch_jars.fetch(artifact(tmp_path), tmp_path / "jars")
    assert (tmp_path / "jars/test.jar").read_bytes() == b"verified"


def test_corrupt_jar_is_rejected_and_not_left_loadable(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch_jars, "urlopen", lambda *a, **kw: BytesIO(b"corrupt"))
    with pytest.raises(ValueError, match="checksum mismatch"):
        fetch_jars.fetch(artifact(tmp_path), tmp_path / "jars")
    assert not (tmp_path / "jars/test.jar").exists()


@pytest.mark.parametrize("overrides", [{"name": "../escape.jar"}, {"url": "http://repo.maven.apache.org/test.jar"}, {"url": "https://repo.maven.apache.org.evil.invalid/test.jar"}])
def test_untrusted_artifact_location_never_downloads(tmp_path, monkeypatch, overrides):
    def forbidden(*a, **kw):
        pytest.fail("Untrusted location reached transport")
    monkeypatch.setattr(fetch_jars, "urlopen", forbidden)
    with pytest.raises(ValueError, match="Invalid locked artifact"):
        fetch_jars.fetch(artifact(tmp_path, **overrides), tmp_path / "jars")


def test_restart_evidence_cannot_inject_table_identifiers():
    with pytest.raises(RuntimeError, match="Unsafe probe name"):
        checkpoint(None, "nessie.phase1_checks.probe_x; DROP TABLE baseline")


def test_acceptance_requirements_fail_closed():
    with pytest.raises(RuntimeError, match="incorrect"):
        require(False, "incorrect")
    with pytest.raises(RuntimeError, match="Readiness timeout"):
        wait_http("http://invalid.invalid", seconds=0)


def test_resource_summary_uses_concurrent_samples_not_sum_of_peaks():
    lines = [json.dumps({"observed_at": time, "stats": {"Name": name, "MemUsage": used + " / 1GiB"}})
             for time, name, used in [
                 ("a", "cartco-infra-p1-spark-smoke-run-a", "300MiB"),
                 ("a", "cartco-infra-p1-nessie-1", "200MiB"),
                 ("b", "cartco-infra-p1-spark-smoke-run-a", "100MiB"),
                 ("b", "cartco-infra-p1-nessie-1", "400MiB")]]
    assert summarize(lines)["observed_peak_stack_bytes"] == 500 * 1024**2


def test_resource_summary_refuses_idle_only_evidence():
    with pytest.raises(ValueError, match="running Spark"):
        summarize([])
