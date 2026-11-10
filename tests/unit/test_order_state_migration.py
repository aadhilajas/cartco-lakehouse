from datetime import datetime, timezone
import json

import pyarrow.parquet as pq
import pytest

from order_state_helpers import events, order
from scripts.prepare_order_state import prepare_candidate


def inputs(tmp_path):
    full = events(order())
    legacy, versions = tmp_path / "legacy.parquet", tmp_path / "versions.parquet"
    pq.write_table(full.select(full.column_names[:11]), legacy)
    pq.write_table(full.select(["channel", "order_id", "source_updated_at"]), versions)
    return legacy, versions


def test_migration_candidate_has_verified_counts_and_preserves_sources(tmp_path):
    legacy, versions = inputs(tmp_path)
    before = legacy.read_bytes(), versions.read_bytes()
    output = tmp_path / "candidate.parquet"
    report = prepare_candidate(legacy, versions, output, applied_at=datetime(2026, 11, 10, tzinfo=timezone.utc), currency="INR", business_timezone="UTC")
    assert report["published"] is False
    assert report["state_rows"] == report["active_rows"] == 1
    assert (legacy.read_bytes(), versions.read_bytes()) == before
    assert pq.read_table(output)["source_updated_at"].to_pylist() == [datetime(2026, 1, 1)]
    assert json.loads(output.with_suffix(".parquet.manifest.json").read_text())["candidate_sha256"] == report["candidate_sha256"]


def test_migration_refuses_overwriting_an_input_or_existing_output(tmp_path):
    legacy, versions = inputs(tmp_path)
    audit = datetime(2026, 11, 10, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="differ from its inputs"):
        prepare_candidate(legacy, versions, legacy, applied_at=audit, currency="INR", business_timezone="UTC")
    output = tmp_path / "candidate.parquet"
    output.write_bytes(b"keep this")
    with pytest.raises(FileExistsError):
        prepare_candidate(legacy, versions, output, applied_at=audit, currency="INR", business_timezone="UTC")
    assert output.read_bytes() == b"keep this"


def test_invalid_baseline_produces_no_candidate(tmp_path):
    legacy, versions = inputs(tmp_path)
    wrong = events(order("different"))
    pq.write_table(wrong.select(["channel", "order_id", "source_updated_at"]), versions)
    output = tmp_path / "candidate.parquet"
    with pytest.raises(ValueError, match="match legacy keys exactly"):
        prepare_candidate(legacy, versions, output, applied_at=datetime.now(timezone.utc), currency="INR", business_timezone="UTC")
    assert not output.exists()
