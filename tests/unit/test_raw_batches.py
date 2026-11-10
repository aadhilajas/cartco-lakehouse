import json
import os
from pathlib import Path
import tempfile

import pytest

from pipeline.raw_batches import archive_csv


@pytest.fixture
def tmp_path(tmp_path_factory):
    if os.name != "nt":
        return tmp_path_factory.mktemp("raw-batch")
    scratch = Path(__file__).resolve().parents[3] / "work" / "r"
    scratch.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="t", dir=scratch))


def test_identical_input_reuses_manifest_without_replacing_it(tmp_path):
    source = tmp_path / "source.csv"
    content = b'order_id,note\r\no1,"a,b"\r\n'
    source.write_bytes(content)
    one = archive_csv(source, tmp_path / "archive", source="shopify", batch_date="2026-01-01")
    manifest = (one.directory / "manifest.json").read_bytes()
    two = archive_csv(source, tmp_path / "archive", source="shopify", batch_date="2026-01-01")
    assert one.batch_id == two.batch_id
    assert (two.directory / "source.csv").read_bytes() == content
    assert (two.directory / "manifest.json").read_bytes() == manifest
    assert two.rows[0]["note"] == "a,b"


def test_changed_content_or_batch_date_gets_a_new_archive(tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("order_id\no1\n")
    one = archive_csv(source, tmp_path / "archive", source="shopify", batch_date="2026-01-01")
    source.write_text("order_id\no2\n")
    two = archive_csv(source, tmp_path / "archive", source="shopify", batch_date="2026-01-01")
    three = archive_csv(source, tmp_path / "archive", source="shopify", batch_date="2026-01-02")
    assert len({one.batch_id, two.batch_id, three.batch_id}) == 3
    assert (one.directory / "source.csv").read_text() == "order_id\no1\n"


def test_tampered_archive_is_detected_and_not_silently_repaired(tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("order_id\no1\n")
    batch = archive_csv(source, tmp_path / "archive", source="shopify", batch_date="2026-01-01")
    (batch.directory / "source.csv").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="Immutable batch artifact differs"):
        archive_csv(source, tmp_path / "archive", source="shopify", batch_date="2026-01-01")
    assert (batch.directory / "source.csv").read_bytes() == b"tampered"


@pytest.mark.parametrize("content", [b"order_id,order_id\no1,o2\n", b"order_id\no1,extra\n", b"\xff"])
def test_malformed_csv_is_retained_with_a_rejected_manifest(tmp_path, content):
    source = tmp_path / "source.csv"
    source.write_bytes(content)
    with pytest.raises((ValueError, UnicodeDecodeError)):
        archive_csv(source, tmp_path / "archive", source="shopify", batch_date="2026-01-01")
    raw = next((tmp_path / "archive").rglob("source.csv"))
    assert raw.read_bytes() == content
    assert json.loads(raw.with_name("manifest.json").read_text())["rows"] is None


def test_missing_file_produces_no_success_manifest(tmp_path):
    with pytest.raises(FileNotFoundError):
        archive_csv(tmp_path / "absent.csv", tmp_path / "archive", source="shopify", batch_date="2026-01-01")
    assert not (tmp_path / "archive").exists()


def test_concurrent_archive_attempts_publish_one_identical_manifest(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    source = tmp_path / "source.csv"
    source.write_text("order_id\no1\n")
    def archive(_):
        return archive_csv(source, tmp_path / "archive", source="shopify", batch_date="2026-01-01")
    with ThreadPoolExecutor(max_workers=4) as pool:
        batches = list(pool.map(archive, range(8)))
    assert len({batch.batch_id for batch in batches}) == 1
    assert len(list((tmp_path / "archive").rglob("manifest.json"))) == 1
