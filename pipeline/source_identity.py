"""Deterministic identity of the assessed Python implementation, stdlib only."""
import hashlib
from pathlib import Path

FILES = [
    "pipeline/source_identity.py", "pipeline/published_release.py", "pipeline/spark_commerce.py", "pipeline/spark_lakehouse.py",
    "pipeline/assessed_release.py", "pipeline/assessed_lineage.py", "pipeline/dashboard_snapshot.py",
    "pipeline/assessed_orchestration.py", "ingestion/protocols/common.py", "ingestion/protocols/shopify.py",
    "ingestion/protocols/amazon.py", "ingestion/protocols/kafka_pos.py", "ingestion/protocols/sftp.py"]


def source_identity(root=None):
    root = Path(root) if root else Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for name in sorted(FILES):
        digest.update(name.encode() + b"\0")
        # Source checkout line-ending conversion must not alter code identity.
        digest.update((root / name).read_bytes().replace(b"\r\n", b"\n") + b"\0")
    return digest.hexdigest()
