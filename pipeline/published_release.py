"""Checksum-verified release reader shared by isolated runtime environments."""
import hashlib
import json
from pathlib import Path

def read_release(root):
    root = Path(root)
    pointer = json.loads((root / "CURRENT.json").read_bytes())
    import re
    if not re.fullmatch(r"r[0-9a-f]{32}", pointer["release_id"]):
        raise ValueError("Unsafe publication pointer")
    folder = root / "releases" / pointer["release_id"]
    raw = (folder / "manifest.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != pointer["manifest_sha256"]:
        raise ValueError("Published manifest checksum differs")
    manifest = json.loads(raw)
    raw = (folder / "candidate.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest["candidate_sha256"]:
        raise ValueError("Published candidate checksum differs")
    return manifest, json.loads(raw)
