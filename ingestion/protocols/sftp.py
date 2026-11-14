"""Read an explicitly completed inventory delivery over verified SSH/SFTP."""
import csv
import hashlib
import io
import json
from pathlib import PurePosixPath, Path
import stat

from ingestion.protocols.common import MAX_RESPONSE_BYTES, encoded, retain
from pipeline.raw_batches import _write_once
from pipeline.batch import resolve_batch_date
from pipeline.money import exact_money

INVENTORY_COLUMNS = {"sku", "location_id", "snapshot_date", "on_hand", "unit_cost", "currency"}


def read_file(sftp, path):
    before = sftp.stat(path)
    if not stat.S_ISREG(before.st_mode) or not 0 <= before.st_size <= MAX_RESPONSE_BYTES:
        raise ValueError("SFTP input must be a bounded regular file")
    with sftp.open(path, "rb") as stream:
        content = stream.read(MAX_RESPONSE_BYTES + 1)
    after = sftp.stat(path)
    if len(content) != before.st_size or (after.st_size, after.st_mtime) != (before.st_size, before.st_mtime):
        raise ValueError("SFTP file changed while downloading")
    return content


def download(sftp, remote_manifest, *, root):
    if not remote_manifest.endswith(".ready.json"):
        raise ValueError("Only .ready.json delivery markers may be consumed")
    marker = read_file(sftp, remote_manifest)
    manifest = json.loads(marker)
    if set(manifest) != {"schema_version", "file", "sha256", "bytes", "rows"} or manifest["schema_version"] != "inventory.sftp.v1":
        raise ValueError("Invalid SFTP completion marker")
    if type(manifest["bytes"]) is not int or not 0 <= manifest["bytes"] <= MAX_RESPONSE_BYTES or type(manifest["rows"]) is not int or manifest["rows"] < 0:
        raise ValueError("Invalid SFTP marker size/count")
    filename = manifest["file"]
    if not isinstance(filename, str) or PurePosixPath(filename).name != filename or not filename.endswith(".csv") or "\\" in filename:
        raise ValueError("SFTP marker must name a sibling CSV")
    remote = str(PurePosixPath(remote_manifest).parent / filename)
    content = read_file(sftp, remote)
    raw = retain(root, "sftp", content)
    retain(root, "sftp", marker)
    if raw["sha256"] != manifest["sha256"] or len(content) != manifest["bytes"]:
        raise ValueError("SFTP checksum/size differs; delivery not accepted")
    reader = csv.DictReader(io.StringIO(content.decode("utf-8"), newline=""), strict=True)
    if reader.fieldnames is None or set(reader.fieldnames) != INVENTORY_COLUMNS or len(reader.fieldnames) != len(INVENTORY_COLUMNS):
        raise ValueError("SFTP inventory schema differs")
    rows, seen = list(reader), set()
    for row in rows:
        if None in row or any(v is None for v in row.values()):
            raise ValueError("Malformed inventory CSV record")
        key = (row["sku"], row["location_id"], resolve_batch_date(row["snapshot_date"]).isoformat())
        if key in seen or any(not x or x.strip() != x for x in key):
            raise ValueError("Duplicate/noncanonical inventory key")
        seen.add(key)
        if not row["on_hand"].isascii() or not row["on_hand"].isdigit() or int(row["on_hand"]) > 2147483647 or row["currency"] != "INR" or exact_money(row["unit_cost"]) < 0:
            raise ValueError("Invalid inventory quantity/cost/currency")
    if len(rows) != manifest["rows"]:
        raise ValueError("SFTP row count differs")
    # Re-read the marker too; uploading a new generation during download is refused.
    if read_file(sftp, remote_manifest) != marker:
        raise ValueError("SFTP completion marker changed")
    directory = Path(root) / "sftp" / "deliveries" / hashlib.sha256(marker).hexdigest()
    _write_once(directory / "inventory.csv", content)
    _write_once(directory / "COMPLETE.json", encoded(manifest))
    return directory / "inventory.csv"


def extract(*, host, port, username, password, known_hosts, remote_manifest, root):
    import paramiko
    with paramiko.SSHClient() as client:
        client.load_host_keys(str(known_hosts))
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        client.connect(host, port=port, username=username, password=password, timeout=10,
                       auth_timeout=10, banner_timeout=10, look_for_keys=False, allow_agent=False)
        with client.open_sftp() as sftp:
            sftp.get_channel().settimeout(10)
            return download(sftp, remote_manifest, root=root)
