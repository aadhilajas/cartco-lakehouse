"""Task entrypoints for the assessed stack; no Docker socket or merchant secrets."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
from urllib.request import urlopen

from pipeline.source_contracts import daily_contract
from ingestion.protocols.common import JsonHTTP
from ingestion.protocols import shopify, amazon, sftp
from ingestion.protocols.kafka_pos import open_consumer, consume

DAY = "2026-11-12"  # Versioned synthetic demonstration date, not today's merchant data.


def root():
    return Path(os.environ.get("CARTCO_ASSESSED_ROOT", "/opt/airflow/data/assessed"))


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write((json.dumps(value, sort_keys=True, indent=2) + "\n").encode())
        stream.flush()
        os.fsync(stream.fileno())
    try: os.replace(temporary, path)
    finally: temporary.unlink(missing_ok=True)


def orders(source):
    contract = daily_contract(source, DAY)
    output = root() / "protocols"
    if source in {"shopify", "amazon"}:
        path = "/admin/api/2026-10/graphql.json" if source == "shopify" else ""
        http = JsonHTTP(os.environ.get("CARTCO_SOURCE_API", "http://source-api:18081") + path, root=output, source=source)
        result = (shopify if source == "shopify" else amazon).extract(http, contract=contract, batch_date=DAY)
    elif source == "pos":
        from scripts.accept_protocols import acknowledge
        topic = os.environ.get("CARTCO_POS_TOPIC", "cartco.synthetic.pos.20261112.v1")
        consumer = open_consumer(os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"), "cartco.assessed.pos.20261112.v1")
        try:
            result = consume(consumer, topic=topic, root=output, contract=contract, batch_date=DAY, accept=acknowledge)
        finally:
            consumer.close()
    else:
        raise ValueError("Unknown assessed order source")
    atomic_json(root() / "ready" / (source + ".json"), {"batch_date": DAY, "path": str(result)})
    return str(result)


def inventory():
    result = sftp.extract(host="sftp-fixture", port=12222, username="fixture", password="fixture-local-only",
        known_hosts=root() / "fixtures/known_hosts", remote_manifest="/delivery.ready.json", root=root() / "protocols")
    atomic_json(root() / "ready/inventory.json", {"batch_date": DAY, "path": str(result)})
    return str(result)


def combine_orders():
    values = {source: json.loads((root() / "ready" / (source + ".json")).read_text()) for source in ["shopify", "amazon", "pos"]}
    if any(row["batch_date"] != DAY for row in values.values()): raise ValueError("Order batch dates differ")
    atomic_json(root() / "ready/orders.json", {"batch_date": DAY, "deliveries": {source: row["path"] for source, row in values.items()}})


def prepare_manifest():
    orders = json.loads((root() / "ready/orders.json").read_text())
    inventory = json.loads((root() / "ready/inventory.json").read_text())
    if orders["batch_date"] != DAY or inventory["batch_date"] != DAY:
        raise ValueError("Source batch dates differ")
    manifest = {"schema_version": 1, "synthetic": True, "batch_date": DAY,
        "deliveries": {**orders["deliveries"], "sftp": inventory["path"]}}
    for path in manifest["deliveries"].values():
        if not Path(path).with_name("COMPLETE.json").is_file(): raise ValueError("Incomplete source delivery")
    atomic_json(root() / "protocols/protocol-acceptance.json", manifest)


def compute():
    repo = Path(__file__).resolve().parents[1]
    subprocess.run([os.environ.get("CARTCO_SPARK_PYTHON", "/opt/cartco-spark/bin/python"), str(repo / "scripts/runtime/run_spark_commerce.py"),
        "--manifest", str(root() / "protocols/protocol-acceptance.json"), "--candidate", str(root() / "candidate.json")],
        check=True, timeout=1200, cwd=repo)


def publish():
    from pipeline.assessed_release import publish
    return publish(root() / "candidate.json", root() / "published")["release_id"]


def lineage():
    from pipeline.assessed_lineage import emit_and_verify
    candidate = json.loads((root() / "candidate.json").read_text())
    folder = root() / "lineage"
    folder.mkdir(parents=True, exist_ok=True)
    return emit_and_verify(candidate, endpoint=os.environ.get("MARQUEZ_URL", "http://marquez-api:5000"), output=folder / (candidate["release_id"] + ".json"))["status"]


def export():
    from pipeline.dashboard_snapshot import export
    candidate = json.loads((root() / "candidate.json").read_text())
    return str(export(root() / "published", root() / "public-snapshot.json", lineage_report=root() / "lineage" / (candidate["release_id"] + ".json")))


def health():
    from pipeline.assessed_release import read_release
    urls = [os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2") + "/config",
            os.environ.get("MARQUEZ_URL", "http://marquez-api:5000") + "/api/v1/namespaces"]
    for url in urls:
        with urlopen(url, timeout=10) as response:
            if response.status != 200: raise RuntimeError("Assessed dependency is unhealthy")
    manifest, _ = read_release(root() / "published")
    print("Verified published manifest and dependency health:", manifest["release_id"])
