"""Actual local HTTP, SSH/SFTP and Kafka acceptance; writes a delivery manifest."""

# Support direct CLI execution and imports from any working directory.
import sys as _sys
from pathlib import Path as _Path
_SCRIPT_PATH = _Path(__file__).resolve()
_REPOSITORY_ROOT = next(
    (parent for parent in _SCRIPT_PATH.parents
     if (parent / "pipeline/source_identity.py").is_file()
     and (parent / "scripts/layout.json").is_file()),
    _SCRIPT_PATH.parent,  # Individually copied stdlib tools do not need a repo.
)
if str(_REPOSITORY_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPOSITORY_ROOT))
import argparse
import csv
from datetime import date, timedelta
import hashlib
import io
import json
from pathlib import Path
import sys
from threading import Thread
from uuid import uuid4

sys.path.insert(0, str(_REPOSITORY_ROOT))
from ingestion.protocols import amazon, shopify, sftp
from ingestion.protocols.common import JsonHTTP, encoded
from ingestion.protocols.fixtures import http_server
from ingestion.protocols.kafka_pos import open_consumer, consume
from ingestion.protocols.sftp_fixture import SFTPFixture
from pipeline.pos import pos_order_id
from pipeline.raw_batches import _write_once
from pipeline.source_contracts import daily_contract


def acknowledge(path):
    complete = json.loads(path.with_name("COMPLETE.json").read_text())
    if hashlib.sha256(path.read_bytes()).hexdigest() != complete["csv_sha256"]:
        raise ValueError("Landing checksum changed")
    _write_once(path.with_name("LANDED.json"), encoded({"delivery_id": complete["delivery_id"], "status": "durable_raw_landing"}))
    return True


def pos_rows(day):
    base = dict(order_id=pos_order_id("STORE-001", "TILL-01", "RECEIPT-001"), store_id="STORE-001", terminal_id="TILL-01", transaction_id="RECEIPT-001", channel="pos", customer_id="",
        order_date=f"{day}T05:00:00Z", sku="POS-001", product_category="Apparel", quantity="1", unit_price="10.50", order_total="10.50", line_count="1", tax_amount="0.00", discount_amount="0.00", status="paid", currency="INR", source_updated_at=f"{day}T06:00:00Z", is_deleted="false")
    older = {**base, "source_updated_at": f"{date.fromisoformat(day) - timedelta(days=1)}T06:00:00Z"}
    changed = {**base, "source_updated_at": f"{day}T06:15:00Z", "status": "refunded"}
    deleted = {**base, "source_updated_at": f"{day}T06:30:00Z", "is_deleted": "true"}
    second = {**base, "order_id": pos_order_id("STORE-001", "TILL-01", "RECEIPT-002"), "transaction_id": "RECEIPT-002", "sku": "POS-002", "quantity": "3", "unit_price": "6.75", "order_total": "20.25"}
    return [base, base, older, changed, deleted, second]


def write_inventory(folder, day):
    folder.mkdir(parents=True, exist_ok=True)
    skus = [f"shopify:gid://shopify/Product/{i}" for i in range(1, 4)] + [f"amazon:SYNTH{i:05d}" for i in range(1, 4)] + ["pos:POS-001", "pos:POS-002"]
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(["sku", "location_id", "snapshot_date", "on_hand", "unit_cost", "currency"])
    for snapshot in [str(date.fromisoformat(day) - timedelta(days=1)), day]:
        writer.writerows([sku, "DEMO-WH-1", snapshot, 10, "5.00", "INR"] for sku in skus)
    data = stream.getvalue().encode()
    (folder / "inventory.csv").write_bytes(data)
    marker = dict(schema_version="inventory.sftp.v1", file="inventory.csv", sha256=hashlib.sha256(data).hexdigest(), bytes=len(data), rows=2 * len(skus))
    (folder / "delivery.ready.json").write_bytes(encoded(marker))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="127.0.0.1:19092")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-date", default="2026-11-12")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    # API fixture data has an explicit fixed as-of date for reproducibility.
    if args.batch_date != "2026-11-12":
        parser.error("This acceptance fixture is versioned for 2026-11-12")
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    topic = "cartco-pos-acceptance-" + uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap, request_timeout_ms=15000)
    admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
    admin.close()
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", retries=5, request_timeout_ms=15000)
    for index, row in enumerate(pos_rows(args.batch_date)):
        producer.send(topic, encoded(row), partition=index % 2).get(timeout=20)
    producer.flush(timeout=20)
    producer.close()
    contract = daily_contract("pos", args.batch_date)
    paths = {}
    for label in ["first", "replay", "resume"]:
        consumer = open_consumer(args.bootstrap, topic + "-" + ("first" if label == "resume" else label))
        try:
            paths[label] = consume(consumer, topic=topic, root=args.output, contract=contract, batch_date=args.batch_date, accept=acknowledge)
            from kafka import TopicPartition
            parts = [TopicPartition(topic, i) for i in consumer.partitions_for_topic(topic)]
            assert all(consumer.committed(part) == consumer.end_offsets(parts)[part] for part in parts)
        finally:
            consumer.close()
    assert paths["first"] == paths["replay"] == paths["resume"]
    consumer = open_consumer(args.bootstrap, topic + "-first")
    try:
        try:
            consume(consumer, topic=topic, root=args.output / "missing-local-landing", contract=contract, batch_date=args.batch_date, accept=acknowledge)
        except ValueError as exc:
            if "no retained durable landing" not in str(exc): raise
        else:
            raise AssertionError("Broker checkpoint without local landing was accepted")
    finally:
        consumer.close()
    server = http_server(throttle_once=True)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deliveries = {"pos": str(paths["first"])}
    try:
        endpoint = f"http://127.0.0.1:{server.server_port}"
        for source, module, path in [("shopify", shopify, "/admin/api/2026-10/graphql.json"), ("amazon", amazon, "")]:
            http = JsonHTTP(endpoint + path, root=args.output, source=source)
            contract = daily_contract(source, args.batch_date)
            result = module.extract(http, contract=contract, batch_date=args.batch_date)
            assert module.extract(http, contract=contract, batch_date=args.batch_date) == result
            deliveries[source] = str(result)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
    folder = args.output / "fixture-inventory"
    write_inventory(folder, args.batch_date)
    with SFTPFixture(folder) as server:
        hosts = args.output / "fixture_known_hosts"
        hosts.write_text(server.known_hosts(), encoding="utf-8")
        config = dict(host=server.host, port=server.port, username=server.username, password=server.password, known_hosts=hosts, remote_manifest="/delivery.ready.json", root=args.output)
        result = sftp.extract(**config)
        assert sftp.extract(**config) == result
        deliveries["sftp"] = str(result)
    manifest = {"schema_version": 1, "synthetic": True, "batch_date": args.batch_date, "deliveries": deliveries,
        "kafka": {"broker": args.bootstrap, "topic": topic, "partitions": 2, "records": 6, "replay_identical": True, "offsets_committed_after_landing": True,
                  "resume_receipt_verified": True, "missing_landing_refused": True}}
    (args.output / "protocol-acceptance.json").write_bytes(encoded(manifest))
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
