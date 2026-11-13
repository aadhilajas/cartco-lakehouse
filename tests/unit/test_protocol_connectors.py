from contextlib import contextmanager
import csv
import hashlib
import io
import json
from pathlib import Path
import stat
from threading import Thread
from types import SimpleNamespace

import pytest

from ingestion.protocols import amazon, shopify, sftp
from ingestion.protocols.common import JsonHTTP
from ingestion.protocols.fixtures import http_server
from ingestion.protocols.kafka_pos import consume
from pipeline.pos import pos_order_id
from pipeline.source_contracts import daily_contract


@contextmanager
def api(**kwargs):
    server = http_server(**kwargs)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.parametrize("source,module,path", [("shopify", shopify, "/admin/api/2026-10/graphql.json"), ("amazon", amazon, "")])
def test_real_http_pagination_throttle_replay_and_empty_window(tmp_path, source, module, path):
    with api(throttle_once=True) as endpoint:
        http = JsonHTTP(endpoint + path, root=tmp_path, source=source, sleep=lambda _: None)
        contract = daily_contract(source, "2026-11-12", delivery_mode="incremental")
        first = module.extract(http, contract=contract, batch_date="2026-11-12")
        assert len(list(csv.DictReader(first.open()))) == 3
        assert module.extract(http, contract=contract, batch_date="2026-11-12") == first
        assert any(item.get("http_status") == 429 for item in http.evidence)
        empty = module.extract(http, contract=daily_contract(source, "2026-11-13", delivery_mode="incremental"), batch_date="2026-11-13")
        assert list(csv.DictReader(empty.open())) == []
        assert len(list(tmp_path.rglob("COMPLETE.json"))) == 2


def test_graphql_page_limit_never_publishes_partial_delivery(tmp_path):
    with api() as endpoint:
        http = JsonHTTP(endpoint + "/admin/api/2026-10/graphql.json", root=tmp_path, source="shopify")
        with pytest.raises(ValueError, match="page limit"):
            shopify.extract(http, contract=daily_contract("shopify", "2026-11-12"), batch_date="2026-11-12", max_pages=1)
    assert list(tmp_path.rglob("objects/*"))
    assert not list(tmp_path.rglob("COMPLETE.json"))


def test_unsupported_multi_line_and_inexact_amazon_price_fail():
    from ingestion.protocols.fixtures import amazon_item, amazon_order, shopify_order
    node = shopify_order(1)
    node["lineItems"]["nodes"] *= 2
    with pytest.raises(ValueError, match="exactly one"):
        shopify.normalize(node)
    item = amazon_item(1)
    item["QuantityOrdered"] = 3
    with pytest.raises(ValueError, match="exact INR"):
        amazon.normalize(amazon_order(1), [item])


def test_sftp_completion_checksum_replay_and_incomplete_refusal(tmp_path):
    data = b"sku,location_id,snapshot_date,on_hand,unit_cost,currency\nSKU-1,WH-1,2026-11-12,5,10.50,INR\n"
    marker = json.dumps(dict(schema_version="inventory.sftp.v1", file="inventory.csv", sha256=hashlib.sha256(data).hexdigest(), bytes=len(data), rows=1)).encode()
    files = {"/incoming/inventory.csv": data, "/incoming/delivery.ready.json": marker}
    class Files:
        def stat(self, path):
            return SimpleNamespace(st_mode=stat.S_IFREG | 0o444, st_size=len(files[path]), st_mtime=1)
        def open(self, path, mode):
            return io.BytesIO(files[path])
    client = Files()
    first = sftp.download(client, "/incoming/delivery.ready.json", root=tmp_path)
    assert sftp.download(client, "/incoming/delivery.ready.json", root=tmp_path) == first
    with pytest.raises(ValueError, match="ready"):
        sftp.download(client, "/incoming/inventory.csv.part", root=tmp_path)
    files["/incoming/inventory.csv"] = data.replace(b"10.50", b"11.50")
    with pytest.raises(ValueError, match="checksum"):
        sftp.download(client, "/incoming/delivery.ready.json", root=tmp_path)
    assert first.read_bytes() == data


def pos_event():
    return dict(order_id=pos_order_id("S1", "T1", "R1"), store_id="S1", terminal_id="T1", transaction_id="R1", channel="pos",
        customer_id="", order_date="2026-11-12T05:00:00Z", sku="SKU1", product_category="Apparel", quantity="1", unit_price="10.50",
        order_total="10.50", line_count="1", tax_amount="0.00", discount_amount="0.00", status="paid", currency="INR",
        source_updated_at="2026-11-12T06:00:00Z", is_deleted="false")


class Consumer:
    def __init__(self, value):
        from kafka import TopicPartition
        self.part = TopicPartition("pos", 0)
        self.value, self.commits = value, []
    def partitions_for_topic(self, topic): return {0}
    def assign(self, parts): pass
    def beginning_offsets(self, parts): return {self.part: 0}
    def end_offsets(self, parts): return {self.part: 1}
    def committed(self, part): return None
    def seek(self, part, offset): pass
    def poll(self, **kwargs): return {self.part: [SimpleNamespace(offset=0, value=self.value)]}
    def commit(self, offsets): self.commits.append(offsets)


def test_pos_commit_only_after_durable_acceptance_and_no_commit_on_rejection(tmp_path):
    consumer = Consumer(json.dumps(pos_event()).encode())
    def accept(path):
        assert path.exists() and not consumer.commits
        return True
    consume(consumer, topic="pos", root=tmp_path, contract=daily_contract("pos", "2026-11-12"), batch_date="2026-11-12", accept=accept)
    assert consumer.commits[0][consumer.part].offset == 1
    consumer = Consumer(b"invalid json")
    with pytest.raises(ValueError):
        consume(consumer, topic="pos", root=tmp_path, contract=daily_contract("pos", "2026-11-12"), batch_date="2026-11-12", accept=accept)
    assert not consumer.commits and list((tmp_path / "pos/rejections").rglob("objects/*"))


def test_pos_accept_failure_does_not_advance_broker(tmp_path):
    consumer = Consumer(json.dumps(pos_event()).encode())
    with pytest.raises(ValueError, match="acknowledged"):
        consume(consumer, topic="pos", root=tmp_path, contract=daily_contract("pos", "2026-11-12"), batch_date="2026-11-12", accept=lambda _: False)
    assert not consumer.commits


def test_pos_refuses_broker_checkpoint_without_the_durable_source_landing(tmp_path):
    consumer = Consumer(json.dumps(pos_event()).encode())
    consumer.committed = lambda part: 1
    with pytest.raises(ValueError, match="no retained durable landing"):
        consume(consumer, topic="pos", root=tmp_path, contract=daily_contract("pos", "2026-11-12"), batch_date="2026-11-12", accept=lambda _: True)
    assert not consumer.commits


def test_real_ssh_sftp_download_and_host_key_rejection(tmp_path):
    import paramiko
    from ingestion.protocols.sftp_fixture import SFTPFixture
    folder = tmp_path / "remote"
    folder.mkdir()
    data = b"sku,location_id,snapshot_date,on_hand,unit_cost,currency\nSKU-1,WH-1,2026-11-12,5,10.50,INR\n"
    (folder / "inventory.csv").write_bytes(data)
    (folder / "delivery.ready.json").write_text(json.dumps(dict(schema_version="inventory.sftp.v1", file="inventory.csv", sha256=hashlib.sha256(data).hexdigest(), bytes=len(data), rows=1)))
    with SFTPFixture(folder) as server:
        hosts = tmp_path / "known_hosts"
        hosts.write_text(server.known_hosts())
        args = dict(host=server.host, port=server.port, username=server.username, password=server.password, known_hosts=hosts, remote_manifest="/delivery.ready.json", root=tmp_path / "accepted")
        assert sftp.extract(**args).read_bytes() == data
        hosts.write_text(f"[{server.host}]:{server.port} ssh-rsa {paramiko.RSAKey.generate(2048).get_base64()}\n")
        with pytest.raises(paramiko.BadHostKeyException):
            sftp.extract(**args)
