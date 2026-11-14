"""Finite Kafka microbatch with durable landing before explicit offset commit."""
import json
import hashlib
from pathlib import Path
import time

from ingestion.protocols.common import encoded, publish_orders, retain
from pipeline.raw_batches import _write_once


def consume(consumer, *, topic, root, contract, batch_date, accept, max_records=10000, deadline_seconds=60):
    """The caller owns one consumer per group/topic (manual partition assignment).

    Snapshot high-water offsets make the run finite. `accept` must durably land
    and validate the complete delivery. Failed acceptance leaves offsets intact;
    a crash between landing and commit replays immutable source identities.
    """
    from kafka import TopicPartition
    from kafka.structs import OffsetAndMetadata
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise ValueError("POS topic has no partitions")
    assigned = [TopicPartition(topic, number) for number in sorted(partitions)]
    consumer.assign(assigned)
    beginning = consumer.beginning_offsets(assigned)
    ends = consumer.end_offsets(assigned)
    next_offsets, starts, previous = {}, {}, []
    group = getattr(consumer, "config", {}).get("group_id", "provided-consumer")
    ledger = Path(root) / "pos/committed-landings" / hashlib.sha256(encoded({"topic": topic, "group": group})).hexdigest()
    for part in assigned:
        committed = consumer.committed(part)
        start = beginning[part] if committed is None else committed
        if start < beginning[part] or start > ends[part]:
            raise ValueError("Committed Kafka offset is outside retained range; no silent reset")
        consumer.seek(part, start)
        next_offsets[part] = start
        starts[part] = start
        if committed is not None and committed > beginning[part]:
            receipt = ledger / f"{part.partition}-{committed}.json"
            if not receipt.is_file():
                raise ValueError("Broker offset has no retained durable landing receipt; restore before consuming")
            record = json.loads(receipt.read_bytes())
            delivery = Path(root) / "pos/deliveries" / record["delivery_id"] / "orders.csv"
            if not delivery.is_file() or hashlib.sha256(delivery.read_bytes()).hexdigest() != record["csv_sha256"]:
                raise ValueError("Previously committed POS landing is missing/corrupt")
            previous.append(delivery)
    if sum(ends[p] - next_offsets[p] for p in assigned) > max_records:
        raise ValueError("Finite POS backlog exceeds batch limit")
    rows, evidence = [], []
    deadline = time.monotonic() + deadline_seconds
    try:
        while any(next_offsets[p] < ends[p] for p in assigned):
            if time.monotonic() >= deadline:
                raise TimeoutError("POS microbatch deadline reached")
            for part, messages in consumer.poll(timeout_ms=500, max_records=max_records).items():
                for message in messages:
                    if message.offset >= ends[part]:
                        continue
                    if message.offset != next_offsets[part]:
                        raise ValueError("Kafka offset gap inside bounded noncompacted POS topic")
                    envelope = {"topic": part.topic, "partition": part.partition, "offset": message.offset,
                                "value_sha256": retain(root, "pos", message.value)["sha256"]}
                    evidence.append(retain(root, "pos/offsets", encoded(envelope)))
                    row = json.loads(message.value)
                    if not isinstance(row, dict):
                        raise ValueError("POS event must be a JSON object")
                    rows.append(row)
                    next_offsets[part] = message.offset + 1
        if not rows and previous and len(set(previous)) == 1:
            return previous[0]  # No new offsets; expose the retained immutable delivery.
        path = publish_orders(rows, root=root, source="pos", contract=contract, batch_date=batch_date, evidence=evidence)
        accepted = accept(path)
        if accepted is not True:
            raise ValueError("Durable POS landing was not acknowledged")
        complete = json.loads(path.with_name("COMPLETE.json").read_bytes())
        for part in assigned:
            if next_offsets[part] > starts[part]:
                _write_once(ledger / f"{part.partition}-{next_offsets[part]}.json", encoded({"delivery_id": complete["delivery_id"], "csv_sha256": complete["csv_sha256"]}))
        consumer.commit({part: OffsetAndMetadata(next_offsets[part], "cartco-durable-v1", -1) for part in assigned})
        return path
    except Exception as exc:
        retain(root, "pos/rejections", encoded({"error_type": type(exc).__name__, "error": str(exc), "records_read": len(rows), "offsets_committed": False}))
        raise


def open_consumer(bootstrap_servers, group_id):
    from kafka import KafkaConsumer
    return KafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=group_id,
        enable_auto_commit=False, auto_offset_reset="none", request_timeout_ms=15000,
        api_version_auto_timeout_ms=10000, max_poll_records=10000)
