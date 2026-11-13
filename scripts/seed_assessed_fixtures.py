"""Seed one immutable synthetic POS topic and completed inventory fixture."""
import os
from pathlib import Path
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.accept_protocols import pos_rows, write_inventory
from ingestion.protocols.common import encoded


def main():
    from kafka import KafkaProducer, TopicPartition, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    broker = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.environ.get("CARTCO_POS_TOPIC", "cartco.synthetic.pos.20261112.v1")
    root = Path(os.environ.get("CARTCO_ASSESSED_ROOT", "/opt/airflow/data/assessed"))
    for attempt in range(30):
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker, request_timeout_ms=10000)
            break
        except Exception:
            if attempt == 29: raise
            time.sleep(2)
    created = False
    try:
        try:
            admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
            created = True
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()
    rows = pos_rows("2026-11-12")
    if created:
        producer = KafkaProducer(bootstrap_servers=broker, acks="all", retries=5)
        try:
            for index, row in enumerate(rows): producer.send(topic, encoded(row), partition=index % 2).get(timeout=20)
            producer.flush(timeout=20)
        finally:
            producer.close()
    consumer = KafkaConsumer(bootstrap_servers=broker, enable_auto_commit=False, group_id=None)
    try:
        parts = [TopicPartition(topic, i) for i in range(2)]
        consumer.assign(parts)
        consumer.seek_to_beginning(*parts)
        ends = consumer.end_offsets(parts)
        if [ends[p] for p in parts] != [3, 3]:
            raise ValueError("Existing synthetic POS topic differs; refusing overwrite/append")
        actual = {p: [] for p in parts}
        deadline = time.monotonic() + 30
        while sum(map(len, actual.values())) < 6:
            if time.monotonic() >= deadline: raise TimeoutError("Seed verification timed out")
            for part, messages in consumer.poll(timeout_ms=500).items():
                actual[part].extend(message.value for message in messages)
        for part in parts:
            if actual[part] != [encoded(row) for index, row in enumerate(rows) if index % 2 == part.partition]:
                raise ValueError("Existing synthetic POS payload differs")
    finally:
        consumer.close()
    write_inventory(root / "fixtures/inventory", "2026-11-12")
    print("Verified immutable six-event POS fixture and completed inventory delivery")


if __name__ == "__main__": main()
