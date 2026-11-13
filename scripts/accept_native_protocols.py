"""Run a disposable, owned JVM Kafka broker and the four real protocol checks.

Kafka distribution/Java are explicit verified local inputs, never downloaded by
this runner. No Docker socket or existing broker resources are touched.
"""
import argparse
import base64
from pathlib import Path
import os
import socket
import subprocess
import sys
import time
from uuid import uuid4


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--java", type=Path, required=True)
    parser.add_argument("--kafka-home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=True)
    broker, controller = free_port(), free_port()
    if broker == controller:
        raise RuntimeError("Port allocation collision; rerun")
    properties = root / "broker.properties"
    properties.write_text(f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{controller}
listeners=PLAINTEXT://127.0.0.1:{broker},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://127.0.0.1:{broker}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
num.partitions=2
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
log.dirs={(root / 'broker-data').as_posix()}
""", encoding="utf-8")
    java = str(args.java.resolve())
    classpath = str(args.kafka_home.resolve() / "libs" / "*")
    cluster = base64.urlsafe_b64encode(uuid4().bytes).decode().rstrip("=")
    with (root / "broker.log").open("w", encoding="utf-8") as log:
        subprocess.run([java, "-cp", classpath, "kafka.tools.StorageTool", "format", "-t", cluster, "-c", str(properties)], stdout=log, stderr=subprocess.STDOUT, check=True)
        command = [java, "-Xms256m", "-Xmx512m", "-Dlog4j.configuration=" + (args.kafka_home.resolve() / "config/log4j.properties").as_uri(), "-cp", classpath, "kafka.Kafka", str(properties)]
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        try:
            deadline = time.monotonic() + 90
            while True:
                if process.poll() is not None:
                    raise RuntimeError("Owned fixture broker exited; inspect broker.log")
                try:
                    with socket.create_connection(("127.0.0.1", broker), timeout=1): break
                except OSError:
                    if time.monotonic() >= deadline: raise TimeoutError("Fixture broker readiness timeout")
                    time.sleep(0.5)
            subprocess.run([sys.executable, "-X", "utf8", str(Path(__file__).with_name("accept_protocols.py")), "--bootstrap", f"127.0.0.1:{broker}", "--output", str(root / "protocols")], check=True)
        finally:
            process.terminate()
            try: process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


if __name__ == "__main__":
    main()
