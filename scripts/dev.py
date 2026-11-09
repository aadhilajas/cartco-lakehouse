"""Portable development commands; invoke with Python 3.11 from any directory."""
from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def run(*command: str) -> None:
    subprocess.run(command, cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["bootstrap", "test", "collect", "build", "up", "down", "check-dags", "generate", "run-local"])
    args = parser.parse_args()
    if sys.version_info[:2] != (3, 11):
        parser.error("Use Python 3.11 (the same minor version as CI and Airflow).")
    if args.command == "bootstrap":
        if sys.prefix == sys.base_prefix:
            parser.error("Create and activate a virtual environment before bootstrap.")
        run(sys.executable, "-m", "pip", "install", "--require-hashes", "-r", "requirements/test.txt")
        run(sys.executable, "-m", "pip", "check")
    elif args.command in {"test", "collect"}:
        run(sys.executable, "-m", "pytest", "tests/unit", "-v", *(["--collect-only"] if args.command == "collect" else []))
    elif args.command in {"build", "up", "down"}:
        commands = {"build": ["build"], "up": ["up", "-d", "--build"], "down": ["down"]}
        run("docker", "compose", *commands[args.command])
    elif args.command == "check-dags":
        run("docker", "compose", "run", "--rm", "--no-deps", "-e", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/cartco-dag-check.db", "--entrypoint", "python", "airflow-scheduler", "/opt/airflow/scripts/check_dags.py")
    elif args.command == "generate":
        for source in ("shopify", "amazon"):
            run(sys.executable, f"ingestion/generate_synthetic_{source}_data.py")
    else:
        # Baseline demonstration only: current scripts use today's UTC date.
        # Execute sequentially because both sources write silver.orders.
        scripts = []
        for source, silver in (("shopify", "silver_shopify_orders"), ("amazon", "amazon_silver")):
            scripts.extend([f"ingestion/generate_synthetic_{source}_data.py", f"ingestion/{source}_bronze.py", f"transforms/ge_bronze_{source}_checkpoint.py", f"transforms/{silver}.py"])
        scripts.extend(f"transforms/gold/{mart}.py" for mart in ("daily_revenue", "channel_performance", "inventory_turnover", "customer_360"))
        for script in scripts:
            run("docker", "compose", "exec", "-T", "airflow-scheduler", "python", f"/opt/airflow/{script}")


if __name__ == "__main__":
    main()

