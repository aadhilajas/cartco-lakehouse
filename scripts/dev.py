"""Portable development commands; invoke with Python 3.11 from any directory."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.batch import resolve_batch_date


def run(*command: str, extra_env: dict[str, str] | None = None) -> None:
    subprocess.run(command, cwd=ROOT, check=True, env={**os.environ, **(extra_env or {})})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["bootstrap", "test", "collect", "build", "up", "down", "check-dags", "generate", "run-local"])
    parser.add_argument("--batch-date", help="YYYY-MM-DD for generate/run-local; defaults to CARTCO_BATCH_DATE or UTC today")
    parser.add_argument("--source-versions", action="store_true", help="Include explicit source version/delete fields when generating fixtures")
    args = parser.parse_args()
    if args.source_versions and args.command != "generate":
        parser.error("--source-versions applies to generate")
    if args.batch_date is not None and args.command not in {"generate", "run-local"}:
        parser.error("--batch-date applies to generate and run-local")
    if args.command in {"generate", "run-local"}:
        try:
            batch_date = resolve_batch_date(args.batch_date).isoformat()
        except ValueError as exc:
            parser.error(str(exc))
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
            run(sys.executable, f"ingestion/generate_synthetic_{source}_data.py", extra_env={"CARTCO_BATCH_DATE": batch_date, "CARTCO_SOURCE_VERSIONS": "1" if args.source_versions else "0"})
    else:
        # Resolve the date once so every stage keeps the same partition.
        # Execute sequentially because both sources write silver.orders.
        scripts = []
        for source, silver in (("shopify", "silver_shopify_orders"), ("amazon", "amazon_silver")):
            scripts.extend([f"ingestion/generate_synthetic_{source}_data.py", f"ingestion/{source}_bronze.py", f"transforms/ge_bronze_{source}_checkpoint.py", f"transforms/{silver}.py"])
        scripts.extend(f"transforms/gold/{mart}.py" for mart in ("daily_revenue", "channel_performance", "inventory_turnover", "customer_360"))
        for script in scripts:
            run("docker", "compose", "exec", "-T", "-e", f"CARTCO_BATCH_DATE={batch_date}", "airflow-scheduler", "python", f"/opt/airflow/{script}")


if __name__ == "__main__":
    main()
