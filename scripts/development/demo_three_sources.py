"""Generate, ingest, replay and reconcile three sources in a NEW local directory."""

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
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
import time

import pandas as pd

sys.path.insert(0, str(_REPOSITORY_ROOT))
from ingestion import generate_synthetic_shopify_data as shopify, generate_synthetic_amazon_data as amazon, generate_synthetic_pos_data as pos
from pipeline.batch import resolve_batch_date
from pipeline.gold_release import publish_gold_release, read_gold_release
from pipeline.source_contracts import daily_contract
from pipeline.versioned_ingestion import STATE_TABLE, run_versioned_batch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path, help="Must not exist; use a short path on Windows")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--rows-per-source", type=int, help="Optional small smoke-test size; default 50000/40000/30000")
    args = parser.parse_args()
    day = resolve_batch_date(args.batch_date).isoformat()
    if args.rows_per_source is not None and args.rows_per_source < 1:
        parser.error("--rows-per-source must be positive")
    from pyiceberg.catalog.sql import SqlCatalog
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    catalog = SqlCatalog("cartco-local", uri="sqlite:///" + (root / "catalog.db").as_posix(),
                         warehouse="file://" + (root / "w").as_posix())
    started = time.perf_counter()
    try:
        receipts, expected_dates, source_counts, expected_sales = [], {}, {}, {}
        for source, module, completed in (("shopify", shopify, "delivered"), ("amazon", amazon, "shipped"), ("pos", pos, "paid")):
            if args.rows_per_source is not None:
                module.N_ROWS = args.rows_per_source
            frame = module.generate(day) if source == "pos" else module.generate(day, include_versions=True)
            source_counts[source] = len(frame)
            path, contract = root / f"{source}.csv", daily_contract(source, day)
            frame.to_csv(path, index=False)
            path.with_suffix(".csv.contract.json").write_text(json.dumps(contract, indent=2), encoding="utf-8")
            dates = pd.to_datetime(frame["order_date"], utc=True).dt.tz_convert("Asia/Kolkata").dt.date
            expected_dates.update({(source, key): value for key, value in zip(frame["order_id"], dates)})
            expected_sales[source] = sum((Decimal(str(row.unit_price)) * int(row.quantity)
                for row in frame.itertuples() if row.status == completed), Decimal("0.00"))
            kwargs = dict(source=source, batch_date=day, contract=contract, applied_at=datetime.now(timezone.utc), initialize_empty=True)
            first = run_versioned_batch(catalog, path, root / "archive", **kwargs)
            replay = run_versioned_batch(catalog, path, root / "archive", **kwargs)
            if replay["committed"] or replay["state_snapshot_id"] != first["state_snapshot_id"]:
                raise RuntimeError("Source replay created an unexpected state commit")
            receipts.append(first["receipt"])
        state = catalog.load_table(STATE_TABLE).scan().to_arrow()
        if state.num_rows != sum(source_counts.values()) or any(
            row["order_date"] != expected_dates[(row["channel"], row["order_id"])]
            for row in state.select(["channel", "order_id", "order_date"]).to_pylist()
        ):
            raise RuntimeError("Persisted business dates/counts differ from independently derived source data")
        manifest = publish_gold_release(catalog, root / "gold", receipts, batch_date=day)
        marts, verified = read_gold_release(root / "gold")
        channels = {row["channel"]: row for row in marts["channel_performance"].to_pylist()}
        if verified != manifest or set(channels) != set(source_counts) or any(
            channels[source]["total_revenue"] != amount or channels[source]["total_orders"] != source_counts[source]
            for source, amount in expected_sales.items()
        ):
            raise RuntimeError("Published source sales/counts differ from independently computed input totals")
        pointer = (root / "gold/CURRENT.json").read_bytes()
        if publish_gold_release(catalog, root / "gold", receipts, batch_date=day) != manifest or (root / "gold/CURRENT.json").read_bytes() != pointer:
            raise RuntimeError("Repeated Gold publication changed the release")
        summary = dict(source_rows=source_counts, source_completed_sales_inr={source: str(value) for source, value in expected_sales.items()},
            quality=manifest["quality"], money_types={name: str(state[name].type) for name in ("unit_price", "order_total")},
            business_timezone="Asia/Kolkata", all_business_dates_verified=True, source_replays_no_commit=True,
            repeat_publication_idempotent=True, consumer_checksums_verified=True, release_id=manifest["release_id"],
            seconds=round(time.perf_counter() - started, 2), scope="Local SQLite/Iceberg/Parquet; no Kafka/REST/MinIO/Airflow verification")
        (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary, indent=2))
    finally:
        catalog.engine.dispose()


if __name__ == "__main__":
    main()
