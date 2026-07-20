# Week 2 Status — CartCo Unified Commerce Lakehouse (B1)

## What's done

- Extended the `shopify_bronze_ingestion` Airflow DAG from 2 tasks to 4: `generate_data >> ingest_to_bronze >> run_ge_checkpoint >> transform_to_silver`. Tasks 1–2 unchanged (`BashOperator`); tasks 3–4 added as `PythonOperator`s calling directly into `transforms/` — no subprocess.
- Built the pre-Silver Great Expectations quality gate (`transforms/ge_bronze_shopify_checkpoint.py`): 8 expectations against `bronze.shopify_orders` (null checks, source-valid `status` set, numeric ranges, schema guards, row-count bound) using GX 0.18.19's Fluent API in ephemeral (in-memory, no filesystem) mode. Raises `RuntimeError` and halts the DAG on any failure.
- Built the Silver transform (`transforms/silver_shopify_orders.py`): reads Bronze, deduplicates on `order_id` (latest `order_date` wins), drops unrecoverable nulls, canonicalizes `status` → `order_status` (source vocabulary `delivered/cancelled/returned` → canonical `completed/cancelled/refunded`), derives `order_total`, writes `silver.shopify_orders` partitioned by `order_date` with an idempotent date-range overwrite. Pure PyArrow — no pandas in the transform path.
- Reconciled a real contract mismatch between the GE checkpoint and Silver: Bronze checkpoint validates the *source* status vocabulary; Silver alone owns the canonical mapping. Documented as an explicit contract boundary in both files' docstrings.
- Ran the full 4-task DAG live against the Airflow scheduler container and got a genuine, verified green run: all 4 tasks succeeded, `silver.shopify_orders` confirmed at 50,000 rows via an independent PyIceberg read-back. Evidence in `deliverables/week2/screenshots/`.
- Found and fixed two real, version-specific bugs during that verification, both root-caused by live introspection of the installed library APIs rather than assumption:
  - `great_expectations==0.18.19`: the checkpoint's original Fluent-API call chain (`context.data_sources`, `add_batch_definition_whole_dataframe`, `batch.validate(expectation_object)`) doesn't exist on this version at all — replaced with the real chain (`context.sources.add_pandas` → `get_validator` → per-expectation validator method calls).
  - `pyarrow==15.0.2`: `ChunkedArray.combine_chunks()` now returns a flat `Array` directly instead of a single-chunk `ChunkedArray`, so the Silver dedup's `.chunk(0)` unwrap broke. Fixed with a one-line change.
- Updated the root `README.md` architecture section, tech-stack table, quickstart, and data sections to reflect the real 4-task current state, and added a current-state architecture diagram (`deliverables/week2/architecture_diagram/current_state_architecture.md`).

## What's stuck

- Amazon, in-store Kafka, and SFTP distributor connectors are still unwritten stubs — Shopify is still the only working channel end to end.
- No Gold layer yet — no revenue, channel-performance, inventory-turnover, or customer-360 marts.
- Lineage (OpenLineage + Marquez) is not wired up yet.
- No automated test suite — all verification this week was live, manual DAG runs plus independent PyIceberg read-backs, not pytest.
- Loom demo for Week 2 not yet recorded.
- The webserver container needed a restart between sessions (it had exited); the scheduler alone was sufficient to trigger and verify runs via CLI, but the UI wasn't confirmed reachable during this week's verification.

## Next period's 3 goals

1. Add unit and integration tests for `transforms/silver_shopify_orders.py` and `transforms/ge_bronze_shopify_checkpoint.py` so future pyarrow/GE version bumps are caught before a live DAG run, not during one.
2. Add the Amazon Bronze connector (synthetic data + Iceberg load) following the Shopify pattern, and wire OpenLineage → Marquez for lineage on the existing Shopify path.
3. Record the Week 2 Loom demo and build the first Gold mart (Shopify revenue) on top of `silver.shopify_orders`.
