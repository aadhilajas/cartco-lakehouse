## What's done

- Added a unit + integration test suite (`tests/`) for the Shopify pipeline: 14 unit tests covering Silver status canonicalization, the full `transform()` pipeline (dedup, null handling, `order_total`, output schema), and the Great Expectations checkpoint's pass/fail behavior; 2 integration tests exercising the GE checkpoint and Silver transform against the live Docker stack, gated behind `RUN_INTEGRATION_TESTS=1` so they're skipped by default.
- Added `pytest.ini`, a root `conftest.py`, and `tests/conftest.py` (with a `require_live_stack` fixture) to make test discovery, markers, and skip behavior explicit and reproducible; resolved a real module-name collision between this repo's `tests/` package and one pulled in transitively by an Airflow provider dependency.
- Added `.github/workflows/ci.yml`: unit tests run automatically on every push/PR; integration tests are manual-trigger-only (`workflow_dispatch`) since they need a full Docker Compose stack CI doesn't provision by default.
- Added minimal, version-aware OpenLineage lineage instrumentation for the working Shopify path:
  - Added `marquez-db` / `marquez-api` / `marquez-web` to `docker-compose.yml` (local-only Marquez backend + UI at `localhost:3000`).
  - Added `lineage/emitter.py`: a small, fail-safe wrapper around the OpenLineage Python client (manual `START`/`COMPLETE`/`FAIL` events, never raises on a Marquez outage) called from `ingestion/shopify_bronze.py`, `transforms/ge_bronze_shopify_checkpoint.py`, and `transforms/silver_shopify_orders.py`.
  - Chose manual client emission over `apache-airflow-providers-openlineage`'s automatic extraction after verifying live that the automatic listener produces empty `inputDatasetVersions`/`outputDatasetVersions` for our `BashOperator`/`PythonOperator` tasks (it has no generic extractor for arbitrary scripts) — confirmed via the Marquez API, then explicitly disabled that listener (`AIRFLOW__OPENLINEAGE__DISABLED=true`) so there's one lineage source of truth, not two overlapping ones.
  - Verified end-to-end against a live, fully green 4-task DAG run: all three instrumented jobs (`ingest_to_bronze`, `run_ge_checkpoint`, `transform_to_silver`) show up in Marquez with correct dataset-level lineage (`bronze.shopify_orders` → `silver.shopify_orders`), independently confirmed via the Marquez HTTP API, not just our own logs. Full transcript in `deliverables/week3/lineage_proof.txt`.
- Found and fixed a real bug during that verification: the pinned `openlineage-python==1.50.0` conflicted with `apache-airflow-providers-openlineage`'s own transitive pin (`1.11.3`), and its newer `event_v2` client module doesn't exist on the older version already shipped in `apache/airflow:2.9.0`. Re-pinned to `openlineage-python==1.11.3` (the version actually resolved in the image) and switched `lineage/emitter.py` to the `openlineage.client.run` module after confirming its API live inside the container.
- Added `docs/lineage.md` documenting how to start the lineage stack, run the DAG, inspect lineage in Marquez, and — explicitly — what is and isn't covered yet (no column-level lineage, no other channels, no Gold, Airflow's own automatic listener intentionally disabled).
- Documentation hardening pass: added `docs/test_report.md` (test scope, what CI does/doesn't run, gaps, plus a full try/except and logging audit of every pipeline module), polished `README.md`'s navigation to the proof artifacts and ADRs, and replaced the placeholder `blog_draft.md` with a real outline built from this project's actual bugs.

## What's stuck

- No Loom recording exists yet for Week 3 — `deliverables/week3/demo_link.md` is still `[LOOM_LINK_PENDING]`.
- Lineage coverage is Shopify-only and table-level only (no schema/row-count facets attached to the OpenLineage events themselves, even though row counts are still logged separately by each script).
- The Marquez UI itself was not screenshotted in this session — `lineage_proof.txt` is a command/API transcript, not a UI capture.

## Next period's 3 goals

1. Record the Week 3 Loom demo: DAG run → Marquez UI showing the 3-job lineage graph → test suite passing locally and in CI.
2. Add the Amazon Bronze connector (synthetic data + Iceberg load), following the same Bronze contract, GE gate, and lineage-emission pattern already established for Shopify.
3. Start on the first Gold mart (revenue) once a second Silver source exists, so Gold isn't built against a single-channel Silver layer.
