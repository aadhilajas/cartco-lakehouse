# Test Report — CartCo Unified Commerce Lakehouse

Status: Week 3. Covers the only pipeline that exists end to end — Shopify
Bronze → GE checkpoint → Silver. Nothing here claims coverage of Amazon,
in-store, distributor, or Gold, because none of those exist yet.

## Purpose and scope

This report documents what is actually tested in this repo, how, and — just
as importantly — what isn't. It's written to be read before trusting a merge
or a demo, not as a checklist to pad out for a submission.

Scope: `ingestion/shopify_bronze.py`, `transforms/ge_bronze_shopify_checkpoint.py`,
`transforms/silver_shopify_orders.py`, and the `shopify_bronze_ingestion` DAG
that wires them together. `lineage/emitter.py` is exercised indirectly
(its `run()` calls are covered by the same tests) but has no dedicated test
of its own — see Gaps below.

## Unit tests (`tests/unit/`, 14 tests, no Docker required)

Run against pure PyArrow/pandas logic with no live catalog, no GE checkpoint
store, and no network calls.

**`test_silver_status_canonicalization.py`** — `_canonicalize_status_to_order_status()`
- known Bronze statuses map to the canonical Silver vocabulary
- an out-of-vocabulary status maps to `"unknown"` instead of raising
- `rows_unknown_status` is tracked correctly in the stats dict
- an all-known-statuses batch reports zero unknowns

**`test_silver_transform.py`** — `transform()`
- dedup keeps the row with the latest `order_date` per `order_id`
- rows with a null `order_id` or `customer_id` are dropped
- `order_total = quantity * unit_price` is computed correctly
- output schema and column order match `TABLE_SCHEMA`/`COLUMN_ORDER`
- `quantity`/`unit_price`/`product_category` nulls get their documented defaults

**`test_ge_checkpoint.py`** — `run_checkpoint()`
- a valid DataFrame passes all 8 expectations
- nulls in `order_id` raise `RuntimeError`
- a status outside the Bronze source vocabulary raises (this is the exact
  case the Bronze/Silver contract boundary depends on — see
  `transforms/ge_bronze_shopify_checkpoint.py`'s docstring)
- an out-of-range `unit_price` raises
- the error message lists every failed expectation, not just the first one

## Integration tests (`tests/integration/`, 2 tests, opt-in)

Marked `@pytest.mark.integration`, gated behind `RUN_INTEGRATION_TESTS=1`
(`tests/conftest.py`'s `require_live_stack` fixture). Require the Docker
stack up and at least one prior Bronze load, and are skipped — not
failed — with a specific reason if either precondition isn't met.

- `test_ge_checkpoint_passes_against_live_bronze_data` — runs
  `load_bronze_as_dataframe() → run_checkpoint()` against whatever is
  actually in `bronze.shopify_orders` right now.
- `test_silver_transform_run_writes_to_live_silver_table` — runs
  `silver_shopify_orders.run()` for real (the same call the
  `transform_to_silver` task makes) and asserts `silver.shopify_orders`
  ends up non-empty with only canonical status values.

Run inside the container:
```bash
docker exec -e RUN_INTEGRATION_TESTS=1 cartco-airflow-scheduler \
    pytest -m integration tests/integration -v
```

## What CI runs automatically

`.github/workflows/ci.yml`, `unit-tests` job: on every push and PR, sets up
Python 3.11, installs `tests/requirements-test.txt`, runs
`pytest -m "not integration" tests/unit -v`. This is the only thing that
runs without a human deciding to run it.

## What CI does not run automatically

The `integration-tests` job exists in the same workflow but only runs on a
manual `workflow_dispatch` trigger with `run_integration` checked — it
builds the Airflow image, brings up the full Compose stack (MinIO, Iceberg
REST, Postgres, Airflow, Marquez), seeds Bronze data, runs the two
integration tests, then tears the stack down. It is not on push/PR because
a full multi-container stack on every commit is slow and would make CI the
bottleneck for a repo at this stage. Nobody has run this job from the
Actions tab yet in this project — it has only been exercised locally.

## Running tests locally

```bash
# Unit — no Docker needed
pip install -r tests/requirements-test.txt
pytest -m "not integration" tests/unit -v

# Integration — stack must already be up with Bronze data loaded
docker compose up -d
docker exec cartco-airflow-scheduler python /opt/airflow/ingestion/generate_synthetic_shopify_data.py
docker exec cartco-airflow-scheduler python /opt/airflow/ingestion/shopify_bronze.py
docker exec -e RUN_INTEGRATION_TESTS=1 cartco-airflow-scheduler \
    pytest -m integration tests/integration -v
```

## Major gaps

- **`ingestion/shopify_bronze.py` has no dedicated test.** It's exercised
  indirectly by the integration tests (they depend on Bronze data existing)
  but there's no test that calls `read_source_csv()` or `run()` directly and
  asserts on its output. This is the same script every downstream test
  quietly depends on, and it's currently the least-tested part of the
  working path.
- **The DAG itself (`dags/shopify_bronze_dag.py`) is untested.** Task
  wiring, `sys.path` setup, and the `generate_data >> ingest_to_bronze >>
  run_ge_checkpoint >> transform_to_silver` dependency chain are only ever
  verified by actually triggering the DAG and reading task states — there's
  no `dagbag` import test or task-dependency assertion.
- **`lineage/emitter.py` has no unit tests.** Its fail-safe behavior (never
  raising when Marquez is down or `OPENLINEAGE_URL` is unset) has only been
  verified by manual runs, not by a test that mocks a broken client and
  asserts the pipeline still completes.
- **No test asserts on the row-count mismatch warnings** (`rows_written !=
  rows_read` in Bronze, `rows_confirmed != rows_written` in Silver). These
  are logged, not raised, and nothing currently checks that they fire when
  they should.
- **Idempotency (re-running the same day doesn't duplicate rows) is not
  covered by an automated test** — it was verified manually during Week 1/2
  development and hasn't regressed since, but there's no test that runs
  Bronze or Silver twice and asserts row counts are stable.

## Why this strategy fits the current stage

The test suite is deliberately weighted toward the transform logic that's
easiest to get subtly wrong and hardest to catch by eyeballing a DAG run —
status canonicalization, dedup, null handling, and the GE expectation
contract — because those are pure functions with no I/O, cheap to test
exhaustively, and every one of them has already had a real version-specific
bug caught the hard way (see `README.md`'s Known Limitations). Wiring,
orchestration, and the Bronze script are still mostly covered by live,
manual verification because at four tasks and one source channel, a full
DAG trigger takes under a minute and is still cheaper than building
Airflow-level test infrastructure. That trade-off will stop making sense
once a second channel or a Gold layer is added — at that point the gaps
listed above are the ones to close first.

---

## Logging & error-handling audit

A pass over every `try`/`except` in the pipeline code, done for this report
rather than assumed.

**What was checked:** `ingestion/shopify_bronze.py`,
`transforms/silver_shopify_orders.py`, `transforms/ge_bronze_shopify_checkpoint.py`,
`lineage/emitter.py`, `dags/shopify_bronze_dag.py`, `tests/integration/test_live_pipeline.py`.

**Result: no bare `except: pass` anywhere in the codebase.** Every `except`
block either does something specific or is a deliberate, documented
swallow:

| File | `except` | Behavior |
|---|---|---|
| `ingestion/shopify_bronze.py` | `NamespaceAlreadyExistsError`, `NoSuchTableError` (in `ensure_table`) | Expected first-run conditions; logged at INFO, not an error |
| `ingestion/shopify_bronze.py` | `Exception` (wrapping the write in `run()`) | Emits an OpenLineage `FAIL` event, then re-raises — the task still fails |
| `transforms/silver_shopify_orders.py` | `NamespaceAlreadyExistsError`, `NoSuchTableError` (in `ensure_table`) | Same pattern as Bronze |
| `transforms/silver_shopify_orders.py` | `NoSuchTableError` (in `run()`, Bronze missing) | Converted to a `RuntimeError` with an actionable message ("run ingestion/shopify_bronze.py first") |
| `transforms/silver_shopify_orders.py` | `Exception` (wrapping the transform+write in `run()`) | Emits `FAIL`, then re-raises |
| `transforms/ge_bronze_shopify_checkpoint.py` | `Exception` (wrapping `run_checkpoint()` in `run()`) | Emits `FAIL`, then re-raises — a quality-gate failure still halts the DAG |
| `lineage/emitter.py` | `Exception` (client construction, event emission) | **Deliberately swallowed**, logged as a warning with `exc_info=True`. This is the one place a broad except is intentionally the end of the line — a Marquez outage must not take down the pipeline it's observing. See the module docstring. |
| `tests/integration/test_live_pipeline.py` | `NoSuchTableError`, `Exception` | Test-infra only: converts "no Bronze data" / "catalog unreachable" into a `pytest.skip` with a specific reason, not a swallowed failure |

**Logging approach:** every pipeline module calls `logging.basicConfig()`
with the same format (`%(asctime)s [%(levelname)s] %(name)s: %(message)s`)
and gets its own named logger (`shopify_bronze`, `shopify_silver`,
`ge_bronze_shopify_checkpoint`, `lineage`). Every `run()` logs a start
timestamp, row counts at each intermediate stage (read → dedup → null-drop →
written, or per-expectation pass/fail with observed values for the GE gate),
and an end timestamp with duration — there is no silent completion anywhere
in the pipeline.

**Where observability exists today:**
- Airflow task logs (visible in the UI and via `airflow tasks logs` /
  `docker exec ... cat` on the log files) — the primary source of truth for
  what happened in a given run.
- Row-count mismatch checks in Bronze and Silver (`rows_written != rows_read`,
  `rows_confirmed != rows_written`) log a `WARNING` if a write doesn't
  read back as expected.
- The Marquez lineage graph (`docs/lineage.md`) — `START`/`COMPLETE`/`FAIL`
  events per job, so a failed run is visible as a `FAIL` state in Marquez as
  well as a red task in Airflow.

**What's still missing:**
- No alerting. A task failure or a GE gate failure shows up red in Airflow
  and as a `FAIL` run in Marquez, but nothing pages or emails anyone —
  there's no `on_failure_callback`, no Slack/email integration.
- No structured logging. Everything is plain-text `logging` output; there's
  no JSON log format or log aggregation, so grepping task logs is currently
  the only way to debug a run after the fact.
- No log rotation/retention policy for `airflow/logs/` — it's git-ignored
  and grows unbounded locally; nothing prunes it.
- The row-count mismatch warnings described above are logged but not
  tested (see Gaps) and don't fail the task — a partial write currently
  degrades silently from the DAG's perspective, only visible if someone
  reads the log.
- `lineage/emitter.py`'s fail-safe behavior (pipeline keeps running if
  Marquez is unreachable) is based on a `try`/`except Exception` around the
  client construction and every emit call, reasoned through by reading the
  code — it has not been verified against an actual Marquez outage in this
  session, and there's no automated test for it either. That's a real gap,
  not just a theoretical one, and it's listed above.
