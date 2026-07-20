"""Pre-Silver Great Expectations quality gate for Bronze Shopify orders.

Runs BEFORE ``transforms/silver_shopify_orders.py``. This is a non-negotiable
architectural rule: Bronze data is validated first, and if it fails any
expectation the pipeline aborts immediately — dirty Bronze data must never
reach Silver.

Contract boundary — Bronze checks source-valid values, Silver canonicalizes
them: per ``ingestion/shopify_bronze.py``, Bronze is a raw landing layer that
performs no business logic, so this checkpoint validates ``status`` against
the *actual source vocabulary* the Shopify generator emits
(``delivered`` / ``cancelled`` / ``returned`` — see ``VALID_STATUSES``), not
the canonical Silver vocabulary. Mapping those source values to the
canonical set (``completed`` / ``cancelled`` / ``refunded``) is Silver's job
alone, done in ``transforms/silver_shopify_orders.py`` via
``STATUS_CANONICAL_MAP``. This checkpoint would reject canonical-looking
values (e.g. ``completed``) in Bronze, because seeing them there would mean
something upstream already broke the Bronze contract.

What this checks (against ``bronze.shopify_orders``, read via PyIceberg and
converted to a pandas DataFrame):
    1. ``order_id`` has no nulls
    2. ``customer_id`` has no nulls
    3. ``status`` is one of the source-valid values Bronze actually emits
       (not the canonical Silver values — see the contract boundary above)
    4. ``unit_price`` is within a sane range (0.0 - 500000.0)
    5. ``quantity`` is within a sane range (0 - 10000)
    6. ``order_id`` column exists (schema guard)
    7. ``customer_id`` column exists (schema guard)
    8. The table is non-empty and within a sane row-count bound

What happens on failure:
    ``run_checkpoint()`` raises a ``RuntimeError`` listing every failed
    expectation and its observed values. When run as an Airflow task this
    makes the task fail loudly (not a silent pass), which halts the DAG
    before ``ingest_to_bronze``'s output can be consumed by Silver.

Lineage: ``run()`` emits START/COMPLETE/FAIL OpenLineage events via
``lineage/emitter.py`` (job ``shopify_bronze_ingestion.run_ge_checkpoint``,
input dataset ``bronze.shopify_orders``) so a Marquez-side viewer can see
this quality gate sitting between the Bronze write and the Silver write.
No-op if ``OPENLINEAGE_URL`` is unset or Marquez is unreachable. See
``docs/lineage.md``.

Implementation notes:
    - Uses the Great Expectations Fluent API with an ephemeral (in-memory)
      DataContext (``gx.get_context(mode="ephemeral")``). There is no
      DataContext project, no YAML config, no checkpoint store, and no
      filesystem writes of any kind — all output goes through the Python
      logger only.
    - The exact Fluent call chain (``context.sources.add_pandas()`` ->
      ``add_dataframe_asset()`` -> ``build_batch_request(dataframe=...)`` ->
      ``context.get_validator(batch_request=...)`` -> one
      ``validator.expect_*(...)`` call per expectation) was verified live
      against the actual installed ``great_expectations==0.18.19`` API
      inside the Airflow container, not assumed from memory or from newer
      GX Core (1.0+) docs — that newer API (``context.data_sources``,
      ``add_batch_definition_whole_dataframe()``, ``batch.validate()`` taking
      a standalone expectation object) does not exist on this version and
      was the cause of a prior production bug here.
    - ``run_checkpoint()`` is pure from the Airflow task's perspective: it
      takes a DataFrame in, raises or returns None, and performs no I/O
      itself. All I/O (the Iceberg catalog read) lives in
      ``load_bronze_as_dataframe()``, so ``run_checkpoint()`` can be unit
      tested with a synthetic DataFrame with no live catalog required.

Run inside the Airflow container (deps are pre-installed there):
    docker exec cartco-airflow-scheduler \\
        python /opt/airflow/transforms/ge_bronze_shopify_checkpoint.py
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import great_expectations as gx
import pandas as pd
from pyiceberg.catalog import load_catalog

# repo root is volume-mounted at /opt/airflow but isn't guaranteed to be on
# sys.path for every process that runs this script directly (see the same
# pattern in dags/shopify_bronze_dag.py).
REPO_ROOT = "/opt/airflow"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lineage import emitter as lineage  # noqa: E402

LINEAGE_JOB_NAME = "shopify_bronze_ingestion.run_ge_checkpoint"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ge_bronze_shopify_checkpoint")

# --- Configuration (env-overridable; identical pattern to ingestion/shopify_bronze.py) ---
ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

BRONZE_TABLE_IDENTIFIER = os.environ.get("SHOPIFY_BRONZE_TABLE", "bronze.shopify_orders")

# Fluent API datasource/asset/expectation-suite names. These only live inside
# the ephemeral, in-memory DataContext for the lifetime of one process run —
# nothing is persisted, so fixed names cannot collide across runs.
DATASOURCE_NAME = "bronze_shopify_pandas"
ASSET_NAME = "bronze_shopify_orders_asset"
EXPECTATION_SUITE_NAME = "bronze_shopify_orders_suite"

# Source-valid Bronze status vocabulary — must match the actual values the
# Shopify generator emits (ingestion/generate_synthetic_shopify_data.py,
# ORDER_STATUSES). This is deliberately NOT the canonical Silver vocabulary
# (completed/cancelled/refunded); Bronze stays source-faithful and Silver
# owns the canonical mapping (transforms/silver_shopify_orders.py,
# STATUS_CANONICAL_MAP). See the module docstring's "contract boundary" note.
VALID_STATUSES = ["delivered", "cancelled", "returned"]
MIN_UNIT_PRICE = 0.0
MAX_UNIT_PRICE = 500_000.0
MIN_QUANTITY = 0
MAX_QUANTITY = 10_000
MIN_ROW_COUNT = 1
MAX_ROW_COUNT = 10_000_000


def get_catalog():
    """Load the Iceberg REST catalog with S3 (MinIO) credentials."""
    return load_catalog(
        "cartco",
        **{
            "type": "rest",
            "uri": ICEBERG_CATALOG_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
            "s3.region": AWS_REGION,
        },
    )


def load_bronze_as_dataframe() -> pd.DataFrame:
    """Read ``bronze.shopify_orders`` via PyIceberg and return it as a pandas DataFrame.

    This is the only I/O in this module (Iceberg catalog load + table scan).
    Kept separate from ``run_checkpoint()`` so the checkpoint logic itself
    can be unit tested against any DataFrame without a live catalog.
    """
    catalog = get_catalog()
    table = catalog.load_table(BRONZE_TABLE_IDENTIFIER)
    arrow_table = table.scan().to_arrow()
    return arrow_table.to_pandas()


def _build_expectations() -> list[tuple[str, dict[str, Any], str]]:
    """Build the 8 required (validator method name, kwargs, human label) triples.

    In ``great_expectations==0.18.19`` there is no standalone ``Expectation``
    object you build and hand to a ``batch.validate(...)`` call (that is a
    GX Core 1.0+ pattern and does not exist on this version). Instead you get
    a ``Validator`` bound to the batch (see ``run_checkpoint()``) and call one
    ``validator.expect_<...>(**kwargs)`` method per expectation, so each entry
    here is the method name to call plus its keyword arguments.
    """
    return [
        (
            "expect_column_values_to_not_be_null",
            {"column": "order_id"},
            "order_id has no nulls",
        ),
        (
            "expect_column_values_to_not_be_null",
            {"column": "customer_id"},
            "customer_id has no nulls",
        ),
        (
            # Source-valid check, not canonical: VALID_STATUSES is Bronze's
            # own vocabulary (delivered/cancelled/returned). Silver — not
            # this checkpoint — maps those to completed/cancelled/refunded.
            "expect_column_values_to_be_in_set",
            {"column": "status", "value_set": VALID_STATUSES},
            f"status is one of the Bronze source-valid values {VALID_STATUSES}",
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "unit_price", "min_value": MIN_UNIT_PRICE, "max_value": MAX_UNIT_PRICE},
            f"unit_price is between {MIN_UNIT_PRICE} and {MAX_UNIT_PRICE}",
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "quantity", "min_value": MIN_QUANTITY, "max_value": MAX_QUANTITY},
            f"quantity is between {MIN_QUANTITY} and {MAX_QUANTITY}",
        ),
        (
            "expect_column_to_exist",
            {"column": "order_id"},
            "order_id column exists",
        ),
        (
            "expect_column_to_exist",
            {"column": "customer_id"},
            "customer_id column exists",
        ),
        (
            "expect_table_row_count_to_be_between",
            {"min_value": MIN_ROW_COUNT, "max_value": MAX_ROW_COUNT},
            f"table row count is between {MIN_ROW_COUNT} and {MAX_ROW_COUNT:,}",
        ),
    ]


def _describe_result_detail(result: Any, total_rows: int) -> str:
    """Build a human-readable row-count / observed-value string for one result.

    Always returns a concrete detail (not just a boolean) so callers can log
    the exact number of failing rows, or the observed value for table- and
    schema-level checks that have no per-row count.
    """
    detail = result.result or {}
    if "unexpected_count" in detail:
        element_count = detail.get("element_count", total_rows)
        return f"{detail.get('unexpected_count', 0)} of {element_count} rows failed"
    if "observed_value" in detail:
        return f"observed_value={detail.get('observed_value')}"
    return "schema-level check (no row count)"


def run_checkpoint(bronze_df: pd.DataFrame) -> None:
    """Validate a Bronze Shopify orders DataFrame against the 8 pre-Silver expectations.

    Pure from the caller's perspective: takes a DataFrame, performs no I/O of
    its own (validation runs entirely in-memory via an ephemeral Great
    Expectations context), and either returns ``None`` on success or raises
    ``RuntimeError`` on failure. This makes it independently unit-testable
    with a synthetic DataFrame, with no live Iceberg catalog required.

    Raises:
        RuntimeError: if any of the 8 expectations fail. The message lists
            every failed expectation together with its observed values.
    """
    context = gx.get_context(mode="ephemeral")
    data_source = context.sources.add_pandas(DATASOURCE_NAME)
    data_asset = data_source.add_dataframe_asset(name=ASSET_NAME)
    batch_request = data_asset.build_batch_request(dataframe=bronze_df)
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name=EXPECTATION_SUITE_NAME,
    )

    expectations = _build_expectations()
    total_rows = len(bronze_df)
    failures: list[str] = []

    for method_name, kwargs, description in expectations:
        expectation_method = getattr(validator, method_name)
        result = expectation_method(**kwargs)
        detail = _describe_result_detail(result, total_rows)
        if result.success:
            logger.info("PASS  %s (%s)", description, detail)
        else:
            failures.append(f"{description} -- {detail}")
            logger.error("FAIL  %s (%s)", description, detail)

    if failures:
        raise RuntimeError(
            "Bronze quality gate FAILED "
            f"({len(failures)}/{len(expectations)} expectations failed); "
            "aborting before Silver write:\n"
            + "\n".join(f"  - {failure}" for failure in failures)
        )

    logger.info(
        "Bronze quality gate PASSED: all %d expectations succeeded (row count: %d)",
        len(expectations),
        total_rows,
    )


def run() -> None:
    """Load Bronze data and run the pre-Silver quality gate; logs start/end and duration."""
    start_time = datetime.now(timezone.utc)
    logger.info("=== Shopify Bronze quality gate START at %s ===", start_time.isoformat())
    logger.info(
        "Bronze table: %s | Iceberg REST catalog: %s | S3 endpoint: %s",
        BRONZE_TABLE_IDENTIFIER,
        ICEBERG_CATALOG_URI,
        S3_ENDPOINT,
    )

    bronze_dataset = lineage.table_dataset(BRONZE_TABLE_IDENTIFIER)
    run_id = lineage.new_run_id()
    lineage.emit_start(LINEAGE_JOB_NAME, run_id, inputs=[bronze_dataset])

    bronze_df = load_bronze_as_dataframe()
    logger.info("Rows loaded from Bronze for validation: %d", len(bronze_df))

    try:
        run_checkpoint(bronze_df)
    except Exception:
        lineage.emit_fail(LINEAGE_JOB_NAME, run_id, inputs=[bronze_dataset])
        raise

    lineage.emit_complete(LINEAGE_JOB_NAME, run_id, inputs=[bronze_dataset])

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info(
        "=== Shopify Bronze quality gate END at %s (%.1fs) ===", end_time.isoformat(), duration
    )


if __name__ == "__main__":
    run()
