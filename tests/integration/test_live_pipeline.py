"""Integration tests against the real local stack (MinIO, Iceberg REST, Postgres).

Opt-in only: skipped unless RUN_INTEGRATION_TESTS=1 is set (see
tests/conftest.py's require_live_stack fixture), and further skipped if the
Iceberg REST catalog isn't actually reachable even when opted in -- so a
developer running `pytest` without the stack up gets a clear skip reason
instead of a confusing connection-error failure.

Run locally (stack already up via `docker compose up -d`, with at least one
successful shopify_bronze_ingestion DAG run so bronze.shopify_orders has
data):
    docker exec cartco-airflow-scheduler bash -c \
        "RUN_INTEGRATION_TESTS=1 pytest -m integration tests/integration -v"
"""

from __future__ import annotations

import pytest
from pyiceberg.exceptions import NoSuchTableError

from transforms import ge_bronze_shopify_checkpoint, silver_shopify_orders


def _bronze_table_or_skip():
    catalog = silver_shopify_orders.get_catalog()
    try:
        return catalog.load_table(silver_shopify_orders.BRONZE_TABLE_IDENTIFIER)
    except NoSuchTableError:
        pytest.skip(
            f"{silver_shopify_orders.BRONZE_TABLE_IDENTIFIER} does not exist yet -- "
            "run the shopify_bronze_ingestion DAG (or ingestion/shopify_bronze.py) first."
        )
    except Exception as exc:  # connection refused, DNS failure, etc.
        pytest.skip(f"Live Iceberg REST catalog not reachable: {exc}")


@pytest.mark.integration
def test_ge_checkpoint_passes_against_live_bronze_data(require_live_stack):
    """The GE gate should pass against whatever is currently in bronze.shopify_orders.

    This is the same load_bronze_as_dataframe() -> run_checkpoint() path the
    run_ge_checkpoint Airflow task calls, exercised against the real table.
    """
    bronze_table = _bronze_table_or_skip()
    bronze_df = bronze_table.scan().to_arrow().to_pandas()
    assert len(bronze_df) > 0, "bronze.shopify_orders exists but has 0 rows"

    # Should not raise -- proves the live Bronze data still satisfies the
    # 8 pre-Silver expectations end to end, via the real Fluent API chain.
    ge_bronze_shopify_checkpoint.run_checkpoint(bronze_df)


@pytest.mark.integration
def test_silver_transform_run_writes_to_live_silver_table(require_live_stack):
    """Running the real Silver transform end to end should populate silver.shopify_orders.

    Exercises: reading Bronze via PyIceberg, the full transform() pipeline,
    ensure_table(), and the idempotent partition-overwrite write -- the same
    path the transform_to_silver Airflow task runs.
    """
    _bronze_table_or_skip()

    silver_shopify_orders.run()

    catalog = silver_shopify_orders.get_catalog()
    silver_table = catalog.load_table(silver_shopify_orders.TABLE_IDENTIFIER)
    silver_df = silver_table.scan().to_arrow().to_pandas()

    assert len(silver_df) > 0
    assert set(silver_df["order_status"].unique()) <= {"completed", "cancelled", "refunded", "unknown"}
