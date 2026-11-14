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

import os

import pytest
from pyiceberg.exceptions import NoSuchTableError

from transforms import ge_bronze_shopify_checkpoint, silver_shopify_orders


def _unavailable(reason):
    if os.environ.get("CARTCO_STRICT_INTEGRATION") == "1":
        pytest.fail(reason)
    pytest.skip(reason)


def _bronze_table_or_skip():
    catalog = silver_shopify_orders.get_catalog()
    try:
        return catalog.load_table(silver_shopify_orders.BRONZE_TABLE_IDENTIFIER)
    except NoSuchTableError:
        _unavailable(
            f"{silver_shopify_orders.BRONZE_TABLE_IDENTIFIER} does not exist yet -- "
            "run the shopify_bronze_ingestion DAG (or ingestion/shopify_bronze.py) first."
        )
    except Exception as exc:  # connection refused, DNS failure, etc.
        _unavailable(f"Live Iceberg REST catalog not reachable: {exc}")


@pytest.mark.integration
def test_ge_checkpoint_passes_against_live_bronze_data(require_live_stack):
    """The GE gate should pass against whatever is currently in bronze.shopify_orders.

    This is the same load_bronze_as_dataframe() -> run_checkpoint() path the
    run_ge_checkpoint Airflow task calls, exercised against the real table.
    """
    bronze_table = _bronze_table_or_skip()
    bronze_df = ge_bronze_shopify_checkpoint.load_bronze_as_dataframe()
    assert len(bronze_df) > 0, "bronze.shopify_orders exists but has 0 rows"

    # Should not raise -- proves the live Bronze data still satisfies the
    # 8 pre-Silver expectations end to end, via the real Fluent API chain.
    ge_bronze_shopify_checkpoint.run_checkpoint(bronze_df)


@pytest.mark.integration
def test_silver_transform_run_writes_to_live_silver_table(require_live_stack):
    """Running the real Silver transform end to end should populate silver.orders.

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
    shopify_df = silver_df[silver_df["channel"] == "shopify"]
    assert len(shopify_df) > 0
    assert set(shopify_df["order_status"].unique()) <= {"completed", "cancelled", "refunded", "unknown"}


@pytest.mark.integration
def test_silver_orders_contains_both_channels_after_both_dags_run(require_live_stack):
    """silver.orders must hold rows from both channel='shopify' and channel='amazon'.

    This verifies the cross-channel UNION contract: both the Shopify and Amazon
    Silver transforms write to the same ``silver.orders`` Iceberg table, each
    scoped to its own channel so they never overwrite each other.

    Pre-condition: both amazon_bronze_ingestion and shopify_bronze_ingestion
    DAGs must have completed at least one successful run before this test is
    invoked (i.e. both bronze tables must exist and silver.orders must have
    been populated for both channels).
    """
    from transforms import amazon_silver

    catalog = amazon_silver.get_catalog()

    # Skip gracefully if silver.orders doesn't exist yet.
    try:
        silver_table = catalog.load_table(amazon_silver.TABLE_IDENTIFIER)
    except NoSuchTableError:
        _unavailable(
            f"{amazon_silver.TABLE_IDENTIFIER} does not exist yet -- "
            "run both shopify_bronze_ingestion and amazon_bronze_ingestion DAGs first."
        )
    except Exception as exc:
        _unavailable(f"Live Iceberg REST catalog not reachable: {exc}")

    silver_df = silver_table.scan().to_arrow().to_pandas()
    assert len(silver_df) > 0, "silver.orders exists but has 0 rows"

    channels_present = set(silver_df["channel"].unique())

    assert "shopify" in channels_present, (
        f"silver.orders is missing channel='shopify' rows. "
        f"Present channels: {channels_present}. "
        "Run the shopify_bronze_ingestion DAG (all 4 tasks) first."
    )
    assert "amazon" in channels_present, (
        f"silver.orders is missing channel='amazon' rows. "
        f"Present channels: {channels_present}. "
        "Run the amazon_bronze_ingestion DAG (all 4 tasks) first."
    )


@pytest.mark.integration
def test_shopify_transform_rerun_does_not_clobber_amazon_rows(require_live_stack):
    """Re-running Shopify Silver must leave Amazon rows in silver.orders untouched.

    Verifies channel-scoped overwrite isolation: channel='shopify' filter ensures
    that re-running the Shopify Silver transform twice in a row does not delete
    or alter Amazon's rows in silver.orders (count and content remain identical).
    """
    from pyiceberg.expressions import EqualTo
    from transforms import amazon_silver, silver_shopify_orders

    catalog = amazon_silver.get_catalog()
    try:
        silver_table = catalog.load_table(amazon_silver.TABLE_IDENTIFIER)
    except NoSuchTableError:
        _unavailable("silver.orders does not exist yet.")
    except Exception as exc:
        _unavailable(f"Live catalog not reachable: {exc}")

    # Scan initial Amazon rows
    amazon_before = silver_table.scan(row_filter=EqualTo("channel", "amazon")).to_arrow()
    if amazon_before.num_rows == 0:
        _unavailable("No Amazon rows in silver.orders to test isolation.")

    # Re-run Shopify Silver transform
    silver_shopify_orders.run()

    # Scan Amazon rows after Shopify Silver re-run
    silver_table = catalog.load_table(amazon_silver.TABLE_IDENTIFIER)
    amazon_after = silver_table.scan(row_filter=EqualTo("channel", "amazon")).to_arrow()

    assert amazon_after.num_rows == amazon_before.num_rows, (
        f"Amazon row count changed after Shopify re-run: "
        f"before={amazon_before.num_rows}, after={amazon_after.num_rows}"
    )
    assert amazon_after.sort_by([("order_id", "ascending")]).equals(
        amazon_before.sort_by([("order_id", "ascending")])
    ), "Amazon row content changed after Shopify re-run!"
