"""Unit tests for transforms.ge_bronze_amazon_checkpoint.run_checkpoint().

Exercises the real Great Expectations Fluent-API call chain (ephemeral
context -> pandas datasource -> validator -> per-expectation validator
methods) against small pandas DataFrames. No live Iceberg catalog is
involved -- load_bronze_as_dataframe() (the only I/O in that module) is
never called from these tests.
"""

from __future__ import annotations

import pandas as pd
import pytest

from transforms.ge_bronze_amazon_checkpoint import run_checkpoint


def _valid_amazon_bronze_df(n: int = 5) -> pd.DataFrame:
    statuses = ["shipped", "cancelled", "refund_initiated", "pending", "shipped"]
    marketplaces = ["amazon.in", "amazon.com", "amazon.co.uk", "amazon.in", "amazon.com"]
    return pd.DataFrame(
        {
            "order_id": [f"o{i}" for i in range(n)],
            "customer_id": [f"c{i}" for i in range(n)],
            "status": statuses[:n],
            "unit_price": [100.0, 200.0, 300.0, 400.0, 500.0][:n],
            "quantity": [1, 2, 3, 4, 5][:n],
            "marketplace": marketplaces[:n],
        }
    )


def test_run_checkpoint_passes_for_a_valid_dataframe():
    df = _valid_amazon_bronze_df()
    # Should not raise.
    run_checkpoint(df)


def test_run_checkpoint_raises_for_nulls_in_order_id():
    df = _valid_amazon_bronze_df()
    df.loc[0, "order_id"] = None

    with pytest.raises(RuntimeError, match="Bronze quality gate FAILED"):
        run_checkpoint(df)


def test_run_checkpoint_raises_for_status_outside_source_vocabulary():
    df = _valid_amazon_bronze_df()
    # "completed" is the *canonical* Silver value, not a valid Amazon Bronze
    # source value — exactly the case the contract boundary must reject.
    df.loc[0, "status"] = "completed"

    with pytest.raises(RuntimeError) as excinfo:
        run_checkpoint(df)

    assert "status is one of the Bronze source-valid values" in str(excinfo.value)


def test_run_checkpoint_raises_for_unit_price_out_of_range():
    df = _valid_amazon_bronze_df()
    df.loc[0, "unit_price"] = -5.0

    with pytest.raises(RuntimeError, match="Bronze quality gate FAILED"):
        run_checkpoint(df)


def test_run_checkpoint_raises_for_invalid_marketplace():
    df = _valid_amazon_bronze_df()
    df.loc[0, "marketplace"] = "amazon.de"  # Not in VALID_MARKETPLACES

    with pytest.raises(RuntimeError) as excinfo:
        run_checkpoint(df)

    assert "marketplace is one of" in str(excinfo.value)


def test_run_checkpoint_error_message_lists_every_failed_expectation():
    df = _valid_amazon_bronze_df()
    df.loc[0, "order_id"] = None
    df.loc[1, "unit_price"] = -1.0

    with pytest.raises(RuntimeError) as excinfo:
        run_checkpoint(df)

    message = str(excinfo.value)
    assert "order_id has no nulls" in message
    assert "unit_price is between" in message
