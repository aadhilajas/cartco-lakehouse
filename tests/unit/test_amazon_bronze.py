"""Unit tests for ingestion.amazon_bronze schema, constants, and CSV reader."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from ingestion.amazon_bronze import (
    COLUMN_ORDER,
    TABLE_IDENTIFIER,
    TABLE_SCHEMA,
    read_source_csv,
)


def test_table_identifier():
    assert TABLE_IDENTIFIER == "bronze.amazon_orders"


def test_table_schema_shape():
    """TABLE_SCHEMA must contain 12 fields including Amazon-specific columns."""
    field_names = [field.name for field in TABLE_SCHEMA.fields]
    assert len(field_names) == 12
    assert "asin" in field_names
    assert "marketplace" in field_names
    assert "fulfillment_channel" in field_names
    assert "ingestion_date" in field_names


def test_read_source_csv_row_count(tmp_path):
    """read_source_csv should load CSV and add ingestion_date column properly."""
    csv_file = tmp_path / "amazon_orders.csv"
    df_src = pd.DataFrame(
        {
            "order_id": ["ord-1", "ord-2"],
            "customer_id": ["cust-1", "cust-2"],
            "channel": ["amazon", "amazon"],
            "order_date": ["2026-01-01 10:00:00", "2026-01-02 11:00:00"],
            "asin": ["B012345678", "B087654321"],
            "product_category": ["Apparel", "Electronics"],
            "quantity": [1, 2],
            "unit_price": [1500.0, 9500.0],
            "status": ["shipped", "pending"],
            "marketplace": ["amazon.in", "amazon.com"],
            "fulfillment_channel": ["FBA", "FBM"],
        }
    )
    df_src.to_csv(csv_file, index=False)

    today = date(2026, 7, 25)
    df_out, arrow_table = read_source_csv(str(csv_file), today)

    assert len(df_out) == 2
    assert arrow_table.num_rows == 2
    assert "ingestion_date" in arrow_table.schema.names


def test_column_order():
    expected = [
        "order_id",
        "customer_id",
        "channel",
        "order_date",
        "asin",
        "product_category",
        "quantity",
        "unit_price",
        "status",
        "marketplace",
        "fulfillment_channel",
        "ingestion_date",
    ]
    assert COLUMN_ORDER == expected


def test_read_source_csv_file_not_found():
    with pytest.raises(FileNotFoundError, match="Source CSV not found"):
        read_source_csv("non_existent_file.csv", date.today())
