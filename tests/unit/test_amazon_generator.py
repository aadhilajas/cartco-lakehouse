"""Unit tests for ingestion.generate_synthetic_amazon_data.generate().

Validates the synthetic Amazon orders DataFrame against the field-level
contracts specified in the implementation plan: correct row count, column
set, channel constant, Amazon-specific status vocabulary, fulfillment
channels, marketplaces, ASIN format, and reproducibility via seed.
"""

from __future__ import annotations

import re

from ingestion.generate_synthetic_amazon_data import (
    FULFILLMENT_CHANNELS,
    MARKETPLACES,
    N_ROWS,
    ORDER_STATUSES,
    generate,
)

EXPECTED_COLUMNS = [
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
]

# Pre-generate once for all tests (deterministic via SEED=42).
_DF = generate()


def test_generate_returns_expected_row_count():
    assert len(_DF) == N_ROWS


def test_generate_has_expected_columns():
    assert list(_DF.columns) == EXPECTED_COLUMNS


def test_generate_channel_is_always_amazon():
    unique_channels = _DF["channel"].unique().tolist()
    assert unique_channels == ["amazon"]


def test_generate_statuses_are_source_valid():
    actual = set(_DF["status"].unique())
    expected = set(ORDER_STATUSES)
    assert actual == expected, f"Unexpected statuses: {actual - expected}"


def test_generate_fulfillment_channels_are_valid():
    actual = set(_DF["fulfillment_channel"].unique())
    expected = set(FULFILLMENT_CHANNELS)
    assert actual == expected, f"Unexpected fulfillment channels: {actual - expected}"


def test_generate_marketplaces_are_valid():
    actual = set(_DF["marketplace"].unique())
    expected = set(MARKETPLACES)
    assert actual == expected, f"Unexpected marketplaces: {actual - expected}"


def test_generate_asin_format():
    """Every ASIN must match the real Amazon format: B0 + 8 uppercase alphanumeric."""
    asin_pattern = re.compile(r"^B0[A-Z0-9]{8}$")
    invalid = _DF[~_DF["asin"].str.match(asin_pattern)]
    assert len(invalid) == 0, f"Found {len(invalid)} invalid ASINs, e.g.: {invalid['asin'].head(3).tolist()}"


def test_generate_is_deterministic_with_seed():
    """Seeded columns must be identical across two calls to generate().

    ``order_id`` uses ``uuid.uuid4()`` (not seeded) and ``order_date``
    depends on ``pd.Timestamp.now()``, so those two columns are expected
    to differ between calls. Every other column is driven by the seeded
    Faker / NumPy RNG and must be identical.
    """
    df2 = generate()
    seeded_cols = [c for c in _DF.columns if c not in ("order_id", "order_date")]
    assert _DF[seeded_cols].equals(df2[seeded_cols]), (
        "generate() is not deterministic for seeded columns across calls"
    )
