"""Shared fixtures for the CartCo lakehouse test suite."""

from __future__ import annotations

import os
import pathlib
import sys

import pytest

# A "tests" package already exists in site-packages (pulled in transitively
# by one of the Airflow provider dependencies), which shadows this
# repository's own tests/ directory as a namespace package. Adding this
# directory directly to sys.path lets test modules do `from helpers import
# ...` instead of `from tests.helpers import ...`, sidestepping that name
# collision entirely.
_TESTS_DIR = pathlib.Path(__file__).resolve().parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))


def integration_tests_enabled() -> bool:
    """Whether the opt-in env var for live-stack integration tests is set."""
    return os.environ.get("RUN_INTEGRATION_TESTS", "").strip().lower() in {"1", "true", "yes"}


@pytest.fixture(scope="session")
def require_live_stack():
    """Skip the test unless RUN_INTEGRATION_TESTS=1 is set.

    Kept separate from the actual connectivity check (in
    tests/integration/test_live_pipeline.py) so the reason for skipping is
    always explicit: this fixture only gates on the opt-in flag, not on
    whether the stack happens to be reachable.
    """
    if not integration_tests_enabled():
        pytest.skip(
            "Integration tests are opt-in and require the local Docker stack "
            "(docker compose up -d) with data already ingested into "
            "bronze.shopify_orders. Set RUN_INTEGRATION_TESTS=1 to enable."
        )
