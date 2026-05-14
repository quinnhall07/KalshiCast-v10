"""Shared pytest fixtures and configuration for kalshicast tests.

Centralises:
- synthetic data fixtures (delegating to ``kalshicast.tests.generators``)
- a lightweight MockKalshiAPI for tests that need a stand-in client
- a session-scoped logging silencer
- pytest marker registration (kept in sync with pyproject.toml).
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from kalshicast.tests.generators import (
    generate_observations,
    generate_orderbook,
    generate_shadow_book,
    generate_station_forecasts,
)


# ─────────────────────────────────────────────────────────────────────
# Pytest configuration
# ─────────────────────────────────────────────────────────────────────

def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers so ``--strict-markers`` runs stay green.

    Kept in sync with ``[tool.pytest.ini_options].markers`` in pyproject.toml.
    """
    config.addinivalue_line(
        "markers",
        "integration: marks tests that exercise external systems "
        "(DB, network, Kalshi API)",
    )
    config.addinivalue_line(
        "markers",
        "slow: marks tests that take more than a few seconds to run",
    )


@pytest.fixture(autouse=True, scope="session")
def _quiet_logging() -> None:
    """Suppress INFO/DEBUG log spam during test runs."""
    logging.basicConfig(level=logging.WARNING, force=True)
    # Tame chatty third-party libraries if/when present.
    for noisy in ("urllib3", "requests", "oracledb"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ─────────────────────────────────────────────────────────────────────
# Mock Kalshi client
# ─────────────────────────────────────────────────────────────────────

class MockKalshiAPI:
    """Lightweight stand-in for the Kalshi REST client.

    Tests can override return values via attribute assignment or by setting
    ``side_effect``-style behaviour on the underlying MagicMocks. Methods are
    backed by MagicMocks so call_args_list / call_count assertions work.
    """

    def __init__(self) -> None:
        self.get_events = MagicMock(return_value=[])
        self.get_markets = MagicMock(return_value=[])
        self.get_orderbook = MagicMock(return_value={"yes": [], "no": []})
        self.place_order = MagicMock(return_value={"order_id": "MOCK-1"})
        self.cancel_order = MagicMock(return_value={"status": "cancelled"})
        self.get_positions = MagicMock(return_value=[])


@pytest.fixture
def mock_kalshi_api() -> MockKalshiAPI:
    """Fresh MockKalshiAPI per test."""
    return MockKalshiAPI()


# ─────────────────────────────────────────────────────────────────────
# Synthetic data fixtures (wrap generators.py — do not duplicate logic)
# ─────────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_station() -> dict:
    """A representative station-config row."""
    return {
        "station_id": "KJFK",
        "city": "New York",
        "tz": "America/New_York",
        "active": True,
        "kalshi_high_series": "KXHIGHNY",
        "kalshi_low_series": "KXLOWNY",
    }


@pytest.fixture
def sample_station_forecasts() -> list[dict]:
    """A small block of synthetic forecast rows."""
    return generate_station_forecasts(n_days=5, n_sources=3)


@pytest.fixture
def sample_observations() -> list[dict]:
    """A small block of synthetic observation rows."""
    return generate_observations(n_days=5)


@pytest.fixture
def sample_shadow_book() -> list[dict]:
    """A 5-bin Gaussian-like shadow book centred at 75F."""
    return generate_shadow_book(n_bins=5, center=75.0, bin_width=2.0)


@pytest.fixture
def sample_orderbook() -> dict:
    """A synthetic Kalshi-shaped orderbook with yes/no ladders."""
    return generate_orderbook(depth=5, base_price=40)
