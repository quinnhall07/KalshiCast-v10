"""Tests for kalshicast.collection.kalshi_markets.

Covers the per-(station, target_type) series_ticker loop and the
KALSHI_SYNC_EMPTY sanity-check alert.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock


# ─────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_conn():
    """A MagicMock conn — supports cursor() and commit() attribute access."""
    return MagicMock()


@pytest.fixture
def mock_client():
    """A MagicMock Kalshi client."""
    return MagicMock()


@pytest.fixture
def patched_db(monkeypatch):
    """Patch db.operations functions; capture upserts and alerts.

    Returns a dict with two lists: 'upserts' and 'alerts'.
    """
    captured = {"upserts": [], "alerts": []}

    def fake_upsert(conn, market):
        captured["upserts"].append(market)

    def fake_is_ignored(conn, event_ticker):
        return False

    def fake_alert(conn, alert):
        captured["alerts"].append(alert)

    # Patch at the SOURCE module — kalshi_markets imports lazily from there
    monkeypatch.setattr(
        "kalshicast.db.operations.upsert_kalshi_market", fake_upsert
    )
    monkeypatch.setattr(
        "kalshicast.db.operations.is_event_ignored", fake_is_ignored
    )
    monkeypatch.setattr(
        "kalshicast.db.operations.insert_system_alert", fake_alert
    )

    return captured


def _make_event(event_ticker: str, ticker_suffixes: list[str]) -> dict:
    """Build a minimal event dict with nested markets.

    event_ticker: e.g. "KXHIGHNYC-26APR16"
    ticker_suffixes: e.g. ["B70", "B72", "B74"] → produces full market tickers.
    """
    base = event_ticker  # e.g. "KXHIGHNYC-26APR16"
    return {
        "event_ticker": base,
        "title": f"Test event {base}",
        "markets": [
            {
                "ticker": f"{base}-{suffix}",
                "subtitle": "test bin",
                "close_time": "2026-04-16T20:00:00Z",
                "settlement_time": "2026-04-16T21:00:00Z",
                "status": "active",
                "last_price": 50,
                "volume": 100,
                "yes_bid": 49,
                "yes_ask": 51,
            }
            for suffix in ticker_suffixes
        ],
    }


# ─────────────────────────────────────────────────────────────────────
# Per-city fetch behavior
# ─────────────────────────────────────────────────────────────────────

class TestPerCityFetch:
    """Verify the sync iterates per (station, target_type) using series_ticker."""

    def test_calls_get_events_once_per_station_and_target(
        self, mock_conn, mock_client, patched_db
    ):
        """One call per station × {HIGH, LOW} = N×2 calls total."""
        from kalshicast.collection.kalshi_markets import sync_kalshi_markets
        from kalshicast.config.stations import get_stations

        mock_client.get_events.return_value = []

        sync_kalshi_markets(mock_conn, mock_client)

        n_stations = len(get_stations(active_only=True))
        assert mock_client.get_events.call_count == n_stations * 2, (
            f"Expected {n_stations * 2} get_events calls, "
            f"got {mock_client.get_events.call_count}"
        )

    def test_uses_correct_series_ticker_per_station(
        self, mock_conn, mock_client, patched_db
    ):
        """series_ticker should be KX{HIGH|LOW}{kalshi_city}, honoring overrides."""
        from kalshicast.collection.kalshi_markets import sync_kalshi_markets

        mock_client.get_events.return_value = []

        sync_kalshi_markets(mock_conn, mock_client)

        called_series = {
            kwargs.get("series_ticker")
            for _, kwargs in mock_client.get_events.call_args_list
        }

        # Plain stations use cli_site
        assert "KXHIGHNYC" in called_series
        assert "KXLOWNYC" in called_series
        # Overrides honored: KMDW → CHI, KLAX → LA
        assert "KXHIGHCHI" in called_series
        assert "KXLOWCHI" in called_series
        assert "KXHIGHLA" in called_series
        assert "KXLOWLA" in called_series

    def test_passes_status_open_filter(
        self, mock_conn, mock_client, patched_db
    ):
        """All calls should request status=open."""
        from kalshicast.collection.kalshi_markets import sync_kalshi_markets

        mock_client.get_events.return_value = []

        sync_kalshi_markets(mock_conn, mock_client)

        for _, kwargs in mock_client.get_events.call_args_list:
            assert kwargs.get("status") == "open"

    def test_one_series_failure_does_not_blank_others(
        self, mock_conn, mock_client, patched_db
    ):
        """If one (station, target) call raises, others must still proceed."""
        from kalshicast.collection.kalshi_markets import sync_kalshi_markets

        ok_event = _make_event("KXHIGHLA-26APR16", ["B72"])

        def side_effect(*args, **kwargs):
            series = kwargs.get("series_ticker", "")
            if series == "KXHIGHNYC":
                raise RuntimeError("simulated 500")
            if series == "KXHIGHLA":
                return [ok_event]
            return []

        mock_client.get_events.side_effect = side_effect

        result = sync_kalshi_markets(mock_conn, mock_client)

        # One series failed, but the LA series still produced a synced market
        assert result.errors >= 1, "the failing series should be counted as an error"
        assert result.synced >= 1, (
            "the OK series should still have produced upserts despite NYC failing"
        )


# ─────────────────────────────────────────────────────────────────────
# Sanity-check alert
# ─────────────────────────────────────────────────────────────────────

class TestSanityCheckAlert:
    """Verify the KALSHI_SYNC_EMPTY alert fires iff synced=0 with stations>0."""

    def test_alert_fires_when_zero_synced(
        self, mock_conn, mock_client, patched_db
    ):
        """If 0 markets sync across all stations, emit a high-severity alert."""
        from kalshicast.collection.kalshi_markets import sync_kalshi_markets

        mock_client.get_events.return_value = []

        sync_kalshi_markets(mock_conn, mock_client)

        sync_alerts = [
            a for a in patched_db["alerts"]
            if a.get("alert_type") == "KALSHI_SYNC_EMPTY"
        ]
        assert len(sync_alerts) == 1, (
            f"Expected exactly 1 KALSHI_SYNC_EMPTY alert, "
            f"got {len(sync_alerts)}: {patched_db['alerts']}"
        )
        assert sync_alerts[0]["severity_score"] >= 0.7, (
            "KALSHI_SYNC_EMPTY should be high severity"
        )

    def test_no_alert_when_synced_nonzero(
        self, mock_conn, mock_client, patched_db
    ):
        """If at least one market syncs, the empty-sync alert must NOT fire."""
        from kalshicast.collection.kalshi_markets import sync_kalshi_markets

        ok_event = _make_event("KXHIGHNYC-26APR16", ["B72"])
        mock_client.get_events.return_value = [ok_event]

        sync_kalshi_markets(mock_conn, mock_client)

        sync_alerts = [
            a for a in patched_db["alerts"]
            if a.get("alert_type") == "KALSHI_SYNC_EMPTY"
        ]
        assert sync_alerts == [], (
            f"Expected no KALSHI_SYNC_EMPTY alerts when sync produced markets, "
            f"got {sync_alerts}"
        )

    def test_alert_includes_diagnostic_details(
        self, mock_conn, mock_client, patched_db
    ):
        """The alert payload should include n_stations and series_attempted."""
        from kalshicast.collection.kalshi_markets import sync_kalshi_markets
        from kalshicast.config.stations import get_stations

        mock_client.get_events.return_value = []

        sync_kalshi_markets(mock_conn, mock_client)

        alert = next(
            a for a in patched_db["alerts"]
            if a.get("alert_type") == "KALSHI_SYNC_EMPTY"
        )
        details = alert.get("details", {})
        n = len(get_stations(active_only=True))
        assert details.get("n_stations") == n
        assert details.get("series_attempted") == n * 2


# ─────────────────────────────────────────────────────────────────────
# Synced market shape
# ─────────────────────────────────────────────────────────────────────

class TestSyncedMarketShape:
    """Spot-check that upserted rows have correct station_id and target_type."""

    def test_high_event_creates_high_row_for_correct_station(
        self, mock_conn, mock_client, patched_db
    ):
        """A KXHIGHNYC event under the NYC HIGH series → row with station=KNYC, type=HIGH."""
        from kalshicast.collection.kalshi_markets import sync_kalshi_markets

        ok_event = _make_event("KXHIGHNYC-26APR16", ["B72", "B74"])

        def side_effect(*args, **kwargs):
            if kwargs.get("series_ticker") == "KXHIGHNYC":
                return [ok_event]
            return []

        mock_client.get_events.side_effect = side_effect

        sync_kalshi_markets(mock_conn, mock_client)

        nyc_high_rows = [
            u for u in patched_db["upserts"]
            if u["station_id"] == "KNYC" and u["target_type"] == "HIGH"
        ]
        assert len(nyc_high_rows) == 2, (
            f"Expected 2 upserted markets (B72, B74), got {len(nyc_high_rows)}"
        )

    def test_low_event_under_chi_series_maps_to_kmdw(
        self, mock_conn, mock_client, patched_db
    ):
        """KXLOWCHI series should produce rows for station KMDW (override active)."""
        from kalshicast.collection.kalshi_markets import sync_kalshi_markets

        ok_event = _make_event("KXLOWCHI-26APR16", ["B40"])

        def side_effect(*args, **kwargs):
            if kwargs.get("series_ticker") == "KXLOWCHI":
                return [ok_event]
            return []

        mock_client.get_events.side_effect = side_effect

        sync_kalshi_markets(mock_conn, mock_client)

        chi_rows = [u for u in patched_db["upserts"] if u["station_id"] == "KMDW"]
        assert len(chi_rows) == 1
        assert chi_rows[0]["target_type"] == "LOW"