"""Kalshi market sync — fetch real tickers from Kalshi API.

This module handles:
1. Fetching active weather markets from Kalshi (per-city, per-target_type)
2. Parsing ticker strings to extract date/bin info
3. Matching Kalshi city codes to our station IDs
4. Storing market data in KALSHI_MARKETS table
5. Emitting a KALSHI_SYNC_EMPTY alert when no markets sync at all
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Ticker Parsing
# ─────────────────────────────────────────────────────────────────────

def parse_ticker_date(ticker: str) -> date:
    """Extract date from Kalshi ticker.

    Format: KXHIGHNYC-26APR13-B80 → 2026-04-13
    Date portion is YYMMMDD (e.g., 26APR13)
    """
    parts = ticker.split("-")
    if len(parts) < 2:
        raise ValueError(f"Invalid ticker format: {ticker}")

    date_str = parts[1]  # "26APR13"
    return datetime.strptime(date_str, "%y%b%d").date()


def parse_ticker_bin(ticker: str) -> tuple[float, bool]:
    """Extract bin value from Kalshi ticker.

    Interior bins: B80 → (80, False)
    Tail bins:     T90 → (90, True)

    Returns (value, is_tail).
    """
    parts = ticker.split("-")
    if len(parts) < 3:
        raise ValueError(f"Invalid ticker format: {ticker}")

    bin_part = parts[2]  # "B80" or "T90"

    if bin_part.startswith("B"):
        return (int(bin_part[1:]), False)
    elif bin_part.startswith("T"):
        return (int(bin_part[1:]), True)
    else:
        raise ValueError(f"Unknown bin format: {bin_part}")


def compute_bin_boundaries(
    bin_value: float,
    is_tail: bool,
    all_bins: list[tuple[float, bool]],
) -> tuple[float | None, float | None]:
    """Compute actual bin boundaries.

    For interior bins: (value - 0.5, value + 1.5)
    For tail bins: determine if low or high tail from context.

    Returns (lower, upper) where None represents infinity.
    """
    if not is_tail:
        return (bin_value - 0.5, bin_value + 1.5)

    non_tail_values = [v for v, t in all_bins if not t]

    if not non_tail_values:
        # Only tail bins? Unusual, default to low tail
        return (None, bin_value + 0.5)

    min_interior = min(non_tail_values)

    if bin_value <= min_interior:
        return (None, bin_value + 0.5)
    return (bin_value - 0.5, None)


def extract_city_code(event_ticker: str) -> str:
    """Extract city code from event ticker.

    KXHIGHNYC → NYC
    KXLOWCHI  → CHI
    """
    if event_ticker.startswith("KXHIGH"):
        return event_ticker[6:]
    elif event_ticker.startswith("KXLOW"):
        return event_ticker[5:]
    else:
        raise ValueError(f"Unknown event ticker format: {event_ticker}")


# ─────────────────────────────────────────────────────────────────────
# Station Matching (kept for backward compatibility — unused by the
# per-city sync below, since station_id is known from the loop variable)
# ─────────────────────────────────────────────────────────────────────

def match_kalshi_to_station(event_ticker: str, market_title: str) -> str | None:
    """Match a Kalshi event to our internal station_id.

    Strategy:
    1. Extract city code from event ticker
    2. Try exact match on kalshi_city or cli_site
    3. Try fuzzy match on city name in title
    4. Return None if no match
    """
    from kalshicast.config.stations import get_station_by_kalshi_city, get_stations

    try:
        # event_ticker may include date suffix (e.g. "KXHIGHNYC-26APR16");
        # extract_city_code expects the bare series prefix.
        prefix = event_ticker.split("-")[0]
        city_code = extract_city_code(prefix)
    except ValueError:
        log.warning("Could not extract city code from %s", event_ticker)
        return None

    station = get_station_by_kalshi_city(city_code)
    if station:
        return station["station_id"]

    title_upper = market_title.upper()
    for station in get_stations():
        if station["city"].upper() in title_upper:
            return station["station_id"]

    return None


# ─────────────────────────────────────────────────────────────────────
# Sync — main + per-series helper
# ─────────────────────────────────────────────────────────────────────

@dataclass
class SyncResult:
    """Result of a Kalshi market sync operation."""
    synced: int
    unmatched: int
    ignored: int
    errors: int


def _sync_one_series(
    conn: Any,
    client: Any,
    station_id: str,
    target_type: str,
    series_ticker: str,
) -> SyncResult:
    """Fetch and upsert all open markets for a single (station, target_type) series.

    Failures are isolated to this call: a raised exception is logged, counted
    as 1 error, and returned. Other series continue.
    """
    from kalshicast.db.operations import upsert_kalshi_market, is_event_ignored

    series_prefix = "KXHIGH" if target_type == "HIGH" else "KXLOW"
    synced = ignored = errors = 0

    try:
        events = client.get_events(
            status="open",
            series_ticker=series_ticker,
            limit=200,
        )
    except Exception as e:
        log.warning(
            "Kalshi: failed to fetch series %s for %s: %s",
            series_ticker, station_id, e,
        )
        return SyncResult(synced=0, unmatched=0, ignored=0, errors=1)

    for event in events:
        event_ticker = event.get("event_ticker", "")

        if is_event_ignored(conn, event_ticker):
            ignored += 1
            continue

        markets = event.get("markets", []) or []
        market_title = event.get("title", "")

        # Pre-pass: collect all bin values to determine tail boundaries
        all_bins: list[tuple[float, bool]] = []
        for mkt in markets:
            try:
                v, t = parse_ticker_bin(mkt.get("ticker", ""))
                all_bins.append((v, t))
            except Exception:
                pass

        for mkt in markets:
            try:
                ticker = mkt.get("ticker", "")
                target_date = parse_ticker_date(ticker)
                v, t = parse_ticker_bin(ticker)
                bin_lower, bin_upper = compute_bin_boundaries(v, t, all_bins)

                market_row = {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "series_ticker": series_prefix,
                    "station_id": station_id,
                    "target_date": target_date.isoformat(),
                    "target_type": target_type,
                    "bin_lower": bin_lower,
                    "bin_upper": bin_upper,
                    "market_title": market_title,
                    "market_subtitle": mkt.get("subtitle"),
                    "close_time": mkt.get("close_time"),
                    "settlement_time": mkt.get("settlement_time"),
                    "status": mkt.get("status"),
                    "last_price": mkt.get("last_price"),
                    "volume": mkt.get("volume"),
                    "yes_bid": mkt.get("yes_bid"),
                    "yes_ask": mkt.get("yes_ask"),
                    "raw": mkt,
                }
                upsert_kalshi_market(conn, market_row)
                synced += 1
            except Exception as e:
                log.warning(
                    "Failed to process market %s: %s",
                    mkt.get("ticker", "unknown"), e,
                )
                errors += 1

    return SyncResult(synced=synced, unmatched=0, ignored=ignored, errors=errors)


def sync_kalshi_markets(conn: Any, client: Any) -> SyncResult:
    """Fetch and sync all active weather markets from Kalshi.

    Iterates over each configured station × {HIGH, LOW} and queries Kalshi's
    /events endpoint with the per-city series_ticker (e.g. KXHIGHNYC,
    KXLOWCHI). This avoids the /events 200-item cap that previously caused
    weather events to be silently dropped when the platform had >200 open
    events overall.

    Errors in any single series are isolated and counted; other series
    continue. After the loop, if zero markets were synced despite stations
    being configured, a KALSHI_SYNC_EMPTY system alert is emitted (a strong
    signal of API change, network failure, or mis-configured series scheme).
    """
    from kalshicast.config.stations import get_stations, get_kalshi_city
    from kalshicast.db.operations import insert_system_alert

    synced = unmatched = ignored = errors = 0
    series_attempted = 0
    series_failed = 0

    stations = get_stations(active_only=True)

    for station in stations:
        station_id = station["station_id"]
        kalshi_city = get_kalshi_city(station_id)

        for target_type, series_prefix in (("HIGH", "KXHIGH"), ("LOW", "KXLOW")):
            series_ticker = f"{series_prefix}{kalshi_city}"
            series_attempted += 1

            result = _sync_one_series(
                conn, client, station_id, target_type, series_ticker,
            )
            synced += result.synced
            ignored += result.ignored
            errors += result.errors
            if result.synced == 0 and result.errors > 0:
                series_failed += 1

    conn.commit()

    log.info(
        "Kalshi sync complete: %d synced, %d unmatched, %d ignored, %d errors "
        "(%d series queried, %d failed)",
        synced, unmatched, ignored, errors, series_attempted, series_failed,
    )

    # Sanity check: stations exist but nothing synced → structural failure
    if synced == 0 and len(stations) > 0:
        log.error(
            "Kalshi sync returned 0 markets across %d stations — "
            "likely API change, network failure, or invalid series scheme",
            len(stations),
        )
        try:
            insert_system_alert(conn, {
                "alert_type": "KALSHI_SYNC_EMPTY",
                "severity_score": 0.85,
                "details": {
                    "n_stations": len(stations),
                    "series_attempted": series_attempted,
                    "series_failed": series_failed,
                    "errors": errors,
                },
            })
            conn.commit()
        except Exception as e:
            log.error("Failed to emit KALSHI_SYNC_EMPTY alert: %s", e)

    return SyncResult(
        synced=synced, unmatched=unmatched, ignored=ignored, errors=errors,
    )