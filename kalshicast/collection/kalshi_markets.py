"""Kalshi market sync — fetch real tickers from Kalshi API.

This module handles:
1. Fetching active weather markets from Kalshi
2. Parsing ticker strings to extract date/bin info
3. Matching Kalshi city codes to our station IDs
4. Storing market data in KALSHI_MARKETS table
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


def parse_iso_utc(ts: Any) -> datetime | None:
    """Parse a Kalshi ISO-8601 timestamp into a Python datetime.

    Kalshi returns timestamps like "2026-04-15T19:00:00Z". Oracle's default
    NLS_TIMESTAMP_TZ_FORMAT does not match this string, so passing the raw
    string to a TIMESTAMP WITH TIME ZONE column triggers implicit-conversion
    errors (ORA-01843 / ORA-01861). Parsing to a datetime lets oracledb bind
    it as a native TIMESTAMP.
    """
    if not ts or not isinstance(ts, str):
        return None
    try:
        # Python <3.11 rejects the trailing 'Z'; normalize it first.
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None


def parse_ticker_bin(ticker: str) -> tuple[float, bool]:
    """Extract bin value from Kalshi ticker.

    Interior bins: B80 → (80.0, False), B77.5 → (77.5, False)
    Tail bins: T90 → (90.0, True)

    Kalshi uses half-degree bins (e.g. B77.5) for some cities, so parse
    as float rather than int.

    Returns (value, is_tail).
    """
    parts = ticker.split("-")
    if len(parts) < 3:
        raise ValueError(f"Invalid ticker format: {ticker}")

    bin_part = parts[2]  # "B80" or "T90" or "B77.5"

    if bin_part.startswith("B"):
        center = float(bin_part[1:])
        return (center, False)
    elif bin_part.startswith("T"):
        threshold = float(bin_part[1:])
        return (threshold, True)
    else:
        raise ValueError(f"Unknown bin format: {bin_part}")


def compute_bin_boundaries(bin_value: float, is_tail: bool,
                           all_bins: list[tuple[float, bool]]) -> tuple[float | None, float | None]:
    """Compute actual bin boundaries.
    
    For interior bins: (value - 0.5, value + 1.5)
    For tail bins: determine if low or high tail from context.
    
    Returns (lower, upper) where None represents infinity.
    """
    if not is_tail:
        return (bin_value - 0.5, bin_value + 1.5)
    
    # For tail bins, find if this is the lowest or highest
    non_tail_values = [v for v, t in all_bins if not t]
    
    if not non_tail_values:
        # Only tail bins? Unusual, default to low tail
        return (None, bin_value + 0.5)
    
    min_interior = min(non_tail_values)
    max_interior = max(non_tail_values)
    
    if bin_value <= min_interior:
        # Low tail: everything below this value
        return (None, bin_value + 0.5)
    else:
        # High tail: everything at/above this value
        return (bin_value - 0.5, None)


def extract_city_code(event_ticker: str) -> str:
    """Extract city code from event ticker.

    KXHIGHNYC-26APR13 → NYC
    KXLOWCHI-26APR13 → CHI
    KXHIGHNYC → NYC (bare series form)
    """
    # Event tickers have the form "{series}-{date}", e.g. "KXHIGHNYC-26APR13".
    # Strip the date suffix first so [6:] / [5:] yield just the city code.
    series = event_ticker.split("-", 1)[0]
    if series.startswith("KXHIGH"):
        return series[6:]
    elif series.startswith("KXLOW"):
        return series[5:]
    else:
        raise ValueError(f"Unknown event ticker format: {event_ticker}")


# ─────────────────────────────────────────────────────────────────────
# Station Matching
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
        city_code = extract_city_code(event_ticker)
    except ValueError:
        log.warning("Could not extract city code from %s", event_ticker)
        return None
    
    # Try exact match via kalshi_city or cli_site
    station = get_station_by_kalshi_city(city_code)
    if station:
        return station["station_id"]
    
    # Try fuzzy match on city name in title
    title_upper = market_title.upper()
    for station in get_stations():
        if station["city"].upper() in title_upper:
            return station["station_id"]
    
    return None


# ─────────────────────────────────────────────────────────────────────
# Main Sync Function
# ─────────────────────────────────────────────────────────────────────

@dataclass
class SyncResult:
    """Result of a Kalshi market sync operation."""
    synced: int
    unmatched: int
    ignored: int
    errors: int


def sync_kalshi_markets(conn: Any, client: Any) -> SyncResult:
    """Fetch and sync all active weather markets from Kalshi.
    
    1. Fetch KXHIGH and KXLOW events from Kalshi API
    2. Parse each market ticker
    3. Match to our stations
    4. Upsert to KALSHI_MARKETS
    5. Create alerts for unmatched stations
    
    Returns SyncResult with counts.
    """
    from kalshicast.db.operations import (
        upsert_kalshi_market, is_event_ignored,
        kalshi_alert_exists, create_unknown_station_alert
    )
    
    synced = 0
    unmatched = 0
    ignored = 0
    errors = 0
    
    # Track which events we've already alerted on this run
    alerted_events: set[str] = set()

    # Fetch weather events by broad series.  Kalshi organises series as
    # "KXHIGH" / "KXLOW" (not per-city), so two calls cover every city.
    # Events have city-specific event_tickers like "KXHIGHNYC-26APR16".
    log.info("Kalshi: base_url=%s", getattr(client, "base_url", "?"))
    from kalshicast.config.stations import get_stations

    stations = get_stations(active_only=True)
    weather_events: list[dict] = []
    per_series_counts: list[tuple[str, int]] = []

    for series_prefix in ("KXHIGH", "KXLOW"):
        try:
            events = client.get_events(
                status="open", series_ticker=series_prefix, limit=200,
            )
        except Exception as e:
            log.warning("Kalshi: fetch %s failed: %s", series_prefix, e)
            events = []

        # Keep only events whose event_ticker belongs to this series
        events = [
            e for e in events
            if e.get("event_ticker", "").startswith(series_prefix)
            or (e.get("series_ticker") or "").startswith(series_prefix)
        ]
        per_series_counts.append((series_prefix, len(events)))
        weather_events.extend(events)

    total_fetched = sum(n for _, n in per_series_counts)
    log.info(
        "Kalshi: fetched %d weather events (%s)",
        total_fetched,
        ", ".join(f"{s}={n}" for s, n in per_series_counts),
    )

    # Log per-city breakdown for transparency
    city_event_map: dict[str, list[str]] = {}
    for ev in weather_events:
        et = ev.get("event_ticker", "")
        try:
            cc = extract_city_code(et)
        except ValueError:
            cc = "UNKNOWN"
        city_event_map.setdefault(cc, []).append(et)
    for cc in sorted(city_event_map):
        log.info("  Kalshi: city=%s -> %d events: %s",
                 cc, len(city_event_map[cc]),
                 ", ".join(city_event_map[cc][:4]))

    # ── Coverage check: alert on missing stations ────────────────────
    expected_city_types: set[str] = set()
    city_to_station: dict[str, str] = {}
    for s in stations:
        cc = s.get("kalshi_city") or s["cli_site"]
        city_to_station[cc] = s["station_id"]
        expected_city_types.add(f"{cc}|HIGH")
        expected_city_types.add(f"{cc}|LOW")

    found_city_types: set[str] = set()
    for ev in weather_events:
        et = ev.get("event_ticker", "")
        try:
            cc = extract_city_code(et)
        except ValueError:
            continue
        tt = "HIGH" if et.startswith("KXHIGH") else "LOW"
        found_city_types.add(f"{cc}|{tt}")

    missing = expected_city_types - found_city_types
    if missing:
        missing_list = sorted(missing)
        log.warning(
            "Kalshi: MISSING %d/%d expected station series: %s",
            len(missing), len(expected_city_types),
            ", ".join(missing_list[:20]),
        )
        create_unknown_station_alert(
            conn,
            "COVERAGE_GAP",
            f"Missing {len(missing)}/{len(expected_city_types)} series",
            ", ".join(missing_list[:10]),
        )
    else:
        log.info("Kalshi: full coverage — all %d station series found",
                 len(expected_city_types))

    if total_fetched == 0:
        log.warning(
            "Kalshi: no weather events returned. "
            "Verify that KXHIGH/KXLOW series exist.",
        )

    for event in weather_events:
        event_ticker = event.get("event_ticker", "")

        # Skip ignored events
        if is_event_ignored(conn, event_ticker):
            ignored += 1
            continue

        # Determine target type and series prefix from event ticker
        if event_ticker.startswith("KXHIGH"):
            target_type = "HIGH"
            series = "KXHIGH"
        else:
            target_type = "LOW"
            series = "KXLOW"

        # Match to our station
        market_title = event.get("title", "")
        station_id = match_kalshi_to_station(event_ticker, market_title)

        # Get nested markets
        markets = event.get("markets", [])

        # Collect all bins to determine tail boundaries
        all_bins: list[tuple[float, bool]] = []
        for mkt in markets:
            try:
                ticker = mkt.get("ticker", "")
                val, is_tail = parse_ticker_bin(ticker)
                all_bins.append((val, is_tail))
            except Exception:
                pass

        for mkt in markets:
            try:
                ticker = mkt.get("ticker", "")
                target_date = parse_ticker_date(ticker)
                val, is_tail = parse_ticker_bin(ticker)
                bin_lower, bin_upper = compute_bin_boundaries(val, is_tail, all_bins)

                market_row = {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "series_ticker": series,
                    "station_id": station_id,
                    # Pass date object directly; oracledb binds it as Oracle
                    # DATE natively, avoiding any TO_DATE/NLS conversion issues.
                    "target_date": target_date,
                    "target_type": target_type,
                    "bin_lower": bin_lower,
                    "bin_upper": bin_upper,
                    "market_title": market_title,
                    "market_subtitle": mkt.get("subtitle"),
                    "close_time": parse_iso_utc(mkt.get("close_time")),
                    "settlement_time": parse_iso_utc(mkt.get("settlement_time")),
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
                log.warning("Failed to process market %s: %s",
                           mkt.get("ticker", "unknown"), e)
                errors += 1

        # Create alert for unmatched station (once per event)
        if station_id is None and event_ticker not in alerted_events:
            if not kalshi_alert_exists(conn, event_ticker):
                sample_ticker = markets[0].get("ticker") if markets else event_ticker
                create_unknown_station_alert(conn, event_ticker, market_title, sample_ticker)
                log.warning("Unknown Kalshi station: %s (%s)", event_ticker, market_title)
            alerted_events.add(event_ticker)
            unmatched += 1

    conn.commit()
    log.info("Kalshi sync complete: %d synced, %d unmatched, %d ignored, %d errors",
             synced, unmatched, ignored, errors)
    
    return SyncResult(synced=synced, unmatched=unmatched, ignored=ignored, errors=errors)