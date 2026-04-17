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
    """Extract bin value from Kalshi ticker (fallback parser).

    Interior bins: B80 → (80.0, False), B77.5 → (77.5, False)
    Tail bins:     T90 → (90.0, True)

    Returns (value, is_tail). Values are floats — modern Kalshi tickers use
    half-integer boundaries (e.g. B77.5) for 2°F-wide bins.
    """
    parts = ticker.split("-")
    if len(parts) < 3:
        raise ValueError(f"Invalid ticker format: {ticker}")

    bin_part = parts[2]  # "B80", "B77.5", or "T90"

    if bin_part.startswith("B"):
        return (float(bin_part[1:]), False)
    elif bin_part.startswith("T"):
        return (float(bin_part[1:]), True)
    else:
        raise ValueError(f"Unknown bin format: {bin_part}")


def compute_bin_boundaries(
    bin_value: float,
    is_tail: bool,
    all_bins: list[tuple[float, bool]],
) -> tuple[float | None, float | None]:
    """Compute bin boundaries from ticker values (fallback path).

    Preferred path is to use ``floor_strike``/``cap_strike`` from the Kalshi
    market payload via :func:`extract_boundaries_from_market`. This helper is
    only used when those fields are missing.

    Heuristics:
    * Interior bins whose label is a half-integer (e.g. ``B77.5``) → the label
      is already the lower boundary; width comes from the gap to the next
      interior value (defaulting to 2°F).
    * Interior bins whose label is an integer (legacy format, e.g. ``B80``) →
      ``(value - 0.5, value + 1.5)``.
    * Tail bins fill the space outside the interior bin range.
    """
    interior_values = sorted(v for v, t in all_bins if not t)

    def _is_half(x: float) -> bool:
        return abs(x - round(x)) > 1e-6

    if not is_tail:
        if _is_half(bin_value):
            width = 2.0
            if len(interior_values) >= 2:
                diffs = [
                    b - a for a, b in zip(interior_values, interior_values[1:])
                    if b - a > 0
                ]
                if diffs:
                    width = min(diffs)
            return (bin_value, bin_value + width)
        return (bin_value - 0.5, bin_value + 1.5)

    if not interior_values:
        return (None, bin_value + 0.5)

    min_interior = min(interior_values)
    max_interior = max(interior_values)

    if bin_value <= min_interior:
        return (None, min_interior if _is_half(min_interior) else bin_value + 0.5)

    if _is_half(max_interior):
        width = 2.0
        if len(interior_values) >= 2:
            diffs = [
                b - a for a, b in zip(interior_values, interior_values[1:])
                if b - a > 0
            ]
            if diffs:
                width = min(diffs)
        return (max_interior + width, None)
    return (bin_value - 0.5, None)


def extract_boundaries_from_market(
    mkt: dict,
) -> tuple[float | None, float | None] | None:
    """Read bin boundaries from Kalshi's ``strike_type``/``floor``/``cap`` fields.

    Returns ``(bin_lower, bin_upper)`` with ``None`` for ±∞, or ``None`` if the
    payload lacks the fields. This is the preferred path — ticker-name parsing
    is only used when the payload doesn't carry strike fields.
    """
    strike_type = (mkt.get("strike_type") or "").lower()
    floor = mkt.get("floor_strike")
    cap = mkt.get("cap_strike")

    if strike_type == "between" and floor is not None and cap is not None:
        return (float(floor), float(cap))
    if strike_type == "less" or (strike_type == "" and cap is not None and floor is None):
        return (None, float(cap)) if cap is not None else None
    if strike_type in ("greater", "greater_or_equal") or (
        strike_type == "" and floor is not None and cap is None
    ):
        return (float(floor), None) if floor is not None else None
    if floor is not None and cap is not None:
        return (float(floor), float(cap))
    return None


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
    from kalshicast.db.connection import to_dt
    from kalshicast.db.operations import upsert_kalshi_market, is_event_ignored

    synced = ignored = errors = 0
    n_markets_seen = 0

    try:
        events = client.get_events(
            status="open",
            series_ticker=series_ticker,
            limit=200,
        )
    except Exception as e:
        log.warning(
            "[kalshi_sync] %s %s series=%s FETCH FAILED: %s",
            station_id, target_type, series_ticker, e,
        )
        return SyncResult(synced=0, unmatched=0, ignored=0, errors=1)

    n_events = len(events)

    def _maybe_dt(value):
        if value is None or value == "":
            return None
        try:
            return to_dt(value)
        except Exception:
            return None

    for event in events:
        event_ticker = event.get("event_ticker", "")

        if is_event_ignored(conn, event_ticker):
            ignored += 1
            continue

        markets = event.get("markets", []) or []
        market_title = event.get("title", "")
        n_markets_seen += len(markets)

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

                # Prefer Kalshi's strike metadata; fall back to ticker parsing.
                bin_lower: float | None
                bin_upper: float | None
                boundaries = extract_boundaries_from_market(mkt)
                if boundaries is not None:
                    bin_lower, bin_upper = boundaries
                else:
                    v, t = parse_ticker_bin(ticker)
                    bin_lower, bin_upper = compute_bin_boundaries(v, t, all_bins)

                market_row = {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "series_ticker": series_ticker,
                    "station_id": station_id,
                    "target_date": target_date,
                    "target_type": target_type,
                    "bin_lower": bin_lower,
                    "bin_upper": bin_upper,
                    "market_title": market_title,
                    "market_subtitle": mkt.get("subtitle"),
                    "close_time": _maybe_dt(mkt.get("close_time")),
                    "settlement_time": _maybe_dt(mkt.get("settlement_time")),
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

    level = log.info if synced > 0 else log.warning
    level(
        "[kalshi_sync] %s %s series=%s events=%d markets_seen=%d synced=%d ignored=%d errors=%d",
        station_id, target_type, series_ticker,
        n_events, n_markets_seen, synced, ignored, errors,
    )

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
    from kalshicast.config.stations import get_stations
    from kalshicast.db.operations import insert_system_alert

    synced = unmatched = ignored = errors = 0
    series_attempted = 0
    series_failed = 0
    series_empty = 0
    series_queried: list[str] = []

    stations = get_stations(active_only=True)

    for station in stations:
        station_id = station["station_id"]

        # Use the series tickers configured per-station. These may use either
        # the plain prefix (KXHIGH/KXLOW) or the "T" variant (KXHIGHT/KXLOWT),
        # and the city portion is not always the cli_site (e.g. KMSY → NOLA,
        # KLAS → LV, KDCA → DC). Reconstructing from a hardcoded prefix +
        # cli_site silently fails for any station that doesn't match that
        # convention — which is 13 of 20 today.
        for target_type, config_key in (
            ("HIGH", "kalshi_high_series"),
            ("LOW", "kalshi_low_series"),
        ):
            series_ticker = station.get(config_key)
            if not series_ticker:
                log.warning(
                    "[kalshi_sync] %s %s has no %s configured — skipping",
                    station_id, target_type, config_key,
                )
                continue

            series_attempted += 1
            series_queried.append(series_ticker)

            result = _sync_one_series(
                conn, client, station_id, target_type, series_ticker,
            )
            synced += result.synced
            ignored += result.ignored
            errors += result.errors
            if result.errors > 0 and result.synced == 0:
                series_failed += 1
            elif result.synced == 0:
                series_empty += 1

    conn.commit()

    log.info(
        "Kalshi sync complete: %d synced, %d unmatched, %d ignored, %d errors "
        "(%d series queried, %d fetch-failed, %d returned zero markets)",
        synced, unmatched, ignored, errors,
        series_attempted, series_failed, series_empty,
    )

    # Sanity check: stations exist but nothing synced → structural failure
    if synced == 0 and len(stations) > 0:
        log.error(
            "Kalshi sync returned 0 markets across %d stations — "
            "likely API change, network failure, or invalid series scheme. "
            "Series queried: %s",
            len(stations), series_queried,
        )
        try:
            insert_system_alert(conn, {
                "alert_type": "KALSHI_SYNC_EMPTY",
                "severity_score": 0.85,
                "details": {
                    "n_stations": len(stations),
                    "series_attempted": series_attempted,
                    "series_failed": series_failed,
                    "series_empty": series_empty,
                    "series_queried": series_queried,
                    "errors": errors,
                },
            })
            conn.commit()
        except Exception as e:
            log.error("Failed to emit KALSHI_SYNC_EMPTY alert: %s", e)

    return SyncResult(
        synced=synced, unmatched=unmatched, ignored=ignored, errors=errors,
    )