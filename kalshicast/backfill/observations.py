# kalshicast/backfill/observations.py
"""Historical observation loaders — all NWS sources.

Primary:   NWS station observations API (hourly JSON, chunked by month).
Secondary: NWS CLI product archive (exact text bulletin, overwrites primary rows).

Both call the existing upsert_observation() — no new write paths.
CLI values always win over station-obs values for the same (station, date).
"""
from __future__ import annotations

import calendar
import logging
import time
from datetime import date, timedelta
from typing import Any

import requests

from kalshicast.backfill.config import (
    NWS_OBS_URL, NWS_OBS_CHUNK_MONTHS, NWS_OBS_LIMIT, NWS_OBS_REQUEST_SLEEP,
    NWS_PRODUCTS_URL, CLI_MAX_PRODUCTS_PER_STATION, NWS_REQUEST_SLEEP,
)
from kalshicast.collection.collectors.collect_cli import c_to_f
from kalshicast.config import HEADERS
from kalshicast.db.operations import upsert_observation

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# NWS Station Observations API (primary source)
# ─────────────────────────────────────────────────────────────────────

def _month_chunks(start_date: str, end_date: str) -> list[tuple[str, str]]:
    """Split a date window into (chunk_start, chunk_end) month-sized slices.

    Each chunk is one calendar month, clipped to [start_date, end_date].
    This keeps NWS API responses under the 500-observation limit.
    """
    s = date.fromisoformat(start_date)
    e = date.fromisoformat(end_date)
    chunks: list[tuple[str, str]] = []

    current = s.replace(day=1)  # start at beginning of first month
    while current <= e:
        _, last_day = calendar.monthrange(current.year, current.month)
        chunk_end = current.replace(day=last_day)
        # Clip to actual window bounds
        chunk_start = max(current, s)
        chunk_end   = min(chunk_end, e)
        chunks.append((chunk_start.isoformat(), chunk_end.isoformat()))
        # Advance to first day of next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return chunks


def _build_nws_obs_params(
    station_id: str,
    chunk_start: str,
    chunk_end: str,
) -> tuple[str, dict]:
    """Build NWS observations API URL and params for one month chunk.

    Returns (url, params_dict).
    The API accepts ISO-8601 UTC timestamps for start/end.
    """
    url = f"{NWS_OBS_URL}/{station_id}/observations"
    params = {
        "start": f"{chunk_start}T00:00:00Z",
        "end":   f"{chunk_end}T23:59:59Z",
        "limit": NWS_OBS_LIMIT,
    }
    return url, params


def _parse_nws_obs_to_daily(geojson: dict) -> dict[str, dict]:
    """Parse NWS observations GeoJSON into {date_str: {high_f, low_f}}.

    Temperature values are always Celsius in NWS API responses.
    Null values are skipped. Timestamps are UTC ISO-8601.
    """
    features = geojson.get("features") or []
    per_date: dict[str, list[float]] = {}

    for feature in features:
        props = (feature.get("properties") or {})
        ts = props.get("timestamp") or ""
        temp_obj = props.get("temperature") or {}
        temp_c = temp_obj.get("value")

        if not ts or temp_c is None:
            continue
        try:
            temp_c_f = float(temp_c)
        except (TypeError, ValueError):
            continue

        # UTC date from timestamp prefix
        date_str = ts[:10]
        if len(date_str) < 10:
            continue

        temp_f = round(c_to_f(temp_c_f), 1)
        per_date.setdefault(date_str, []).append(temp_f)

    result: dict[str, dict] = {}
    for d, temps in per_date.items():
        if not temps:
            continue
        result[d] = {
            "high_f": round(max(temps), 1),
            "low_f":  round(min(temps), 1),
        }
    return result


def load_nws_station_observations(
    conn: Any,
    station_id: str,
    start_date: str,
    end_date: str,
) -> int:
    """Fetch NWS station hourly observations for one station over the full window.

    Chunks into one-month slices. Aggregates hourly readings to daily high/low.
    Calls upsert_observation() for each (station, date) pair found.
    Source tag: 'NWS_OBS_BACKFILL'.

    Returns count of observation-dates upserted.
    """
    chunks = _month_chunks(start_date, end_date)
    total = 0

    for chunk_start, chunk_end in chunks:
        url, params = _build_nws_obs_params(station_id, chunk_start, chunk_end)

        try:
            resp = requests.get(
                url,
                params=params,
                headers={**dict(HEADERS), "Accept": "application/geo+json"},
                timeout=30,
            )
            resp.raise_for_status()
            geojson = resp.json()
        except Exception as ex:
            log.warning("[nws_obs] %s %s–%s failed: %s",
                        station_id, chunk_start, chunk_end, ex)
            time.sleep(NWS_OBS_REQUEST_SLEEP)
            continue

        daily = _parse_nws_obs_to_daily(geojson)

        for d_str, temps in daily.items():
            try:
                upsert_observation(
                    conn,
                    station_id=station_id,
                    target_date=d_str,
                    observed_high=temps["high_f"],
                    observed_low=temps["low_f"],
                    source="NWS_OBS_BACKFILL",
                )
                total += 1
            except Exception as ex:
                log.warning("[nws_obs] upsert failed %s/%s: %s",
                            station_id, d_str, ex)

        conn.commit()
        log.info("[nws_obs] %s %s–%s: %d dates loaded",
                 station_id, chunk_start, chunk_end, len(daily))
        time.sleep(NWS_OBS_REQUEST_SLEEP)

    return total


# ─────────────────────────────────────────────────────────────────────
# NWS CLI product archive (secondary — overwrites station-obs rows)
# ─────────────────────────────────────────────────────────────────────

def _get_cli_product_list(cli_site: str, limit: int = CLI_MAX_PRODUCTS_PER_STATION) -> list[dict]:
    """Return up to `limit` CLI product stubs for a site from api.weather.gov."""
    url = f"{NWS_PRODUCTS_URL}/{cli_site}"
    try:
        resp = requests.get(
            url,
            headers={**dict(HEADERS), "Accept": "application/ld+json"},
            timeout=20,
        )
        resp.raise_for_status()
        items = resp.json().get("@graph", [])
        return items[:limit]
    except Exception as ex:
        log.warning("[cli_archive] failed to list products for %s: %s", cli_site, ex)
        return []


def load_cli_archive_observations(
    conn: Any,
    station: dict,
    start_date: str,
    end_date: str,
) -> int:
    """Walk the NWS CLI product archive and upsert exact daily high/low values.

    Reuses _fetch_product() and _parse_cli_max_min() from collect_cli.py.
    Always overwrites NWS_OBS_BACKFILL rows — CLI text bulletins are the
    authoritative source, identical to what the live night pipeline writes.
    Source tag: 'CLI_BACKFILL'.

    Returns count of observations inserted/updated.
    """
    from kalshicast.collection.collectors.collect_cli import (
        _fetch_product, _parse_cli_max_min, _cli_matches_site, _parse_cli_report_date,
    )

    station_id = station["station_id"]
    cli_site = station.get("cli_site", station_id[1:])
    s = date.fromisoformat(start_date)
    e = date.fromisoformat(end_date)

    items = _get_cli_product_list(cli_site)
    if not items:
        return 0

    # Sort newest first (already the default, but make explicit)
    items = sorted(items,
                   key=lambda x: x.get("issuanceTime") or x.get("issueTime") or "",
                   reverse=True)

    total = 0
    for item in items:
        pid = item.get("id") or item.get("@id")
        if not isinstance(pid, str) or not pid.strip():
            continue

        try:
            text, issued_at = _fetch_product(pid.strip())
            time.sleep(NWS_REQUEST_SLEEP)
        except Exception as ex:
            log.debug("[cli_archive] fetch failed %s: %s", pid, ex)
            continue

        if not _cli_matches_site(text, cli_site):
            continue

        report_date_str = _parse_cli_report_date(text)
        if not report_date_str:
            continue

        try:
            rd = date.fromisoformat(report_date_str)
        except ValueError:
            continue

        if not (s <= rd <= e):
            # Products are newest-first — once rd < s we can stop
            if rd < s:
                break
            continue

        parsed = _parse_cli_max_min(text)
        if not parsed:
            continue

        high_f, low_f = parsed
        try:
            upsert_observation(
                conn,
                station_id=station_id,
                target_date=report_date_str,
                observed_high=high_f,
                observed_low=low_f,
                source="CLI_BACKFILL",
            )
            total += 1
        except Exception as ex:
            log.warning("[cli_archive] upsert failed %s/%s: %s",
                        station_id, report_date_str, ex)

    conn.commit()
    log.info("[cli_archive] %s: %d observations loaded", station_id, total)
    return total