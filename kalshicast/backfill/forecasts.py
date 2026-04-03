# kalshicast/backfill/forecasts.py
"""Open-Meteo Historical Forecast adapter.

Thin wrapper around the OME historical API. One API call per (model, station)
covers the full 2-year backfill window. We then explode the multi-day response
into per-issued_at FORECAST_RUNS rows, exactly as the live morning pipeline
would have produced them.

Reuses:
  bulk_upsert_forecasts_daily()  from db/operations.py
  bulk_upsert_forecasts_hourly() from db/operations.py
  get_or_create_forecast_run()   from db/operations.py
  classify_lead_hours()          from collection/lead_time.py
"""
from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from kalshicast.backfill.config import (
    OME_HISTORICAL_BASE_URL, OME_HISTORICAL_MODELS,
    BACKFILL_LEAD_OFFSETS_DAYS, OME_REQUEST_SLEEP,
)
from kalshicast.collection.lead_time import classify_lead_hours
from kalshicast.config import HEADERS
from kalshicast.db.operations import (
    bulk_upsert_forecasts_daily,
    bulk_upsert_forecasts_hourly,
    get_or_create_forecast_run,
)

log = logging.getLogger(__name__)

_HOURLY_VARS = [
    "temperature_2m", "dew_point_2m", "relative_humidity_2m",
    "wind_speed_10m", "wind_direction_10m", "cloud_cover",
    "precipitation_probability",
]


def _issued_at_for_offset(target_date: date, offset_days: int) -> str:
    """Compute a synthetic issued_at string for a given offset.

    issued_at = target_date - offset_days, at 12:00 UTC.
    """
    issued = target_date - timedelta(days=offset_days)
    return f"{issued.isoformat()}T12:00:00Z"


def _approx_lead_hours(offset_days: int, target_local_hour: int = 15) -> float:
    """Approximate lead hours for a given offset.

    Assumes forecast issued at 12:00 UTC and target_local_hour is when the
    daily high/low is expected (15=high, 7=low). Ignores timezone for the
    approximation — this is marked LEAD_HOURS_APPROX anyway.
    """
    issued_hour_utc = 12
    target_hour_utc = target_local_hour  # rough: assume station near UTC-5 → +5 hours
    return offset_days * 24 + (target_hour_utc - issued_hour_utc)


def _find_key(d: dict, base: str) -> str:
    """Find OME response key, allowing for model-suffixed variants."""
    if base in d:
        return base
    for k in d:
        if k.startswith(base + "_"):
            return k
    return base


def _parse_ome_historical_response(
    data: dict,
) -> tuple[list[dict], list[dict]]:
    """Parse OME historical API response into (daily_rows, hourly_rows).

    Mirrors the logic in collect_ome_model.py but accepts arbitrary data dict
    without rebuilding a time_axis.

    daily_rows: [{target_date, high_f, low_f}]
    hourly_rows: [{valid_time, temperature_f, dewpoint_f, ...}]
    """
    daily_out: list[dict] = []
    hourly_out: list[dict] = []

    # Daily
    daily = data.get("daily") or {}
    d_time = daily.get("time") or []
    d_hi_key = _find_key(daily, "temperature_2m_max")
    d_lo_key = _find_key(daily, "temperature_2m_min")
    d_hi = daily.get(d_hi_key) or []
    d_lo = daily.get(d_lo_key) or []

    for i in range(min(len(d_time), len(d_hi), len(d_lo))):
        try:
            hi = float(d_hi[i])
            lo = float(d_lo[i])
        except (TypeError, ValueError):
            continue  # skip rows with None or non-numeric values
        if not math.isfinite(hi) or not math.isfinite(lo):
            continue
        daily_out.append({
            "target_date": str(d_time[i])[:10],
            "high_f": hi,
            "low_f":  lo,
        })

    # Hourly
    hourly = data.get("hourly") or {}
    h_time = hourly.get("time") or []

    def _normalize_ts(ts: str) -> str:
        """Convert 'YYYY-MM-DDTHH:MM' → 'YYYY-MM-DDTHH:00:00Z'."""
        if not isinstance(ts, str) or len(ts) < 13:
            return ts
        return ts[:13] + ":00:00Z"

    temp_key = _find_key(hourly, "temperature_2m")
    dew_key  = _find_key(hourly, "dew_point_2m")
    rh_key   = _find_key(hourly, "relative_humidity_2m")
    ws_key   = _find_key(hourly, "wind_speed_10m")
    wd_key   = _find_key(hourly, "wind_direction_10m")
    cc_key   = _find_key(hourly, "cloud_cover")
    pp_key   = _find_key(hourly, "precipitation_probability")

    temps = hourly.get(temp_key) or []
    dews  = hourly.get(dew_key) or []
    rhs   = hourly.get(rh_key) or []
    wss   = hourly.get(ws_key) or []
    wds   = hourly.get(wd_key) or []
    ccs   = hourly.get(cc_key) or []
    pps   = hourly.get(pp_key) or []

    def _safe_float(arr, i):
        try:
            v = arr[i]
            return float(v) if v is not None else None
        except (IndexError, TypeError, ValueError):
            return None

    for i, ts in enumerate(h_time):
        vt = _normalize_ts(str(ts))
        hourly_out.append({
            "valid_time":      vt,
            "temperature_f":   _safe_float(temps, i),
            "dewpoint_f":      _safe_float(dews, i),
            "humidity_pct":    _safe_float(rhs, i),
            "wind_speed_mph":  _safe_float(wss, i),
            "wind_dir_deg":    _safe_float(wds, i),
            "cloud_cover_pct": _safe_float(ccs, i),
            "precip_prob_pct": _safe_float(pps, i),
        })

    return daily_out, hourly_out


def _fetch_ome_historical(
    lat: float,
    lon: float,
    models: str,
    start_date: str,
    end_date: str,
) -> dict:
    """Call Open-Meteo Historical Forecast API.

    Returns raw JSON response dict or raises on failure.
    """
    params: dict = {
        "latitude":          lat,
        "longitude":         lon,
        "models":            models,
        "start_date":        start_date,
        "end_date":          end_date,
        "forecast_days":     4,
        "temperature_unit":  "fahrenheit",
        "wind_speed_unit":   "mph",
        "timezone":          "UTC",
        "daily":             "temperature_2m_max,temperature_2m_min",
        "hourly":            ",".join(_HOURLY_VARS),
    }
    resp = requests.get(
        OME_HISTORICAL_BASE_URL,
        params=params,
        headers=dict(HEADERS),
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"OME historical error: {data.get('reason')}")
    return data


def load_ome_historical_forecasts(
    conn: Any,
    station: dict,
    source_id: str,
    ome_model: str,
    start_date: str,
    end_date: str,
    skip_dates: set[str] | None = None,
) -> int:
    """Fetch and store OME historical forecasts for one station + model.

    Makes one API call covering the full window. For each target_date in the
    response, and for each lead_offset in BACKFILL_LEAD_OFFSETS_DAYS, we create
    a synthetic FORECAST_RUNS row with issued_at = target_date - offset at noon UTC,
    then insert the daily row into FORECASTS_DAILY with LEAD_HOURS_APPROX = 1.

    skip_dates: set of "YYYY-MM-DD" strings already present in DB for this source
                (used for idempotency — skip dates we've already loaded).

    Returns total daily rows written.
    """
    lat = station.get("lat")
    lon = station.get("lon")
    station_id = station["station_id"]
    if lat is None or lon is None:
        log.warning("[ome_hist] %s/%s: no lat/lon", station_id, source_id)
        return 0

    log.info("[ome_hist] fetching %s/%s %s–%s", station_id, source_id, start_date, end_date)

    try:
        data = _fetch_ome_historical(lat, lon, ome_model, start_date, end_date)
        time.sleep(OME_REQUEST_SLEEP)
    except Exception as ex:
        log.warning("[ome_hist] %s/%s fetch failed: %s", station_id, source_id, ex)
        return 0

    daily_rows, hourly_rows = _parse_ome_historical_response(data)

    if not daily_rows:
        log.warning("[ome_hist] %s/%s: no daily rows parsed", station_id, source_id)
        return 0

    # Build a lookup of hourly data by date so we can attach to each offset
    hourly_by_date: dict[str, list[dict]] = {}
    for hr in hourly_rows:
        d = (hr.get("valid_time") or "")[:10]
        if d:
            hourly_by_date.setdefault(d, []).append(hr)

    total = 0

    for offset in BACKFILL_LEAD_OFFSETS_DAYS:
        for dr in daily_rows:
            td_str = dr["target_date"]
            if skip_dates and td_str in skip_dates:
                continue

            try:
                td = date.fromisoformat(td_str)
            except ValueError:
                continue

            issued_at_str = _issued_at_for_offset(td, offset)

            # Approximate lead hours (high=15:00 local, low=07:00 local)
            lead_high = _approx_lead_hours(offset, target_local_hour=15)
            lead_low  = _approx_lead_hours(offset, target_local_hour=7)

            # Get or create the FORECAST_RUNS row for this synthetic issued_at
            run_id = get_or_create_forecast_run(
                conn,
                source_id=source_id,
                issued_at=issued_at_str,
            )

            # Mark IS_BACKFILL on the run row
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE FORECAST_RUNS SET IS_BACKFILL = 1
                        WHERE RUN_ID = :rid
                    """, {"rid": run_id})
            except Exception:
                pass  # Column may not exist yet — migration guards this

            daily_batch = [{
                "run_id":             run_id,
                "source_id":          source_id,
                "station_id":         station_id,
                "target_date":        td_str,
                "high_f":             dr["high_f"],
                "low_f":              dr["low_f"],
                "lead_hours_high":    lead_high,
                "lead_hours_low":     lead_low,
                "lead_bracket_high":  classify_lead_hours(lead_high),
                "lead_bracket_low":   classify_lead_hours(lead_low),
            }]

            try:
                bulk_upsert_forecasts_daily(conn, daily_batch)
                total += 1
            except Exception as ex:
                log.warning("[ome_hist] daily upsert failed %s/%s/%s: %s",
                            station_id, source_id, td_str, ex)
                continue

            # Hourly — only for the first offset (same physical data, different issued_at)
            if offset == BACKFILL_LEAD_OFFSETS_DAYS[0]:
                h_rows = hourly_by_date.get(td_str, [])
                if h_rows:
                    hourly_batch = [{
                        "run_id":         run_id,
                        "source_id":      source_id,
                        "station_id":     station_id,
                        **hr,
                    } for hr in h_rows]
                    try:
                        bulk_upsert_forecasts_hourly(conn, hourly_batch)
                    except Exception as ex:
                        log.debug("[ome_hist] hourly upsert failed %s/%s: %s",
                                  station_id, source_id, ex)

        conn.commit()
        log.info("[ome_hist] %s/%s offset=%dd: %d daily rows",
                 station_id, source_id, offset, total)

    return total