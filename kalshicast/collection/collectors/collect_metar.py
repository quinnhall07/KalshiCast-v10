"""METAR intraday observation collector.

Spec §4.10: Fetch METAR reports from aviationweather.gov for temperature
observations at airport stations. Used for same-day truncation in L3.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

import requests

from kalshicast.config import HEADERS
from kalshicast.config.params_bootstrap import get_param_int

log = logging.getLogger(__name__)

METAR_URL = "https://aviationweather.gov/api/data/metar"


def _parse_temperature_f(metar_text: str) -> float | None:
    """Extract temperature in °F from raw METAR string.

    METAR temperatures are in format: TT/DD where TT is temp, DD is dewpoint in °C.
    Negative values prefixed with M (e.g., M02 = -2°C).
    """
    match = re.search(r'\b(M?\d{2})/(M?\d{2})\b', metar_text)
    if not match:
        return None

    temp_str = match.group(1)
    if temp_str.startswith("M"):
        temp_c = -int(temp_str[1:])
    else:
        temp_c = int(temp_str)

    return round(temp_c * 9.0 / 5.0 + 32.0, 1)


def _parse_dewpoint_f(metar_text: str) -> float | None:
    """Extract dewpoint in °F from raw METAR string."""
    match = re.search(r'\b(M?\d{2})/(M?\d{2})\b', metar_text)
    if not match:
        return None

    dp_str = match.group(2)
    if dp_str.startswith("M"):
        dp_c = -int(dp_str[1:])
    else:
        dp_c = int(dp_str)

    return round(dp_c * 9.0 / 5.0 + 32.0, 1)


def _parse_wind(metar_text: str) -> tuple[int | None, int | None]:
    """Extract wind speed (kt) and direction (deg) from METAR."""
    match = re.search(r'\b(\d{3}|VRB)(\d{2,3})(?:G\d{2,3})?KT\b', metar_text)
    if not match:
        return None, None

    dir_str = match.group(1)
    wind_dir = None if dir_str == "VRB" else int(dir_str)
    wind_speed = int(match.group(2))
    return wind_speed, wind_dir


def fetch_metar_observations(
    stations: list[dict],
    conn: Any,
    *,
    hours: int = 3,
) -> int:
    """Fetch recent METAR observations for all stations.

    Returns count of observations inserted.
    """
    count = 0

    for station in stations:
        station_id = station["station_id"]
        try:
            resp = requests.get(
                METAR_URL,
                params={"ids": station_id, "format": "raw", "hours": hours},
                headers=HEADERS,
                timeout=15,
            )
            resp.raise_for_status()
            raw_text = resp.text.strip()

            if not raw_text or "No data" in raw_text:
                continue

            # Parse each METAR report (one per line)
            for line in raw_text.split("\n"):
                line = line.strip()
                if not line or not line.startswith(station_id):
                    continue

                temp_f = _parse_temperature_f(line)
                if temp_f is None:
                    continue

                dp_f = _parse_dewpoint_f(line)
                wind_speed, wind_dir = _parse_wind(line)

                # Extract observation time from METAR (DDHHMMz)
                time_match = re.search(r'\b(\d{6})Z\b', line)
                obs_utc = datetime.now(timezone.utc)
                if time_match:
                    ddhhnn = time_match.group(1)
                    day = int(ddhhnn[:2])
                    hour = int(ddhhnn[2:4])
                    minute = int(ddhhnn[4:6])
                    obs_utc = obs_utc.replace(day=day, hour=hour, minute=minute, second=0, microsecond=0)

                # Insert METAR_OBSERVATIONS
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO METAR_OBSERVATIONS (
                            STATION_ID, OBSERVED_UTC, TEMPERATURE_F,
                            DEW_POINT_F, WIND_SPEED_KT, WIND_DIR_DEG, RAW_METAR
                        ) VALUES (
                            :sid, :obs_utc, :temp, :dp, :ws, :wd, :raw
                        )
                    """, {
                        "sid": station_id,
                        "obs_utc": obs_utc,
                        "temp": temp_f,
                        "dp": dp_f,
                        "ws": wind_speed,
                        "wd": wind_dir,
                        "raw": line[:500],
                    })
                    count += 1

                # Update METAR_DAILY_MAX
                local_date = obs_utc.strftime("%Y-%m-%d")
                with conn.cursor() as cur:
                    cur.execute("""
                        MERGE INTO METAR_DAILY_MAX tgt USING DUAL
                        ON (tgt.STATION_ID = :sid AND tgt.LOCAL_DATE = TO_DATE(:ld, 'YYYY-MM-DD'))
                        WHEN MATCHED THEN UPDATE SET
                            T_OBS_MAX_F = GREATEST(tgt.T_OBS_MAX_F, :temp),
                            T_OBS_MIN_F = LEAST(tgt.T_OBS_MIN_F, :temp),
                            OBS_COUNT = tgt.OBS_COUNT + 1,
                            LAST_OBS_AT = :obs_utc,
                            LAST_UPDATED_UTC = SYSTIMESTAMP
                        WHEN NOT MATCHED THEN INSERT (
                            STATION_ID, LOCAL_DATE, T_OBS_MAX_F, T_OBS_MIN_F,
                            OBS_COUNT, LAST_OBS_AT, LAST_UPDATED_UTC
                        ) VALUES (
                            :sid, TO_DATE(:ld, 'YYYY-MM-DD'), :temp, :temp,
                            1, :obs_utc, SYSTIMESTAMP
                        )
                    """, {"sid": station_id, "ld": local_date, "temp": temp_f, "obs_utc": obs_utc})

            conn.commit()

        except Exception as e:
            log.warning("METAR fetch failed for %s: %s", station_id, e)

    log.info("[metar] fetched %d observations across %d stations", count, len(stations))
    return count
