"""Database MERGE/INSERT operations — ported from db.py.

All functions take conn as first parameter.
Uses MERGE USING (UNION ALL) for batch upserts.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from kalshicast.db.connection import GUID_EXPR, to_dt

log = logging.getLogger(__name__)


def new_run_id() -> str:
    """Generate a UUID for pipeline/forecast run IDs."""
    return str(uuid.uuid4())


# ─────────────────────────────────────────────────────────────────────
# Stations
# ─────────────────────────────────────────────────────────────────────

def upsert_station(conn: Any, station: dict) -> None:
    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO STATIONS tgt
    USING DUAL
    ON (tgt.STATION_ID = :station_id)
    WHEN MATCHED THEN UPDATE SET
      CLI_SITE     = COALESCE(:cli_site,     tgt.CLI_SITE),
      NAME         = COALESCE(:name,         tgt.NAME),
      STATE_CODE   = COALESCE(:state,        tgt.STATE_CODE),
      CITY         = COALESCE(:city,         tgt.CITY),
      TIMEZONE     = COALESCE(:timezone,     tgt.TIMEZONE),
      LAT          = COALESCE(:lat,          tgt.LAT),
      LON          = COALESCE(:lon,          tgt.LON),
      ELEVATION_FT = COALESCE(:elevation_ft, tgt.ELEVATION_FT),
      WFO_ID       = COALESCE(:wfo_id,      tgt.WFO_ID),
      IS_ACTIVE    = COALESCE(:is_active,    tgt.IS_ACTIVE)
    WHEN NOT MATCHED THEN INSERT (
      STATION_ID, CLI_SITE, NAME, STATE_CODE, CITY,
      TIMEZONE, LAT, LON, ELEVATION_FT, WFO_ID, IS_ACTIVE
    ) VALUES (
      :station_id, :cli_site, :name, :state, :city,
      :timezone, :lat, :lon, :elevation_ft, :wfo_id, :is_active
    )
    """
    bind = {
        "station_id":   station["station_id"],
        "cli_site":     station.get("cli_site"),
        "name":         station.get("name"),
        "state":        station.get("state"),
        "city":         station.get("city"),
        "timezone":     station.get("timezone"),
        "lat":          station.get("lat"),
        "lon":          station.get("lon"),
        "elevation_ft": station.get("elevation_ft"),
        "wfo_id":       station.get("wfo_id"),
        "is_active":    1 if station.get("is_active", True) else 0,
    }
    with conn.cursor() as cur:
        cur.execute(sql, bind)


# ─────────────────────────────────────────────────────────────────────
# Forecast Runs
# ─────────────────────────────────────────────────────────────────────

def get_or_create_forecast_run(conn: Any, *, source_id: str, issued_at: str) -> str:
    """Upsert into FORECAST_RUNS and return the run_id."""
    merge_sql = f"""
    MERGE /*+ NO_PARALLEL(tgt) */ INTO FORECAST_RUNS tgt
    USING DUAL
    ON (tgt.SOURCE_ID = :source_id AND tgt.ISSUED_AT = :issued_at)
    WHEN NOT MATCHED THEN INSERT (RUN_ID, SOURCE_ID, ISSUED_AT)
    VALUES ({GUID_EXPR}, :source_id, :issued_at)
    """
    bind = {"source_id": source_id, "issued_at": to_dt(issued_at)}

    with conn.cursor() as cur:
        cur.execute(merge_sql, bind)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT RUN_ID FROM FORECAST_RUNS WHERE SOURCE_ID = :source_id AND ISSUED_AT = :issued_at",
            bind,
        )
        return cur.fetchone()[0]


# ─────────────────────────────────────────────────────────────────────
# Daily Forecasts — batch MERGE
# ─────────────────────────────────────────────────────────────────────

def bulk_upsert_forecasts_daily(conn: Any, rows: list[dict]) -> int:
    """Upsert into FORECASTS_DAILY using MERGE USING (UNION ALL).

    Batched at 500 rows per statement.
    """
    if not rows:
        return 0

    prepared = []
    for r in rows:
        lead_hi = r.get("lead_hours_high", r.get("lead_high_hours"))
        lead_lo = r.get("lead_hours_low", r.get("lead_low_hours"))
        prepared.append({
            "run_id":          r["run_id"],
            "source_id":       r.get("source_id", ""),
            "station_id":      r["station_id"],
            "target_date":     str(r["target_date"]),
            "high_f":          r.get("high_f"),
            "low_f":           r.get("low_f"),
            "lead_hours_high": lead_hi,
            "lead_hours_low":  lead_lo,
            "lead_bracket_high": r.get("lead_bracket_high"),
            "lead_bracket_low":  r.get("lead_bracket_low"),
        })

    BATCH = 500
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(prepared), BATCH):
            chunk = prepared[i:i + BATCH]
            union_parts = []
            bind: dict[str, Any] = {}
            for j, r in enumerate(chunk):
                union_parts.append(
                    f"SELECT :run_id_{j} AS run_id, :source_id_{j} AS source_id, "
                    f":station_id_{j} AS station_id, "
                    f"TO_DATE(:target_date_{j}, 'YYYY-MM-DD') AS target_date, "
                    f"CAST(:high_f_{j} AS NUMBER(5,1)) AS high_f, "
                    f"CAST(:low_f_{j} AS NUMBER(5,1)) AS low_f, "
                    f"CAST(:lead_hours_high_{j} AS NUMBER(6,1)) AS lead_hours_high, "
                    f"CAST(:lead_hours_low_{j} AS NUMBER(6,1)) AS lead_hours_low, "
                    f":lead_bracket_high_{j} AS lead_bracket_high, "
                    f":lead_bracket_low_{j} AS lead_bracket_low FROM DUAL"
                )
                bind[f"run_id_{j}"]          = r["run_id"]
                bind[f"source_id_{j}"]       = r["source_id"]
                bind[f"station_id_{j}"]      = r["station_id"]
                bind[f"target_date_{j}"]     = r["target_date"]
                bind[f"high_f_{j}"]          = r["high_f"]
                bind[f"low_f_{j}"]           = r["low_f"]
                bind[f"lead_hours_high_{j}"] = r["lead_hours_high"]
                bind[f"lead_hours_low_{j}"]  = r["lead_hours_low"]
                bind[f"lead_bracket_high_{j}"] = r["lead_bracket_high"]
                bind[f"lead_bracket_low_{j}"]  = r["lead_bracket_low"]

            sql = f"""
            MERGE /*+ NO_PARALLEL(tgt) */ INTO FORECASTS_DAILY tgt
            USING (
                {" UNION ALL ".join(union_parts)}
            ) src
            ON (
                tgt.RUN_ID      = src.run_id
                AND tgt.STATION_ID  = src.station_id
                AND tgt.TARGET_DATE = src.target_date
            )
            WHEN MATCHED THEN UPDATE SET
                tgt.HIGH_F          = src.high_f,
                tgt.LOW_F           = src.low_f,
                tgt.LEAD_HOURS_HIGH = src.lead_hours_high,
                tgt.LEAD_HOURS_LOW  = src.lead_hours_low,
                tgt.LEAD_BRACKET_HIGH = src.lead_bracket_high,
                tgt.LEAD_BRACKET_LOW  = src.lead_bracket_low
            WHEN NOT MATCHED THEN INSERT (
                RUN_ID, SOURCE_ID, STATION_ID, TARGET_DATE,
                HIGH_F, LOW_F, LEAD_HOURS_HIGH, LEAD_HOURS_LOW,
                LEAD_BRACKET_HIGH, LEAD_BRACKET_LOW, CREATED_AT
            ) VALUES (
                src.run_id, src.source_id, src.station_id, src.target_date,
                src.high_f, src.low_f, src.lead_hours_high, src.lead_hours_low,
                src.lead_bracket_high, src.lead_bracket_low, SYSTIMESTAMP
            )
            """
            cur.execute(sql, bind)
            total += len(chunk)

    return total


# ─────────────────────────────────────────────────────────────────────
# Hourly Forecasts — batch MERGE
# ─────────────────────────────────────────────────────────────────────

def bulk_upsert_forecasts_hourly(conn: Any, rows: list[dict]) -> int:
    """Upsert into FORECASTS_HOURLY using MERGE USING (UNION ALL).

    Batched at 200 rows per statement.
    """
    if not rows:
        return 0

    prepared = []
    for r in rows:
        prepared.append({
            "run_id":          r["run_id"],
            "source_id":       r.get("source_id", ""),
            "station_id":      r["station_id"],
            "valid_time":      to_dt(str(r["valid_time"])),
            "temperature_f":   r.get("temperature_f"),
            "dewpoint_f":      r.get("dewpoint_f"),
            "humidity_pct":    r.get("humidity_pct"),
            "wind_speed_mph":  r.get("wind_speed_mph"),
            "wind_dir_deg":    r.get("wind_dir_deg"),
            "cloud_cover_pct": r.get("cloud_cover_pct"),
            "precip_prob_pct": r.get("precip_prob_pct"),
        })

    BATCH = 200
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(prepared), BATCH):
            chunk = prepared[i:i + BATCH]
            union_parts = []
            bind: dict[str, Any] = {}
            for j, r in enumerate(chunk):
                union_parts.append(
                    f"SELECT :run_id_{j} AS run_id, :source_id_{j} AS source_id, "
                    f":station_id_{j} AS station_id, "
                    f":valid_time_{j} AS valid_time_utc, "
                    f"CAST(:temperature_f_{j} AS NUMBER(5,1)) AS temperature_f, "
                    f"CAST(:dewpoint_f_{j} AS NUMBER(5,1)) AS dewpoint_f, "
                    f"CAST(:humidity_pct_{j} AS NUMBER(5,2)) AS humidity_pct, "
                    f"CAST(:wind_speed_mph_{j} AS NUMBER(5,1)) AS wind_speed_mph, "
                    f"CAST(:wind_dir_deg_{j} AS NUMBER(3,0)) AS wind_dir_deg, "
                    f"CAST(:cloud_cover_pct_{j} AS NUMBER(3,0)) AS cloud_cover_pct, "
                    f"CAST(:precip_prob_pct_{j} AS NUMBER(3,0)) AS precip_prob_pct FROM DUAL"
                )
                bind[f"run_id_{j}"]          = r["run_id"]
                bind[f"source_id_{j}"]       = r["source_id"]
                bind[f"station_id_{j}"]      = r["station_id"]
                bind[f"valid_time_{j}"]      = r["valid_time"]
                bind[f"temperature_f_{j}"]   = r["temperature_f"]
                bind[f"dewpoint_f_{j}"]      = r["dewpoint_f"]
                bind[f"humidity_pct_{j}"]    = r["humidity_pct"]
                bind[f"wind_speed_mph_{j}"]  = r["wind_speed_mph"]
                bind[f"wind_dir_deg_{j}"]    = r["wind_dir_deg"]
                bind[f"cloud_cover_pct_{j}"] = r["cloud_cover_pct"]
                bind[f"precip_prob_pct_{j}"] = r["precip_prob_pct"]

            sql = f"""
            MERGE /*+ NO_PARALLEL(tgt) */ INTO FORECASTS_HOURLY tgt
            USING (
                {" UNION ALL ".join(union_parts)}
            ) src
            ON (
                tgt.RUN_ID         = src.run_id
                AND tgt.STATION_ID     = src.station_id
                AND tgt.VALID_TIME_UTC = src.valid_time_utc
            )
            WHEN MATCHED THEN UPDATE SET
                tgt.TEMPERATURE_F   = src.temperature_f,
                tgt.DEWPOINT_F      = src.dewpoint_f,
                tgt.HUMIDITY_PCT    = src.humidity_pct,
                tgt.WIND_SPEED_MPH  = src.wind_speed_mph,
                tgt.WIND_DIR_DEG    = src.wind_dir_deg,
                tgt.CLOUD_COVER_PCT = src.cloud_cover_pct,
                tgt.PRECIP_PROB_PCT = src.precip_prob_pct
            WHEN NOT MATCHED THEN INSERT (
                RUN_ID, SOURCE_ID, STATION_ID, VALID_TIME_UTC,
                TEMPERATURE_F, DEWPOINT_F, HUMIDITY_PCT,
                WIND_SPEED_MPH, WIND_DIR_DEG, CLOUD_COVER_PCT, PRECIP_PROB_PCT,
                CREATED_AT
            ) VALUES (
                src.run_id, src.source_id, src.station_id, src.valid_time_utc,
                src.temperature_f, src.dewpoint_f, src.humidity_pct,
                src.wind_speed_mph, src.wind_dir_deg, src.cloud_cover_pct, src.precip_prob_pct,
                SYSTIMESTAMP
            )
            """
            cur.execute(sql, bind)
            total += len(chunk)

    return total


# ─────────────────────────────────────────────────────────────────────
# Pipeline Runs (heartbeat)
# ─────────────────────────────────────────────────────────────────────

def insert_pipeline_run(conn: Any, run_id: str, run_type: str) -> None:
    """Insert a new pipeline run record (status=RUNNING)."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO PIPELINE_RUNS (RUN_ID, RUN_TYPE, STARTED_UTC, STATUS)
            VALUES (:run_id, :run_type, SYSTIMESTAMP, 'RUNNING')
        """, {"run_id": run_id, "run_type": run_type})


def update_pipeline_run(
    conn: Any,
    run_id: str,
    *,
    status: str,
    stations_ok: int = 0,
    stations_fail: int = 0,
    rows_daily: int = 0,
    rows_hourly: int = 0,
    error_msg: str | None = None,
) -> None:
    """Update a pipeline run record on completion."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE PIPELINE_RUNS SET
                COMPLETED_UTC = SYSTIMESTAMP,
                STATUS        = :status,
                STATIONS_OK   = :sok,
                STATIONS_FAIL = :sfail,
                ROWS_DAILY    = :rd,
                ROWS_HOURLY   = :rh,
                ERROR_MSG     = :err
            WHERE RUN_ID = :run_id
        """, {
            "run_id": run_id, "status": status,
            "sok": stations_ok, "sfail": stations_fail,
            "rd": rows_daily, "rh": rows_hourly,
            "err": error_msg,
        })


# ─────────────────────────────────────────────────────────────────────
# Observation Runs / Observations
# ─────────────────────────────────────────────────────────────────────

def get_or_create_observation_run(conn: Any, *, run_issued_at: str) -> str:
    """Upsert into OBSERVATION_RUNS and return run_id."""
    merge_sql = f"""
    MERGE /*+ NO_PARALLEL(tgt) */ INTO OBSERVATION_RUNS tgt
    USING DUAL
    ON (tgt.RUN_ISSUED_AT = :run_issued_at)
    WHEN NOT MATCHED THEN INSERT (RUN_ID, RUN_ISSUED_AT)
    VALUES ({GUID_EXPR}, :run_issued_at)
    """
    with conn.cursor() as cur:
        cur.execute(merge_sql, {"run_issued_at": to_dt(run_issued_at)})

    with conn.cursor() as cur:
        cur.execute(
            "SELECT RUN_ID FROM OBSERVATION_RUNS WHERE RUN_ISSUED_AT = :run_issued_at",
            {"run_issued_at": to_dt(run_issued_at)},
        )
        return cur.fetchone()[0]


def upsert_observation(
    conn: Any,
    *,
    station_id: str,
    target_date: str,
    observed_high: float | None,
    observed_low: float | None,
    source: str = "CLI",
    flagged_raw_text: str | None = None,
    flagged_reason: str | None = None,
) -> None:
    """Upsert into OBSERVATIONS (v10 schema — keyed by station_id + target_date)."""
    if flagged_raw_text and not str(flagged_raw_text).strip():
        flagged_raw_text = None
    if flagged_reason and not str(flagged_reason).strip():
        flagged_reason = None

    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO OBSERVATIONS tgt
    USING DUAL
    ON (
      tgt.STATION_ID  = :station_id
      AND tgt.TARGET_DATE = TO_DATE(:obs_date, 'YYYY-MM-DD')
    )
    WHEN MATCHED THEN UPDATE SET
      OBSERVED_HIGH_F  = :observed_high,
      OBSERVED_LOW_F   = :observed_low,
      SOURCE           = :source,
      FLAGGED_RAW_TEXT = COALESCE(:flagged_raw_text, tgt.FLAGGED_RAW_TEXT),
      FLAGGED_REASON   = COALESCE(:flagged_reason,   tgt.FLAGGED_REASON)
    WHEN NOT MATCHED THEN INSERT (
      STATION_ID, TARGET_DATE,
      OBSERVED_HIGH_F, OBSERVED_LOW_F, SOURCE,
      FLAGGED_RAW_TEXT, FLAGGED_REASON
    ) VALUES (
      :station_id, TO_DATE(:obs_date, 'YYYY-MM-DD'),
      :observed_high, :observed_low, :source,
      :flagged_raw_text, :flagged_reason
    )
    """
    bind = {
        "station_id":       station_id,
        "obs_date":         str(target_date),
        "observed_high":    observed_high,
        "observed_low":     observed_low,
        "source":           source,
        "flagged_raw_text": flagged_raw_text,
        "flagged_reason":   flagged_reason,
    }
    with conn.cursor() as cur:
        cur.execute(sql, bind)


# ─────────────────────────────────────────────────────────────────────
# Params read
# ─────────────────────────────────────────────────────────────────────

def load_all_params(conn: Any) -> dict[str, str]:
    """Read all PARAMS rows and return {key: value} dict."""
    with conn.cursor() as cur:
        cur.execute("SELECT PARAM_KEY, PARAM_VALUE FROM PARAMS")
        return {row[0]: row[1] for row in cur if row[1] is not None}
