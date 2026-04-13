"""Database MERGE/INSERT operations — ported from db.py.

All functions take conn as first parameter.
Uses MERGE USING (UNION ALL) for batch upserts.
"""

from __future__ import annotations

import json
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
      FLAGGED_RAW_TEXT = COALESCE(TO_CLOB(:flagged_raw_text), tgt.FLAGGED_RAW_TEXT),
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


# ─────────────────────────────────────────────────────────────────────
# Forecast Errors — MERGE from FORECASTS_DAILY + OBSERVATIONS
# ─────────────────────────────────────────────────────────────────────

def build_forecast_errors_for_date(conn: Any, target_date: str) -> int:
    """Populate FORECAST_ERRORS by joining forecasts with observations.

    Unpivots HIGH/LOW into separate TARGET_TYPE rows.
    Error = Forecast - Observed (positive = model too warm).
    """
    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO FORECAST_ERRORS tgt
    USING (
      WITH latest_fc AS (
        SELECT d.RUN_ID, d.SOURCE_ID, d.STATION_ID, d.TARGET_DATE,
               d.HIGH_F, d.LOW_F,
               d.LEAD_HOURS_HIGH, d.LEAD_HOURS_LOW,
               d.LEAD_BRACKET_HIGH, d.LEAD_BRACKET_LOW,
               ROW_NUMBER() OVER (
                 PARTITION BY d.STATION_ID, d.SOURCE_ID, d.TARGET_DATE
                 ORDER BY fr.ISSUED_AT DESC
               ) AS rn
        FROM FORECASTS_DAILY d
        JOIN FORECAST_RUNS fr ON fr.RUN_ID = d.RUN_ID
        WHERE d.TARGET_DATE = TO_DATE(:target_date, 'YYYY-MM-DD')
      ),
      fc AS (SELECT * FROM latest_fc WHERE rn = 1),
      obs AS (
        SELECT STATION_ID, TARGET_DATE, OBSERVED_HIGH_F, OBSERVED_LOW_F
        FROM OBSERVATIONS
        WHERE TARGET_DATE = TO_DATE(:target_date, 'YYYY-MM-DD')
      ),
      high_rows AS (
        SELECT fc.STATION_ID, fc.SOURCE_ID, fc.TARGET_DATE,
               'HIGH' AS TARGET_TYPE,
               fc.LEAD_BRACKET_HIGH AS LEAD_BRACKET,
               fc.LEAD_HOURS_HIGH AS LEAD_HOURS,
               fc.RUN_ID,
               fc.HIGH_F AS F_RAW,
               obs.OBSERVED_HIGH_F AS OBSERVED,
               fc.HIGH_F - obs.OBSERVED_HIGH_F AS ERROR_RAW
        FROM fc
        JOIN obs ON obs.STATION_ID = fc.STATION_ID
                AND obs.TARGET_DATE = fc.TARGET_DATE
        WHERE fc.HIGH_F IS NOT NULL AND obs.OBSERVED_HIGH_F IS NOT NULL
      ),
      low_rows AS (
        SELECT fc.STATION_ID, fc.SOURCE_ID, fc.TARGET_DATE,
               'LOW' AS TARGET_TYPE,
               fc.LEAD_BRACKET_LOW AS LEAD_BRACKET,
               fc.LEAD_HOURS_LOW AS LEAD_HOURS,
               fc.RUN_ID,
               fc.LOW_F AS F_RAW,
               obs.OBSERVED_LOW_F AS OBSERVED,
               fc.LOW_F - obs.OBSERVED_LOW_F AS ERROR_RAW
        FROM fc
        JOIN obs ON obs.STATION_ID = fc.STATION_ID
                AND obs.TARGET_DATE = fc.TARGET_DATE
        WHERE fc.LOW_F IS NOT NULL AND obs.OBSERVED_LOW_F IS NOT NULL
      ),
      all_rows AS (
        SELECT * FROM high_rows
        UNION ALL
        SELECT * FROM low_rows
      )
      SELECT STATION_ID, SOURCE_ID, TARGET_DATE, TARGET_TYPE,
             LEAD_BRACKET, LEAD_HOURS, RUN_ID, F_RAW, OBSERVED, ERROR_RAW
      FROM all_rows
    ) src
    ON (
      tgt.STATION_ID  = src.STATION_ID
      AND tgt.SOURCE_ID   = src.SOURCE_ID
      AND tgt.TARGET_DATE  = src.TARGET_DATE
      AND tgt.TARGET_TYPE  = src.TARGET_TYPE
      AND tgt.LEAD_BRACKET = src.LEAD_BRACKET
    )
    WHEN MATCHED THEN UPDATE SET
      tgt.LEAD_HOURS  = src.LEAD_HOURS,
      tgt.RUN_ID      = src.RUN_ID,
      tgt.F_RAW       = src.F_RAW,
      tgt.OBSERVED    = src.OBSERVED,
      tgt.ERROR_RAW   = src.ERROR_RAW
    WHEN NOT MATCHED THEN INSERT (
      STATION_ID, SOURCE_ID, TARGET_DATE, TARGET_TYPE,
      LEAD_BRACKET, LEAD_HOURS, RUN_ID, F_RAW, OBSERVED, ERROR_RAW
    ) VALUES (
      src.STATION_ID, src.SOURCE_ID, src.TARGET_DATE, src.TARGET_TYPE,
      src.LEAD_BRACKET, src.LEAD_HOURS, src.RUN_ID, src.F_RAW, src.OBSERVED, src.ERROR_RAW
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"target_date": str(target_date)})
        return cur.rowcount or 0


# ─────────────────────────────────────────────────────────────────────
# Dashboard Stats — rolling distributions
# ─────────────────────────────────────────────────────────────────────

def update_dashboard_stats(conn: Any, window_days: int) -> None:
    """Recompute DASHBOARD_STATS from FORECAST_ERRORS for a given window."""
    if window_days <= 0:
        window_days = 1

    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO DASHBOARD_STATS tgt
    USING (
      WITH errs AS (
        SELECT STATION_ID, SOURCE_ID, TARGET_TYPE, LEAD_BRACKET,
               ERROR_RAW AS err
        FROM FORECAST_ERRORS
        WHERE TARGET_DATE >= TRUNC(SYSDATE) - :window_days
          AND ERROR_RAW IS NOT NULL
      ),
      agg AS (
        SELECT STATION_ID, SOURCE_ID, TARGET_TYPE, LEAD_BRACKET,
          COUNT(*)                                                   AS n,
          AVG(err)                                                  AS bias,
          AVG(ABS(err))                                             AS mae,
          SQRT(AVG(err * err))                                      AS rmse_raw,
          PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY err)        AS p10,
          PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY err)        AS p25,
          PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY err)        AS p50,
          PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY err)        AS p75,
          PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY err)        AS p90
        FROM errs
        WHERE LEAD_BRACKET IS NOT NULL
        GROUP BY STATION_ID, SOURCE_ID, TARGET_TYPE, LEAD_BRACKET
      )
      SELECT STATION_ID, SOURCE_ID, TARGET_TYPE, LEAD_BRACKET,
             :window_days AS WINDOW_DAYS,
             n, bias, mae, rmse_raw, p10, p25, p50, p75, p90,
             SYSTIMESTAMP AS COMPUTED_AT
      FROM agg
    ) src
    ON (
      tgt.STATION_ID   = src.STATION_ID
      AND tgt.SOURCE_ID    = src.SOURCE_ID
      AND tgt.TARGET_TYPE  = src.TARGET_TYPE
      AND tgt.LEAD_BRACKET = src.LEAD_BRACKET
      AND tgt.WINDOW_DAYS  = src.WINDOW_DAYS
    )
    WHEN MATCHED THEN UPDATE SET
      tgt.N          = src.n,
      tgt.BIAS       = src.bias,
      tgt.MAE        = src.mae,
      tgt.RMSE_RAW   = src.rmse_raw,
      tgt.P10        = src.p10,
      tgt.P25        = src.p25,
      tgt.P50        = src.p50,
      tgt.P75        = src.p75,
      tgt.P90        = src.p90,
      tgt.COMPUTED_AT = src.COMPUTED_AT
    WHEN NOT MATCHED THEN INSERT (
      STATION_ID, SOURCE_ID, TARGET_TYPE, LEAD_BRACKET, WINDOW_DAYS,
      N, BIAS, MAE, RMSE_RAW, P10, P25, P50, P75, P90, COMPUTED_AT
    ) VALUES (
      src.STATION_ID, src.SOURCE_ID, src.TARGET_TYPE, src.LEAD_BRACKET, src.WINDOW_DAYS,
      src.n, src.bias, src.mae, src.rmse_raw, src.p10, src.p25, src.p50, src.p75, src.p90,
      src.COMPUTED_AT
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"window_days": window_days})


# ─────────────────────────────────────────────────────────────────────
# Kalman State — read / write
# ─────────────────────────────────────────────────────────────────────

def get_kalman_state(conn: Any, station_id: str, target_type: str) -> dict | None:
    """Read current Kalman state for (station, type). Returns None if not initialized."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT B_K, U_K, Q_BASE, STATE_VERSION, TOP_MODEL_ID, LAST_OBSERVATION_DATE, LAST_UPDATED_UTC
            FROM KALMAN_STATES
            WHERE STATION_ID = :sid AND TARGET_TYPE = :tt
        """, {"sid": station_id, "tt": target_type})
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "b_k": float(row[0]) if row[0] is not None else 0.0,
        "u_k": float(row[1]) if row[1] is not None else 4.0,
        "q_base": float(row[2]) if row[2] is not None else 0.0,
        "state_version": int(row[3]) if row[3] is not None else 0,
        "top_model_id": row[4],
        "last_observation_date": row[5],
        "last_updated_utc": row[6],
    }


def upsert_kalman_state(conn: Any, station_id: str, target_type: str, state: dict) -> None:
    """MERGE into KALMAN_STATES."""
    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO KALMAN_STATES tgt
    USING DUAL
    ON (tgt.STATION_ID = :sid AND tgt.TARGET_TYPE = :tt)
    WHEN MATCHED THEN UPDATE SET
      B_K                  = :b_k,
      U_K                  = :u_k,
      Q_BASE               = :q_base,
      STATE_VERSION        = :sv,
      TOP_MODEL_ID         = :tmid,
      LAST_OBSERVATION_DATE = :lod,
      LAST_UPDATED_UTC     = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
      STATION_ID, TARGET_TYPE, B_K, U_K, Q_BASE,
      STATE_VERSION, TOP_MODEL_ID, LAST_OBSERVATION_DATE, LAST_UPDATED_UTC
    ) VALUES (
      :sid, :tt, :b_k, :u_k, :q_base,
      :sv, :tmid, :lod, SYSTIMESTAMP
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "sid": station_id, "tt": target_type,
            "b_k": state["b_k"], "u_k": state["u_k"],
            "q_base": state.get("q_base", 0.0),
            "sv": state["state_version"],
            "tmid": state.get("top_model_id"),
            "lod": state.get("last_observation_date"),
        })


def insert_kalman_history(conn: Any, row: dict) -> None:
    """Append to KALMAN_HISTORY."""
    sql = """
    INSERT INTO KALMAN_HISTORY (
      STATION_ID, TARGET_TYPE, PIPELINE_RUN_ID,
      B_K, U_K, Q_K, R_K, K_K, EPSILON_K,
      STATE_VERSION, IS_AMENDMENT
    ) VALUES (
      :sid, :tt, :run_id,
      :b_k, :u_k, :q_k, :r_k, :k_k, :epsilon_k,
      :sv, :is_amend
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "sid": row["station_id"], "tt": row["target_type"],
            "run_id": row.get("pipeline_run_id"),
            "b_k": row["b_k"], "u_k": row["u_k"],
            "q_k": row.get("q_k"), "r_k": row.get("r_k"),
            "k_k": row.get("k_k"), "epsilon_k": row.get("epsilon_k"),
            "sv": row.get("state_version", 0),
            "is_amend": 1 if row.get("is_amendment") else 0,
        })



# ─────────────────────────────────────────────────────────────────────
# Ensemble State / Model Weights
# ─────────────────────────────────────────────────────────────────────

def upsert_ensemble_state(conn: Any, rows: list[dict]) -> int:
    """MERGE into ENSEMBLE_STATE (batched)."""
    if not rows:
        return 0
    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO ENSEMBLE_STATE tgt
    USING DUAL
    ON (tgt.RUN_ID = :run_id AND tgt.STATION_ID = :sid
        AND tgt.TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD') AND tgt.TARGET_TYPE = :tt)
    WHEN MATCHED THEN UPDATE SET
      F_TK_TOP      = :f_tk_top,
      TOP_MODEL_ID  = :tmid,
      F_BAR_TK      = :f_bar,
      S_TK          = :s_tk,
      S_WEIGHTED_TK = :s_w,
      SIGMA_EFF     = :sigma_eff,
      M_K           = :m_k,
      WEIGHT_JSON   = :wj,
      STALE_MODEL_IDS = :stale
    WHEN NOT MATCHED THEN INSERT (
      RUN_ID, STATION_ID, TARGET_DATE, TARGET_TYPE,
      F_TK_TOP, TOP_MODEL_ID, F_BAR_TK, S_TK, S_WEIGHTED_TK,
      SIGMA_EFF, M_K, WEIGHT_JSON, STALE_MODEL_IDS
    ) VALUES (
      :run_id, :sid, TO_DATE(:td, 'YYYY-MM-DD'), :tt,
      :f_tk_top, :tmid, :f_bar, :s_tk, :s_w,
      :sigma_eff, :m_k, :wj, :stale
    )
    """
    bind_rows = []
    for r in rows:
        wj = json.dumps(r["weight_json"]) if isinstance(r.get("weight_json"), (dict, list)) else r.get("weight_json")
        bind_rows.append({
            "run_id": r["run_id"], "sid": r["station_id"],
            "td": str(r["target_date"]), "tt": r["target_type"],
            "f_tk_top": r.get("f_tk_top"), "tmid": r.get("top_model_id"),
            "f_bar": r.get("f_bar_tk"), "s_tk": r.get("s_tk"),
            "s_w": r.get("s_weighted_tk"), "sigma_eff": r.get("sigma_eff"),
            "m_k": r.get("m_k"), "wj": wj,
            "stale": r.get("stale_model_ids"),
        })
    with conn.cursor() as cur:
        cur.executemany(sql, bind_rows)
    return len(bind_rows)


def upsert_model_weights(conn: Any, rows: list[dict]) -> int:
    """Batch INSERT into MODEL_WEIGHTS (append per run)."""
    if not rows:
        return 0
    sql = """
    INSERT INTO MODEL_WEIGHTS (
      RUN_ID, STATION_ID, SOURCE_ID, LEAD_BRACKET,
      W_M, BSS_M, IS_STALE, STALE_DECAY_FACTOR
    ) VALUES (
      :run_id, :sid, :src_id, :lb,
      :w_m, :bss_m, :is_stale, :decay
    )
    """
    bind_rows = [{
        "run_id": r["run_id"], "sid": r["station_id"],
        "src_id": r["source_id"], "lb": r.get("lead_bracket"),
        "w_m": r.get("w_m"), "bss_m": r.get("bss_m"),
        "is_stale": 1 if r.get("is_stale") else 0,
        "decay": r.get("stale_decay_factor"),
    } for r in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, bind_rows)
    return len(bind_rows)


# ─────────────────────────────────────────────────────────────────────
# Forecast data reads for L2 processing
# ─────────────────────────────────────────────────────────────────────

def get_latest_forecasts_for_date(conn: Any, target_date: str) -> list[dict]:
    """Get latest forecast per (station, source, target_date) with lead info."""
    sql = """
    SELECT d.STATION_ID, d.SOURCE_ID, d.TARGET_DATE,
           d.HIGH_F, d.LOW_F,
           d.LEAD_BRACKET_HIGH, d.LEAD_BRACKET_LOW,
           d.LEAD_HOURS_HIGH, d.LEAD_HOURS_LOW,
           fr.ISSUED_AT
    FROM (
      SELECT d2.*, ROW_NUMBER() OVER (
        PARTITION BY d2.STATION_ID, d2.SOURCE_ID, d2.TARGET_DATE
        ORDER BY fr2.ISSUED_AT DESC
      ) AS rn
      FROM FORECASTS_DAILY d2
      JOIN FORECAST_RUNS fr2 ON fr2.RUN_ID = d2.RUN_ID
      WHERE d2.TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD')
    ) d
    JOIN FORECAST_RUNS fr ON fr.RUN_ID = d.RUN_ID
    WHERE d.rn = 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"td": str(target_date)})
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, row)) for row in cur]


def get_forecast_errors_window(conn: Any, station_id: str, source_id: str | None,
                                target_type: str, lead_bracket: str,
                                window_days: int) -> list[dict]:
    """Get forecast errors for sigma/skewness computation over a trailing window."""
    if source_id:
        sql = """
        SELECT ERROR_RAW, ERROR_ADJUSTED, F_RAW, F_ADJUSTED, OBSERVED, TARGET_DATE
        FROM FORECAST_ERRORS
        WHERE STATION_ID = :sid AND SOURCE_ID = :src
          AND TARGET_TYPE = :tt AND LEAD_BRACKET = :lb
          AND TARGET_DATE >= TRUNC(SYSDATE) - :window
        ORDER BY TARGET_DATE
        """
        bind = {"sid": station_id, "src": source_id, "tt": target_type,
                "lb": lead_bracket, "window": window_days}
    else:
        sql = """
        SELECT ERROR_RAW, ERROR_ADJUSTED, F_RAW, F_ADJUSTED, OBSERVED, TARGET_DATE, SOURCE_ID
        FROM FORECAST_ERRORS
        WHERE STATION_ID = :sid
          AND TARGET_TYPE = :tt AND LEAD_BRACKET = :lb
          AND TARGET_DATE >= TRUNC(SYSDATE) - :window
        ORDER BY TARGET_DATE
        """
        bind = {"sid": station_id, "tt": target_type,
                "lb": lead_bracket, "window": window_days}

    with conn.cursor() as cur:
        cur.execute(sql, bind)
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, row)) for row in cur]


def get_per_source_rmse(conn: Any, station_id: str, target_type: str,
                        lead_bracket: str, source_ids: list[str],
                        window_days: int) -> dict[str, tuple[float, int]]:
    """Get RMSE per source for a (station, target_type, lead_bracket) cell.

    Returns {source_id: (rmse, n_obs)} for each source that has errors.
    """
    if not source_ids:
        return {}
    placeholders = ", ".join(f":s{i}" for i in range(len(source_ids)))
    sql = f"""
    SELECT SOURCE_ID,
           SQRT(AVG(ERROR_RAW * ERROR_RAW)) AS RMSE,
           COUNT(*) AS N
    FROM FORECAST_ERRORS
    WHERE STATION_ID = :sid
      AND TARGET_TYPE = :tt
      AND LEAD_BRACKET = :lb
      AND TARGET_DATE >= TRUNC(SYSDATE) - :window
      AND ERROR_RAW IS NOT NULL
      AND SOURCE_ID IN ({placeholders})
    GROUP BY SOURCE_ID
    """
    bind = {"sid": station_id, "tt": target_type, "lb": lead_bracket,
            "window": window_days}
    for i, sid in enumerate(source_ids):
        bind[f"s{i}"] = sid

    with conn.cursor() as cur:
        cur.execute(sql, bind)
        return {row[0]: (float(row[1]), int(row[2])) for row in cur}


def get_latest_ensemble_state(conn: Any, station_id: str,
                              target_type: str) -> dict | None:
    """Get the most recent ENSEMBLE_STATE for a (station, target_type).

    Returns dict with s_tk, s_weighted_tk, f_bar_tk, f_tk_top, or None.
    """
    sql = """
    SELECT S_TK, S_WEIGHTED_TK, F_BAR_TK, F_TK_TOP, SIGMA_EFF
    FROM ENSEMBLE_STATE
    WHERE STATION_ID = :sid AND TARGET_TYPE = :tt
    ORDER BY TARGET_DATE DESC
    FETCH FIRST 1 ROWS ONLY
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"sid": station_id, "tt": target_type})
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "s_tk": float(row[0]) if row[0] is not None else 0.0,
        "s_weighted_tk": float(row[1]) if row[1] is not None else 0.0,
        "f_bar_tk": float(row[2]) if row[2] is not None else 0.0,
        "f_tk_top": float(row[3]) if row[3] is not None else 0.0,
        "sigma_eff": float(row[4]) if row[4] is not None else 0.0,
    }


# ─────────────────────────────────────────────────────────────────────
# Shadow Book — write
# ─────────────────────────────────────────────────────────────────────

def upsert_shadow_book(conn: Any, rows: list[dict]) -> int:
    """MERGE into SHADOW_BOOK on TICKER PK (batched)."""
    if not rows:
        return 0
    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO SHADOW_BOOK tgt
    USING DUAL
    ON (tgt.TICKER = :ticker)
    WHEN MATCHED THEN UPDATE SET
      MU              = :mu,
      SIGMA_EFF       = :sigma_eff,
      G1_S            = :g1_s,
      ALPHA_S         = :alpha_s,
      XI_S            = :xi_s,
      OMEGA_S         = :omega_s,
      P_WIN           = :p_win,
      METAR_TRUNCATED = :metar_trunc,
      T_OBS_MAX       = :t_obs_max,
      TOP_MODEL_ID    = :tmid,
      PIPELINE_RUN_ID = :run_id,
      UPDATED_AT      = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
      TICKER, STATION_ID, TARGET_DATE, TARGET_TYPE, BIN_LOWER, BIN_UPPER,
      MU, SIGMA_EFF, G1_S, ALPHA_S, XI_S, OMEGA_S, P_WIN,
      METAR_TRUNCATED, T_OBS_MAX, TOP_MODEL_ID, PIPELINE_RUN_ID
    ) VALUES (
      :ticker, :sid, TO_DATE(:td, 'YYYY-MM-DD'), :tt, :bin_lo, :bin_hi,
      :mu, :sigma_eff, :g1_s, :alpha_s, :xi_s, :omega_s, :p_win,
      :metar_trunc, :t_obs_max, :tmid, :run_id
    )
    """
    bind_rows = [{
        "ticker": r["ticker"], "sid": r["station_id"],
        "td": str(r["target_date"]), "tt": r["target_type"],
        "bin_lo": r.get("bin_lower"), "bin_hi": r.get("bin_upper"),
        "mu": r.get("mu"), "sigma_eff": r.get("sigma_eff"),
        "g1_s": r.get("g1_s"), "alpha_s": r.get("alpha_s"),
        "xi_s": r.get("xi_s"), "omega_s": r.get("omega_s"),
        "p_win": r.get("p_win"),
        "metar_trunc": 1 if r.get("metar_truncated") else 0,
        "t_obs_max": r.get("t_obs_max"),
        "tmid": r.get("top_model_id"), "run_id": r.get("pipeline_run_id"),
    } for r in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, bind_rows)
    return len(bind_rows)


def insert_shadow_book_history(conn: Any, rows: list[dict]) -> int:
    """Append-only insert into SHADOW_BOOK_HISTORY (batched)."""
    if not rows:
        return 0
    sql = """
    INSERT INTO SHADOW_BOOK_HISTORY (TICKER, P_WIN, MU, SIGMA_EFF, PIPELINE_RUN_ID)
    VALUES (:ticker, :p_win, :mu, :sigma_eff, :run_id)
    """
    bind_rows = [{
        "ticker": r["ticker"], "p_win": r.get("p_win"),
        "mu": r.get("mu"), "sigma_eff": r.get("sigma_eff"),
        "run_id": r.get("pipeline_run_id"),
    } for r in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, bind_rows)
    return len(bind_rows)


# ─────────────────────────────────────────────────────────────────────
# Brier Scores — grading
# ─────────────────────────────────────────────────────────────────────

def grade_brier_scores(conn: Any, target_date: str) -> int:
    """Grade SHADOW_BOOK predictions against OBSERVATIONS → BRIER_SCORES.

    For each ticker with target_date, determine outcome (1 if observed in bin, 0 else),
    compute BS = (p_win - outcome)^2.
    """
    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO BRIER_SCORES tgt
    USING (
      SELECT sb.TICKER,
             sb.P_WIN AS P_WIN_AT_GRADING,
             CASE
               WHEN sb.TARGET_TYPE = 'HIGH' AND
                    obs.OBSERVED_HIGH_F >= sb.BIN_LOWER AND
                    obs.OBSERVED_HIGH_F < sb.BIN_UPPER THEN 1
               WHEN sb.TARGET_TYPE = 'LOW' AND
                    obs.OBSERVED_LOW_F >= sb.BIN_LOWER AND
                    obs.OBSERVED_LOW_F < sb.BIN_UPPER THEN 1
               ELSE 0
             END AS OUTCOME,
             POWER(sb.P_WIN - CASE
               WHEN sb.TARGET_TYPE = 'HIGH' AND
                    obs.OBSERVED_HIGH_F >= sb.BIN_LOWER AND
                    obs.OBSERVED_HIGH_F < sb.BIN_UPPER THEN 1
               WHEN sb.TARGET_TYPE = 'LOW' AND
                    obs.OBSERVED_LOW_F >= sb.BIN_LOWER AND
                    obs.OBSERVED_LOW_F < sb.BIN_UPPER THEN 1
               ELSE 0
             END, 2) AS BRIER_SCORE
      FROM SHADOW_BOOK sb
      JOIN OBSERVATIONS obs
        ON obs.STATION_ID = sb.STATION_ID
       AND obs.TARGET_DATE = sb.TARGET_DATE
      WHERE sb.TARGET_DATE = TO_DATE(:target_date, 'YYYY-MM-DD')
        AND sb.P_WIN IS NOT NULL
    ) src
    ON (tgt.TICKER = src.TICKER)
    WHEN MATCHED THEN UPDATE SET
      tgt.P_WIN_AT_GRADING = src.P_WIN_AT_GRADING,
      tgt.OUTCOME          = src.OUTCOME,
      tgt.BRIER_SCORE      = src.BRIER_SCORE,
      tgt.GRADED_AT        = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
      TICKER, P_WIN_AT_GRADING, OUTCOME, BRIER_SCORE
    ) VALUES (
      src.TICKER, src.P_WIN_AT_GRADING, src.OUTCOME, src.BRIER_SCORE
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"target_date": str(target_date)})
        return cur.rowcount or 0


# ─────────────────────────────────────────────────────────────────────
# BSS Matrix — skill scoring
# ─────────────────────────────────────────────────────────────────────

def get_bss_for_cell(conn: Any, station_id: str, lead_bracket: str,
                     target_type: str) -> dict | None:
    """Read BSS for a single cell. Returns None if not computed yet."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT BSS_1, BSS_2, IS_QUALIFIED, N_OBSERVATIONS, BS_MODEL, BS_BASELINE_1
            FROM BSS_MATRIX
            WHERE STATION_ID = :sid AND LEAD_BRACKET = :lb AND TARGET_TYPE = :tt
        """, {"sid": station_id, "lb": lead_bracket, "tt": target_type})
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "bss_1": float(row[0]) if row[0] is not None else None,
        "bss_2": float(row[1]) if row[1] is not None else None,
        "is_qualified": bool(row[2]),
        "n_observations": int(row[3]) if row[3] is not None else 0,
        "bs_model": float(row[4]) if row[4] is not None else None,
        "bs_baseline_1": float(row[5]) if row[5] is not None else None,
    }


def get_bss_matrix_all(conn: Any) -> list[dict]:
    """Read entire BSS_MATRIX for pattern analysis."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT STATION_ID, TARGET_TYPE, LEAD_BRACKET,
                   BSS_1, BSS_2, IS_QUALIFIED, N_OBSERVATIONS
            FROM BSS_MATRIX
        """)
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, row)) for row in cur]


def refresh_bss_matrix(conn: Any, window_days: int) -> int:
    """Recompute BSS for all (station, target_type, lead_bracket) cells.

    BSS_1 = 1 - BS_model / BS_clim (climatological baseline = uniform 1/N_bins).
    Hysteresis is applied in Python after reading current IS_QUALIFIED.
    """
    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO BSS_MATRIX tgt
    USING (
      WITH graded AS (
        SELECT sb.STATION_ID, sb.TARGET_TYPE,
               COALESCE(fe.LEAD_BRACKET, 'h3') AS LEAD_BRACKET,
               bs.BRIER_SCORE, bs.OUTCOME
        FROM BRIER_SCORES bs
        JOIN SHADOW_BOOK sb ON sb.TICKER = bs.TICKER
        LEFT JOIN FORECAST_ERRORS fe
          ON fe.STATION_ID = sb.STATION_ID
         AND fe.TARGET_DATE = sb.TARGET_DATE
         AND fe.TARGET_TYPE = sb.TARGET_TYPE
        WHERE sb.TARGET_DATE >= TRUNC(SYSDATE) - :window
          AND bs.BRIER_SCORE IS NOT NULL
      ),
      stats AS (
        SELECT STATION_ID, TARGET_TYPE, LEAD_BRACKET,
               COUNT(*)               AS n_obs,
               AVG(BRIER_SCORE)       AS bs_model,
               AVG(POWER(0.0667 - OUTCOME, 2)) AS bs_clim
        FROM graded
        GROUP BY STATION_ID, TARGET_TYPE, LEAD_BRACKET
        HAVING COUNT(*) >= 10
      )
      SELECT STATION_ID, TARGET_TYPE, LEAD_BRACKET,
             :window AS WINDOW_DAYS,
             bs_model, bs_clim AS BS_BASELINE_1,
             CASE WHEN bs_clim > 0 THEN 1.0 - bs_model / bs_clim ELSE NULL END AS BSS_1,
             n_obs AS N_OBSERVATIONS,
             SYSTIMESTAMP AS COMPUTED_AT
      FROM stats
    ) src
    ON (
      tgt.STATION_ID  = src.STATION_ID
      AND tgt.TARGET_TYPE  = src.TARGET_TYPE
      AND tgt.LEAD_BRACKET = src.LEAD_BRACKET
    )
    WHEN MATCHED THEN UPDATE SET
      tgt.WINDOW_DAYS   = src.WINDOW_DAYS,
      tgt.BS_MODEL      = src.bs_model,
      tgt.BS_BASELINE_1 = src.BS_BASELINE_1,
      tgt.BSS_1         = src.BSS_1,
      tgt.N_OBSERVATIONS = src.N_OBSERVATIONS,
      tgt.COMPUTED_AT   = src.COMPUTED_AT
    WHEN NOT MATCHED THEN INSERT (
      STATION_ID, TARGET_TYPE, LEAD_BRACKET, WINDOW_DAYS,
      BS_MODEL, BS_BASELINE_1, BSS_1, N_OBSERVATIONS, COMPUTED_AT
    ) VALUES (
      src.STATION_ID, src.TARGET_TYPE, src.LEAD_BRACKET, src.WINDOW_DAYS,
      src.bs_model, src.BS_BASELINE_1, src.BSS_1, src.N_OBSERVATIONS, src.COMPUTED_AT
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"window": window_days})
        return cur.rowcount or 0


# ─────────────────────────────────────────────────────────────────────
# Financial Metrics / System Alerts
# ─────────────────────────────────────────────────────────────────────

def upsert_financial_metrics(conn: Any, row: dict) -> None:
    """MERGE into FINANCIAL_METRICS on METRIC_DATE PK."""
    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO FINANCIAL_METRICS tgt
    USING DUAL
    ON (tgt.METRIC_DATE = TO_DATE(:md, 'YYYY-MM-DD'))
    WHEN MATCHED THEN UPDATE SET
      BANKROLL       = :bank,     PORTFOLIO_VALUE = :pv,
      DAILY_PNL      = :dpnl,    CUMULATIVE_PNL  = :cpnl,
      MDD_ALLTIME    = :mdd_all, MDD_ROLLING_90  = :mdd_90,
      SR_DOLLAR      = :sr_d,    SR_SIMPLE       = :sr_s,
      SHARPE_ROLLING_30 = :sr30, FDR             = :fdr,
      EUR            = :eur,     CAL             = :cal,
      MARKET_CAL     = :mcal,    N_BETS_TOTAL    = :nbt,
      N_BETS_WON     = :nbw,    N_BETS_LOST     = :nbl,
      GROSS_PROFIT   = :gp,     NET_PROFIT      = :np,
      TOTAL_FEES     = :tf,     COMPUTED_AT     = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
      METRIC_DATE, BANKROLL, PORTFOLIO_VALUE, DAILY_PNL, CUMULATIVE_PNL,
      MDD_ALLTIME, MDD_ROLLING_90, SR_DOLLAR, SR_SIMPLE, SHARPE_ROLLING_30,
      FDR, EUR, CAL, MARKET_CAL,
      N_BETS_TOTAL, N_BETS_WON, N_BETS_LOST,
      GROSS_PROFIT, NET_PROFIT, TOTAL_FEES
    ) VALUES (
      TO_DATE(:md, 'YYYY-MM-DD'), :bank, :pv, :dpnl, :cpnl,
      :mdd_all, :mdd_90, :sr_d, :sr_s, :sr30,
      :fdr, :eur, :cal, :mcal,
      :nbt, :nbw, :nbl,
      :gp, :np, :tf
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "md": str(row["metric_date"]),
            "bank": row.get("bankroll"), "pv": row.get("portfolio_value"),
            "dpnl": row.get("daily_pnl"), "cpnl": row.get("cumulative_pnl"),
            "mdd_all": row.get("mdd_alltime"), "mdd_90": row.get("mdd_rolling_90"),
            "sr_d": row.get("sr_dollar"), "sr_s": row.get("sr_simple"),
            "sr30": row.get("sharpe_rolling_30"), "fdr": row.get("fdr"),
            "eur": row.get("eur"), "cal": row.get("cal"),
            "mcal": row.get("market_cal"), "nbt": row.get("n_bets_total", 0),
            "nbw": row.get("n_bets_won", 0), "nbl": row.get("n_bets_lost", 0),
            "gp": row.get("gross_profit", 0), "np": row.get("net_profit", 0),
            "tf": row.get("total_fees", 0),
        })


# ─────────────────────────────────────────────────────────────────────
# L4: Best Bets
# ─────────────────────────────────────────────────────────────────────

def upsert_best_bets(conn: Any, rows: list[dict]) -> int:
    """MERGE into BEST_BETS — one row per ticker (batched)."""
    if not rows:
        return 0

    sql = """
    MERGE /*+ NO_PARALLEL(tgt) */ INTO BEST_BETS tgt
    USING DUAL
    ON (tgt.TICKER = :ticker)
    WHEN MATCHED THEN UPDATE SET
        PIPELINE_RUN_ID = :run_id,
        STATION_ID = :sid, TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD'),
        TARGET_TYPE = :tt, BIN_LOWER = :bl, BIN_UPPER = :bu,
        P_WIN = :pw, CONTRACT_PRICE = :cp, EV_NET = :ev,
        EV_THRESHOLD_H = :evth, ORDER_TYPE = :otype,
        C_VWAP = :cvwap, C_VWAP_NET = :cvn,
        F_STAR = :fstar, F_OP = :fop, F_FINAL = :ffinal,
        IBE_COMPOSITE = :ibe, IBE_VETO = :veto, D_SCALE = :dscale,
        GAMMA_CONVERGENCE = :gamma,
        RANK_WITHIN_STATION_DAY = :rank,
        IS_SELECTED_FOR_EXECUTION = :selected,
        PIPELINE_RUN_STATUS = :prs,
        ALL_GATE_FLAGS_JSON = :gates
    WHEN NOT MATCHED THEN INSERT (
        TICKER, PIPELINE_RUN_ID, STATION_ID, TARGET_DATE, TARGET_TYPE,
        BIN_LOWER, BIN_UPPER, P_WIN, CONTRACT_PRICE, EV_NET,
        EV_THRESHOLD_H, ORDER_TYPE, C_VWAP, C_VWAP_NET,
        F_STAR, F_OP, F_FINAL,
        IBE_COMPOSITE, IBE_VETO, D_SCALE, GAMMA_CONVERGENCE,
        RANK_WITHIN_STATION_DAY, IS_SELECTED_FOR_EXECUTION,
        PIPELINE_RUN_STATUS, ALL_GATE_FLAGS_JSON
    ) VALUES (
        :ticker, :run_id, :sid, TO_DATE(:td, 'YYYY-MM-DD'), :tt,
        :bl, :bu, :pw, :cp, :ev,
        :evth, :otype, :cvwap, :cvn,
        :fstar, :fop, :ffinal,
        :ibe, :veto, :dscale, :gamma,
        :rank, :selected,
        :prs, :gates
    )
    """
    bind_rows = [{
        "ticker": r["ticker"], "run_id": r.get("pipeline_run_id"),
        "sid": r.get("station_id"),
        "td": str(r.get("target_date", ""))[:10],
        "tt": r.get("target_type"),
        "bl": r.get("bin_lower"), "bu": r.get("bin_upper"),
        "pw": r.get("p_win"), "cp": r.get("contract_price"),
        "ev": r.get("ev_net"), "evth": r.get("ev_threshold_h"),
        "otype": r.get("order_type"),
        "cvwap": r.get("c_vwap"), "cvn": r.get("c_vwap_net"),
        "fstar": r.get("f_star"), "fop": r.get("f_op"),
        "ffinal": r.get("f_final"),
        "ibe": r.get("ibe_composite"), "veto": 1 if r.get("ibe_veto") else 0,
        "dscale": r.get("d_scale"), "gamma": r.get("gamma_convergence"),
        "rank": r.get("rank_within_station_day"),
        "selected": 1 if r.get("is_selected_for_execution") else 0,
        "prs": r.get("pipeline_run_status"),
        "gates": json.dumps(r.get("gate_flags")) if r.get("gate_flags") else None,
    } for r in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, bind_rows)
    return len(bind_rows)


# ─────────────────────────────────────────────────────────────────────
# L4: Orderbook Snapshots
# ─────────────────────────────────────────────────────────────────────

def insert_orderbook_snapshot(conn: Any, row: dict) -> None:
    """INSERT into MARKET_ORDERBOOK_SNAPSHOTS."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO MARKET_ORDERBOOK_SNAPSHOTS (
                TICKER, SNAPSHOT_UTC, YES_BOOK_JSON, NO_BOOK_JSON,
                C_VWAP_COMPUTED, AVAILABLE_DEPTH
            ) VALUES (
                :ticker, SYSTIMESTAMP, :yes, :no, :cvwap, :depth
            )
        """, {
            "ticker": row["ticker"],
            "yes": json.dumps(row.get("yes_book")) if row.get("yes_book") else None,
            "no": json.dumps(row.get("no_book")) if row.get("no_book") else None,
            "cvwap": row.get("c_vwap"),
            "depth": row.get("available_depth"),
        })


# ─────────────────────────────────────────────────────────────────────
# L4: IBE Signal Log
# ─────────────────────────────────────────────────────────────────────

def insert_ibe_signal_log(conn: Any, row: dict) -> None:
    """INSERT into IBE_SIGNAL_LOG."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO IBE_SIGNAL_LOG (
                TICKER, PIPELINE_RUN_ID,
                KCV_NORM, KCV_MOD, MPDS_K, MPDS_MOD,
                HMAS, HMAS_MOD, FCT, FCT_MOD, SCAS, SCAS_MOD,
                COMPOSITE, VETO_TRIGGERED, VETO_REASON
            ) VALUES (
                :ticker, :run_id,
                :kcv_norm, :kcv_mod, :mpds_k, :mpds_mod,
                :hmas, :hmas_mod, :fct, :fct_mod, :scas, :scas_mod,
                :composite, :veto, :reason
            )
        """, {
            "ticker": row.get("ticker"),
            "run_id": row.get("pipeline_run_id"),
            "kcv_norm": row.get("kcv_norm"),
            "kcv_mod": row.get("kcv_mod"),
            "mpds_k": row.get("mpds_k"),
            "mpds_mod": row.get("mpds_mod"),
            "hmas": row.get("hmas"),
            "hmas_mod": row.get("hmas_mod"),
            "fct": row.get("fct"),
            "fct_mod": row.get("fct_mod"),
            "scas": row.get("scas"),
            "scas_mod": row.get("scas_mod"),
            "composite": row.get("composite"),
            "veto": 1 if row.get("veto") else 0,
            "reason": row.get("veto_reason"),
        })


# ─────────────────────────────────────────────────────────────────────
# L4: Position Queries
# ─────────────────────────────────────────────────────────────────────


def get_previous_shadow_book(conn: Any, ticker: str) -> dict | None:
    """Get previous Shadow Book entry for MPDS computation."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT P_WIN, PIPELINE_RUN_ID, UPDATED_AT
            FROM SHADOW_BOOK
            WHERE TICKER = :ticker
        """, {"ticker": ticker})
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "p_win": float(row[0]) if row[0] is not None else None,
        "pipeline_run_id": row[1],
        "updated_at": row[2],
    }


def insert_system_alert(conn: Any, alert: dict) -> None:
    """Insert a new SYSTEM_ALERT."""
    sql = """
    INSERT INTO SYSTEM_ALERTS (
      ALERT_ID, ALERT_TYPE, STATION_ID, SOURCE_ID,
      SEVERITY_SCORE, DETAILS_JSON
    ) VALUES (
      :aid, :atype, :sid, :src_id, :sev, :details
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "aid": new_run_id(),
            "atype": alert.get("alert_type"),
            "sid": alert.get("station_id"),
            "src_id": alert.get("source_id"),
            "sev": alert.get("severity_score"),
            "details": json.dumps(alert.get("details")) if alert.get("details") else None,
        })

# ─────────────────────────────────────────────────────────────────────
# Kalshi Market Sync
# ─────────────────────────────────────────────────────────────────────

def upsert_kalshi_market(conn: Any, market: dict) -> None:
    """Upsert a single Kalshi market row."""
    import json
    with conn.cursor() as cur:
        cur.execute("""
            MERGE /*+ NO_PARALLEL(tgt) */ INTO KALSHI_MARKETS tgt
            USING (SELECT :ticker AS ticker FROM DUAL) src
            ON (tgt.TICKER = src.ticker)
            WHEN MATCHED THEN UPDATE SET
                EVENT_TICKER = :event_ticker,
                SERIES_TICKER = :series_ticker,
                STATION_ID = :station_id,
                TARGET_DATE = TO_DATE(:target_date, 'YYYY-MM-DD'),
                TARGET_TYPE = :target_type,
                BIN_LOWER = :bin_lower,
                BIN_UPPER = :bin_upper,
                MARKET_TITLE = :market_title,
                MARKET_SUBTITLE = :market_subtitle,
                CLOSE_TIME = :close_time,
                SETTLEMENT_TIME = :settlement_time,
                STATUS = :status,
                LAST_PRICE = :last_price,
                VOLUME = :volume,
                YES_BID = :yes_bid,
                YES_ASK = :yes_ask,
                SYNCED_AT = SYSTIMESTAMP,
                RAW_JSON = :raw_json
            WHEN NOT MATCHED THEN INSERT (
                TICKER, EVENT_TICKER, SERIES_TICKER, STATION_ID,
                TARGET_DATE, TARGET_TYPE, BIN_LOWER, BIN_UPPER,
                MARKET_TITLE, MARKET_SUBTITLE, CLOSE_TIME, SETTLEMENT_TIME,
                STATUS, LAST_PRICE, VOLUME, YES_BID, YES_ASK, RAW_JSON
            ) VALUES (
                :ticker, :event_ticker, :series_ticker, :station_id,
                TO_DATE(:target_date, 'YYYY-MM-DD'), :target_type, :bin_lower, :bin_upper,
                :market_title, :market_subtitle, :close_time, :settlement_time,
                :status, :last_price, :volume, :yes_bid, :yes_ask, :raw_json
            )
        """, {
            "ticker": market["ticker"],
            "event_ticker": market["event_ticker"],
            "series_ticker": market["series_ticker"],
            "station_id": market.get("station_id"),
            "target_date": market["target_date"],
            "target_type": market["target_type"],
            "bin_lower": market.get("bin_lower"),
            "bin_upper": market.get("bin_upper"),
            "market_title": market.get("market_title"),
            "market_subtitle": market.get("market_subtitle"),
            "close_time": market.get("close_time"),
            "settlement_time": market.get("settlement_time"),
            "status": market.get("status"),
            "last_price": market.get("last_price"),
            "volume": market.get("volume"),
            "yes_bid": market.get("yes_bid"),
            "yes_ask": market.get("yes_ask"),
            "raw_json": json.dumps(market.get("raw")) if market.get("raw") else None,
        })


def get_kalshi_bins(conn: Any, station_id: str, target_date: str,
                    target_type: str) -> list[dict]:
    """Get real Kalshi bins for a station/date/type combination.
    
    Returns list of dicts with ticker, bin_lower, bin_upper, and market prices.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT TICKER, BIN_LOWER, BIN_UPPER, LAST_PRICE, YES_BID, YES_ASK
            FROM KALSHI_MARKETS
            WHERE STATION_ID = :sid
              AND TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD')
              AND TARGET_TYPE = :tt
              AND STATUS IN ('open', 'active')
            ORDER BY BIN_LOWER NULLS FIRST
        """, {"sid": station_id, "td": target_date, "tt": target_type})
        
        return [
            {
                "ticker": row[0],
                "bin_lower": float(row[1]) if row[1] is not None else float('-inf'),
                "bin_upper": float(row[2]) if row[2] is not None else float('inf'),
                "last_price": float(row[3]) if row[3] else None,
                "yes_bid": float(row[4]) if row[4] else None,
                "yes_ask": float(row[5]) if row[5] else None,
            }
            for row in cur
        ]


def is_event_ignored(conn: Any, event_ticker: str) -> bool:
    """Check if an event ticker is in the ignored list."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM KALSHI_IGNORED_EVENTS
            WHERE EVENT_TICKER = :et
        """, {"et": event_ticker})
        return cur.fetchone() is not None


def kalshi_alert_exists(conn: Any, event_ticker: str) -> bool:
    """Check if an unknown station alert already exists for this event."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM SYSTEM_ALERTS
            WHERE ALERT_TYPE = 'UNKNOWN_KALSHI_STATION'
              AND DETAILS LIKE :ref_pattern
              AND STATUS = 'OPEN'
        """, {"ref_pattern": f'%{event_ticker}%'})
        return cur.fetchone() is not None


def create_unknown_station_alert(conn: Any, event_ticker: str,
                                  market_title: str, sample_ticker: str) -> None:
    """Create an alert for an unmatched Kalshi station."""
    import json
    import uuid
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO SYSTEM_ALERTS (
                ALERT_ID, ALERT_TYPE, SEVERITY, TITLE, DETAILS, STATUS, CREATED_AT
            ) VALUES (
                :aid, 'UNKNOWN_KALSHI_STATION', 'WARNING',
                :title, :details, 'OPEN', SYSTIMESTAMP
            )
        """, {
            "aid": str(uuid.uuid4()),
            "title": f"Unmatched Kalshi market: {event_ticker}",
            "details": json.dumps({
                "event_ticker": event_ticker,
                "market_title": market_title,
                "sample_ticker": sample_ticker,
                "action_required": "Add station to stations.py or add to KALSHI_IGNORED_EVENTS"
            }),
        })