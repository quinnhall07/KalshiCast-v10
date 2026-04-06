"""Morning pipeline — L1 collection orchestrator.

Collects forecasts from 9 sources across 20 stations, writes to Oracle.
Ported from morning.py with retry/semaphore logic extracted to collector_harness.py.
"""

from __future__ import annotations

import concurrent.futures
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from kalshicast.config import HEADERS, get_stations
from kalshicast.config.params_bootstrap import get_param_int
from kalshicast.collection.sources_registry import load_fetchers_safe
from kalshicast.collection.time_axis import truncate_issued_at_to_hour_z
from kalshicast.collection.lead_time import compute_lead_hours, classify_lead_hours
from kalshicast.collection.collector_harness import call_with_retry
from kalshicast.config.sources import SOURCES
from kalshicast.db.connection import get_conn, close_pool
from kalshicast.db.operations import (
    upsert_station,
    get_or_create_forecast_run,
    bulk_upsert_forecasts_daily,
    bulk_upsert_forecasts_hourly,
    update_pipeline_run,
    insert_system_alert,
)
from kalshicast.pipeline import pipeline_init, RUN_MORNING

log = logging.getLogger(__name__)

# Debug knobs (set via env)
DEBUG_DUMP = os.getenv("DEBUG_DUMP", "0") == "1"
DEBUG_SOURCE = os.getenv("DEBUG_SOURCE", "").strip()
DEBUG_STATION = os.getenv("DEBUG_STATION", "").strip()


# ─────────────────────────────────────────────────────────────────────
# Payload normalization (preserved from original morning.py)
# ─────────────────────────────────────────────────────────────────────

def _require_str(d: dict, k: str) -> str:
    v = d.get(k)
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"payload missing/invalid '{k}'")
    return v.strip()


def _coerce_float(x: Any, *, field: str) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if s:
            return float(s)
    raise ValueError(f"invalid float for {field}: {x!r}")


def _normalize_daily(payload: dict) -> List[Dict[str, Any]]:
    daily = payload.get("daily")
    if not isinstance(daily, list):
        raise ValueError("payload missing/invalid 'daily'")

    out: List[Dict[str, Any]] = []
    for r in daily:
        if not isinstance(r, dict):
            continue
        td = r.get("target_date")
        if not isinstance(td, str) or len(td) < 10:
            continue
        try:
            high_f = _coerce_float(r.get("high_f"), field="high_f")
            low_f = _coerce_float(r.get("low_f"), field="low_f")
        except Exception:
            continue
        out.append({"target_date": td[:10], "high_f": high_f, "low_f": low_f})

    return out


def _normalize_hourly_arrays(payload: dict) -> List[Dict[str, Any]]:
    hourly = payload.get("hourly")
    if hourly is None:
        return []
    if not isinstance(hourly, dict):
        raise ValueError("payload 'hourly' must be an object when present")

    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        return []

    key_map = {
        "temperature_f": ["temperature_f", "temperature_2m"],
        "dewpoint_f": ["dewpoint_f", "dew_point_2m"],
        "humidity_pct": ["humidity_pct", "relative_humidity_2m"],
        "wind_speed_mph": ["wind_speed_mph", "wind_speed_10m"],
        "wind_dir_deg": ["wind_dir_deg", "wind_direction_10m"],
        "cloud_cover_pct": ["cloud_cover_pct", "cloud_cover"],
        "precip_prob_pct": ["precip_prob_pct", "precipitation_probability"],
    }

    series: Dict[str, List[Any]] = {}
    for out_k, candidates in key_map.items():
        for cand in candidates:
            v = hourly.get(cand)
            if isinstance(v, list):
                series[out_k] = v
                break

    m = len(times)
    for v in series.values():
        m = min(m, len(v))

    out: List[Dict[str, Any]] = []
    for i in range(m):
        vt = times[i]
        if not isinstance(vt, str) or not vt.strip():
            continue

        row: Dict[str, Any] = {"valid_time": vt.strip()}
        for k, arr in series.items():
            val = arr[i]
            if val is None:
                continue
            try:
                row[k] = float(val)
            except Exception:
                continue
        out.append(row)

    return out


def _normalize_payload_strict(raw: Any) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not isinstance(raw, dict):
        raise ValueError(f"collector returned {type(raw)}; expected dict payload")
    issued_at = _require_str(raw, "issued_at")
    daily_rows = _normalize_daily(raw)
    hourly_rows = _normalize_hourly_arrays(raw)
    return issued_at, daily_rows, hourly_rows


# ─────────────────────────────────────────────────────────────────────
# Fetch one (station, source) pair
# ─────────────────────────────────────────────────────────────────────

def _fetch_one(st: dict, source_id: str, fetcher, provider_group: str):
    station_id = st["station_id"]
    try:
        raw = call_with_retry(fetcher, st, source_id, provider_group)
        if raw is None:
            return (station_id, st, source_id, [], [], Exception("all retries exhausted"))

        _, daily_rows, hourly_rows = _normalize_payload_strict(raw)
        return (station_id, st, source_id, daily_rows, hourly_rows, None)

    except Exception as e:
        return (station_id, st, source_id, [], [], e)


# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────

def main() -> None:
    pipeline_run_id, _ = pipeline_init(RUN_MORNING)

    stations = get_stations(active_only=True)

    # Upsert stations
    conn = get_conn()
    try:
        for st in stations:
            upsert_station(conn, st)
        conn.commit()
    finally:
        conn.close()

    # Load fetchers
    fetchers = load_fetchers_safe()
    if not fetchers:
        log.error("No enabled sources loaded.")
        conn2 = get_conn()
        try:
            insert_system_alert(conn2, {
                "alert_type": "COLLECTION_FAILURE",
                "severity_score": 0.9,
                "details": {"error": "No enabled forecast sources loaded — morning pipeline aborted."},
            })
            update_pipeline_run(conn2, pipeline_run_id, status="ERROR",
                                error_msg="No enabled sources loaded")
            conn2.commit()
        finally:
            conn2.close()
        return

    # Global timestamp sync — all records share one issued_at
    global_issued_at = truncate_issued_at_to_hour_z(datetime.now(timezone.utc))
    log.info("Locked pipeline time anchor at: %s", global_issued_at)

    total_daily = 0
    total_hourly = 0
    stations_ok = 0
    stations_fail = 0

    conn = get_conn()
    try:
        # Pre-cache run_ids (eliminates 180 redundant DB round-trips)
        run_id_cache: Dict[str, str] = {}
        for source_id in fetchers:
            run_id_cache[source_id] = get_or_create_forecast_run(
                conn, source_id=source_id, issued_at=global_issued_at
            )
        conn.commit()
        log.info("Pre-cached %d run_ids", len(run_id_cache))

        # Dispatch concurrent fetches
        max_workers = get_param_int("pipeline.max_workers")
        tasks: list[concurrent.futures.Future] = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            for st in stations:
                for source_id, fetcher in fetchers.items():
                    pg = SOURCES.get(source_id, {}).get("provider_group", "OTHER")
                    tasks.append(ex.submit(_fetch_one, st, source_id, fetcher, pg))

            for fut in concurrent.futures.as_completed(tasks):
                station_id, st, source_id, daily_rows, hourly_rows, err = fut.result()

                if err is not None:
                    log.warning("FAIL %s %s: %s", station_id, source_id, err)
                    stations_fail += 1
                    continue

                if not daily_rows and not hourly_rows:
                    log.warning("WARN %s %s: no rows", station_id, source_id)
                    continue

                stations_ok += 1
                run_id = run_id_cache[source_id]

                # Daily rows
                if daily_rows:
                    daily_batch: list[dict] = []
                    for r in daily_rows:
                        td = r["target_date"]

                        lead_high = compute_lead_hours(
                            station_tz=st["timezone"],
                            issued_at=global_issued_at,
                            target_date=td,
                            kind="high",
                            hourly_rows=hourly_rows,
                        )
                        lead_low = compute_lead_hours(
                            station_tz=st["timezone"],
                            issued_at=global_issued_at,
                            target_date=td,
                            kind="low",
                            hourly_rows=hourly_rows,
                        )

                        daily_batch.append({
                            "run_id": run_id,
                            "source_id": source_id,
                            "station_id": station_id,
                            "target_date": td,
                            "high_f": r["high_f"],
                            "low_f": r["low_f"],
                            "lead_hours_high": lead_high,
                            "lead_hours_low": lead_low,
                            "lead_bracket_high": classify_lead_hours(lead_high),
                            "lead_bracket_low": classify_lead_hours(lead_low),
                        })

                    wrote = bulk_upsert_forecasts_daily(conn, daily_batch)
                    total_daily += int(wrote or 0)

                # Hourly rows
                if hourly_rows:
                    hourly_batch: list[dict] = []
                    for hr in hourly_rows:
                        vt = hr.get("valid_time")
                        if not isinstance(vt, str) or not vt.strip():
                            continue
                        hourly_batch.append({
                            "run_id": run_id,
                            "source_id": source_id,
                            "station_id": station_id,
                            "valid_time": vt.strip(),
                            "temperature_f": hr.get("temperature_f"),
                            "dewpoint_f": hr.get("dewpoint_f"),
                            "humidity_pct": hr.get("humidity_pct"),
                            "wind_speed_mph": hr.get("wind_speed_mph"),
                            "wind_dir_deg": hr.get("wind_dir_deg"),
                            "cloud_cover_pct": hr.get("cloud_cover_pct"),
                            "precip_prob_pct": hr.get("precip_prob_pct"),
                        })

                    if hourly_batch:
                        wrote = bulk_upsert_forecasts_hourly(conn, hourly_batch)
                        total_hourly += int(wrote or 0)

                # Single commit per (station, source) result
                conn.commit()

        # Detect high failure rate and alert
        total_tasks = stations_ok + stations_fail
        fail_rate = stations_fail / total_tasks if total_tasks > 0 else 0
        final_status = "OK"
        if fail_rate >= 0.5:
            final_status = "PARTIAL"
            insert_system_alert(conn, {
                "alert_type": "COLLECTION_HIGH_FAILURE_RATE",
                "severity_score": 0.75,
                "details": {
                    "stations_ok": stations_ok,
                    "stations_fail": stations_fail,
                    "fail_rate": round(fail_rate, 3),
                    "rows_daily": total_daily,
                    "rows_hourly": total_hourly,
                },
            })
        if total_daily == 0:
            final_status = "ERROR"
            insert_system_alert(conn, {
                "alert_type": "COLLECTION_NO_DATA",
                "severity_score": 0.85,
                "details": {
                    "error": "Morning pipeline produced zero daily forecast rows.",
                    "stations_ok": stations_ok,
                    "stations_fail": stations_fail,
                },
            })

        # Catch-all alert for non-OK runs
        if final_status != "OK":
            insert_system_alert(conn, {
                "alert_type": f"PIPELINE_MORNING_{final_status}",
                "severity_score": 0.8 if final_status == "ERROR" else 0.6,
                "details": {
                    "pipeline_run_id": pipeline_run_id,
                    "status": final_status,
                    "stations_ok": stations_ok,
                    "stations_fail": stations_fail,
                    "rows_daily": total_daily,
                    "rows_hourly": total_hourly,
                },
            })

        # Finalize pipeline run
        update_pipeline_run(
            conn, pipeline_run_id,
            status=final_status,
            stations_ok=stations_ok,
            stations_fail=stations_fail,
            rows_daily=total_daily,
            rows_hourly=total_hourly,
        )
        conn.commit()

    except Exception as e:
        log.exception("Pipeline failed: %s", e)
        try:
            insert_system_alert(conn, {
                "alert_type": "PIPELINE_MORNING_CRASH",
                "severity_score": 0.95,
                "details": {"error": str(e)[:500]},
            })
            update_pipeline_run(
                conn, pipeline_run_id,
                status="ERROR",
                stations_ok=stations_ok,
                stations_fail=stations_fail,
                rows_daily=total_daily,
                rows_hourly=total_hourly,
                error_msg=str(e)[:2000],
            )
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()
        close_pool()

    log.info(
        "DONE — daily=%d hourly=%d stations_ok=%d stations_fail=%d",
        total_daily, total_hourly, stations_ok, stations_fail,
    )


if __name__ == "__main__":
    main()
