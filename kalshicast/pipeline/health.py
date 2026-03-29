"""Health monitor — detect missed runs, staleness, MDD breaches, unresolved alerts.

Spec §9.4: Runs every 5 minutes via health heartbeat. Checks DB connectivity,
pipeline schedule adherence, METAR freshness, and critical alert status.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from kalshicast.config.params_bootstrap import get_param_int, get_param_float
from kalshicast.db.operations import insert_system_alert

log = logging.getLogger(__name__)


def check_db_connectivity(conn: Any) -> bool:
    """Ping database with a simple SELECT."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM DUAL")
            return cur.fetchone() is not None
    except Exception as e:
        log.error("[health] DB ping failed: %s", e)
        return False


def check_missed_runs(conn: Any, window_hours: int = 25) -> list[dict]:
    """Detect pipeline runs that should have occurred but didn't."""
    expected = {
        "morning": get_param_int("pipeline.morning_utc_hour"),
        "night": get_param_int("pipeline.night_utc_hour"),
        "market_open": get_param_int("pipeline.market_open_utc_hour"),
    }

    missed = []
    now = datetime.now(timezone.utc)

    for run_type, sched_hour in expected.items():
        # Check if there's a run in the last window_hours
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM PIPELINE_RUNS
                WHERE RUN_TYPE = :rt
                  AND STARTED_UTC >= SYSTIMESTAMP - NUMTODSINTERVAL(:hours, 'HOUR')
            """, {"rt": run_type, "hours": window_hours})
            count = cur.fetchone()[0]

        # If no run and we're past the scheduled hour today
        if count == 0 and now.hour > sched_hour:
            missed.append({
                "run_type": run_type,
                "expected_hour_utc": sched_hour,
                "hours_since_check": window_hours,
            })

    return missed


def check_metar_freshness(conn: Any) -> list[str]:
    """Find stations with stale or missing METAR data."""
    staleness_min = get_param_int("metar.staleness_minutes")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.STATION_ID
            FROM STATIONS s
            LEFT JOIN METAR_DAILY_MAX m
              ON m.STATION_ID = s.STATION_ID
              AND m.LOCAL_DATE = TRUNC(SYSDATE)
            WHERE s.IS_ACTIVE = 1
              AND (m.LAST_OBS_AT IS NULL
                   OR m.LAST_OBS_AT < SYSTIMESTAMP - NUMTODSINTERVAL(:stale, 'MINUTE'))
        """, {"stale": staleness_min})
        return [row[0] for row in cur]


def check_mdd_status(conn: Any) -> dict:
    """Check current MDD against safe/halt thresholds."""
    mdd_safe = get_param_float("drawdown.mdd_safe")
    mdd_halt = get_param_float("drawdown.mdd_halt")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT MDD_ALLTIME, MDD_ROLLING_90
            FROM FINANCIAL_METRICS
            ORDER BY METRIC_DATE DESC
            FETCH FIRST 1 ROWS ONLY
        """)
        row = cur.fetchone()

    if not row:
        return {"mdd_alltime": 0.0, "mdd_90": 0.0, "status": "NO_DATA"}

    mdd_all = float(row[0]) if row[0] else 0.0
    mdd_90 = float(row[1]) if row[1] else 0.0

    if mdd_all >= mdd_halt:
        status = "HALT"
    elif mdd_all >= mdd_safe:
        status = "WARNING"
    else:
        status = "OK"

    return {"mdd_alltime": mdd_all, "mdd_90": mdd_90, "status": status,
            "mdd_safe": mdd_safe, "mdd_halt": mdd_halt}


def check_unresolved_alerts(conn: Any) -> list[dict]:
    """Find unresolved CRITICAL alerts."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ALERT_ID, ALERT_TYPE, STATION_ID, SEVERITY_SCORE, ALERT_TS
            FROM SYSTEM_ALERTS
            WHERE IS_RESOLVED = 0 AND SEVERITY_SCORE >= 0.7
            ORDER BY ALERT_TS DESC
            FETCH FIRST 20 ROWS ONLY
        """)
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, row)) for row in cur]


def run_health_check(conn: Any) -> dict:
    """Master health check — returns full status report."""
    report: dict[str, Any] = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "healthy": True,
        "checks": {},
    }

    # DB connectivity
    db_ok = check_db_connectivity(conn)
    report["checks"]["db"] = {"ok": db_ok}
    if not db_ok:
        report["healthy"] = False
        return report

    # Missed runs
    missed = check_missed_runs(conn)
    report["checks"]["missed_runs"] = {"count": len(missed), "details": missed}
    if missed:
        report["healthy"] = False
        for m in missed:
            insert_system_alert(conn, {
                "alert_type": "MISSED_PIPELINE_RUN",
                "severity_score": 0.8,
                "details": m,
            })

    # METAR freshness
    stale_stations = check_metar_freshness(conn)
    report["checks"]["metar_freshness"] = {
        "stale_count": len(stale_stations),
        "stations": stale_stations[:10],
    }

    # MDD status
    mdd = check_mdd_status(conn)
    report["checks"]["mdd"] = mdd
    if mdd["status"] == "HALT":
        report["healthy"] = False

    # Unresolved alerts
    alerts = check_unresolved_alerts(conn)
    report["checks"]["unresolved_alerts"] = {"count": len(alerts)}
    if len(alerts) >= 5:
        report["healthy"] = False

    if report["healthy"]:
        conn.commit()

    status_str = "HEALTHY" if report["healthy"] else "UNHEALTHY"
    log.info("[health] %s — missed=%d stale_metar=%d mdd=%s alerts=%d",
             status_str, len(missed), len(stale_stations), mdd["status"], len(alerts))

    return report
