"""Night pipeline — 13-step L1+L2+L5 orchestrator.

Spec §9.1: Runs at 06:00 UTC. Scores yesterday's forecasts against observations,
updates Kalman filters, refreshes BSS matrix, computes financial metrics.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any

from kalshicast.config.params_bootstrap import get_param_int, load_db_overrides
from kalshicast.db.connection import init_db, get_conn, close_pool
from kalshicast.db.schema import ensure_schema, seed_config_tables
from kalshicast.db.operations import (
    new_run_id, insert_pipeline_run, update_pipeline_run,
    load_all_params,
)

log = logging.getLogger(__name__)


def main() -> None:
    """Night pipeline — 13-step execution sequence."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    # Step 1: init_db + PIPELINE_RUNS
    init_db()
    conn = get_conn()
    try:
        ensure_schema(conn)
        seed_config_tables(conn)
    finally:
        conn.close()

    pipeline_run_id = new_run_id()
    conn = get_conn()
    try:
        insert_pipeline_run(conn, pipeline_run_id, "night")
        conn.commit()
    finally:
        conn.close()

    # Step 2: load_params
    conn = get_conn()
    try:
        db_params = load_all_params(conn)
        load_db_overrides(db_params)
    finally:
        conn.close()

    # Target date: yesterday in US Eastern Time
    now_est = datetime.now(ZoneInfo("America/New_York"))
    target_date = (now_est.date() - timedelta(days=1)).isoformat()
    log.info("Night pipeline for target_date=%s (run_id=%s)", target_date, pipeline_run_id[:8])

    status = "OK"
    error_msg = None
    steps_ok = 0

    conn = get_conn()
    try:
        # Step 3: fetch_cli_observations
        try:
            from kalshicast.collection.collectors.collect_cli import fetch_observations
            from kalshicast.config import get_stations
            stations = get_stations(active_only=True)
            fetch_observations(stations=stations, target_date=target_date)
            conn.commit()
            steps_ok += 1
            log.info("Step 3 OK: observations fetched")
        except Exception as e:
            log.warning("Step 3 WARN: observation fetch failed: %s", e)

        # Step 4: check_amendments
        try:
            amended = _check_amendments(conn, target_date)
            steps_ok += 1
            log.info("Step 4 OK: %d amendments detected", len(amended))
        except Exception as e:
            amended = []
            log.warning("Step 4 WARN: amendment check failed: %s", e)

        # Step 5: build_forecast_errors
        try:
            from kalshicast.processing.errors import build_forecast_errors
            n_errors = build_forecast_errors(conn, target_date, pipeline_run_id)
            conn.commit()
            steps_ok += 1
            log.info("Step 5 OK: %d forecast errors", n_errors)
        except Exception as e:
            log.error("Step 5 ERROR: forecast errors failed: %s", e)

        # Step 6: update_kalman_filters
        try:
            from kalshicast.processing.kalman import update_kalman_filters
            n_kalman = update_kalman_filters(conn, target_date, pipeline_run_id)
            steps_ok += 1
            log.info("Step 6 OK: %d Kalman filters updated", n_kalman)
        except Exception as e:
            log.error("Step 6 ERROR: Kalman update failed: %s", e)

        # Step 7: retroactive_kalman_correction
        try:
            if amended:
                from kalshicast.processing.kalman import retroactive_kalman_correction
                retroactive_kalman_correction(conn, amended, pipeline_run_id)
            steps_ok += 1
            log.info("Step 7 OK: amendment correction done")
        except Exception as e:
            log.warning("Step 7 WARN: amendment correction failed: %s", e)

        # Step 8: refresh_dashboard_stats
        try:
            from kalshicast.processing.dashboard import refresh_dashboard_stats
            refresh_dashboard_stats(conn)
            conn.commit()
            steps_ok += 1
            log.info("Step 8 OK: dashboard stats refreshed")
        except Exception as e:
            log.error("Step 8 ERROR: dashboard stats failed: %s", e)

        # Step 9: grade_brier_scores
        try:
            from kalshicast.evaluation.brier import grade_brier_scores
            n_graded = grade_brier_scores(conn, target_date)
            conn.commit()  # <--- Add this missing commit
            steps_ok += 1
            log.info("Step 9 OK: %d Brier scores graded", n_graded)
        except Exception as e:
            log.warning("Step 9 WARN: Brier grading failed: %s", e)

        # Step 10: refresh_bss_matrix
        try:
            from kalshicast.evaluation.bss_matrix import refresh_bss_matrix
            n_cells = refresh_bss_matrix(conn)
            steps_ok += 1
            log.info("Step 10 OK: %d BSS cells refreshed", n_cells)
        except Exception as e:
            log.warning("Step 10 WARN: BSS matrix refresh failed: %s", e)

        # Step 11: compute_financial_metrics
        try:
            from kalshicast.evaluation.financial import compute_financial_metrics
            compute_financial_metrics(conn, target_date)
            steps_ok += 1
            log.info("Step 11 OK: financial metrics computed")
        except Exception as e:
            log.warning("Step 11 WARN: financial metrics failed: %s", e)

        # Step 12: run_pattern_classifier
        try:
            interval = get_param_int("eval.pattern_check_interval_days")
            day_num = (now_est.date() - now_est.date().replace(month=1, day=1)).days
            if day_num % interval == 0:
                from kalshicast.evaluation.pattern_classifier import run_pattern_classifier
                alerts = run_pattern_classifier(conn)
                log.info("Step 12 OK: pattern classifier ran, %d alerts", len(alerts))
            else:
                log.info("Step 12 SKIP: not pattern check day (every %d days)", interval)
            steps_ok += 1
        except Exception as e:
            log.warning("Step 12 WARN: pattern classifier failed: %s", e)

        # Step 13: update_pipeline_run
        update_pipeline_run(
            conn, pipeline_run_id,
            status=status,
            stations_ok=steps_ok,
        )
        conn.commit()

    except Exception as e:
        log.exception("Night pipeline failed: %s", e)
        status = "ERROR"
        error_msg = str(e)[:2000]
        try:
            update_pipeline_run(conn, pipeline_run_id, status=status,
                                error_msg=error_msg)
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()
        close_pool()

    log.info("DONE — night pipeline: %d/12 steps OK, status=%s", steps_ok, status)


def _check_amendments(conn: Any, target_date: str) -> list[tuple]:
    """Check for amended observations in the lookback window.

    Returns list of (station_id, target_date) tuples where AMENDED=1.
    """
    lookback = get_param_int("pipeline.amendment_lookback_days")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT STATION_ID, TARGET_DATE
            FROM OBSERVATIONS
            WHERE AMENDED = 1
              AND TARGET_DATE >= TO_DATE(:td, 'YYYY-MM-DD') - :lb
              AND TARGET_DATE <= TO_DATE(:td, 'YYYY-MM-DD')
        """, {"td": target_date, "lb": lookback})
        return [(row[0], row[1]) for row in cur]


if __name__ == "__main__":
    main()
