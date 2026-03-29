"""Market-open pipeline — paper mode (L2+L3, no L4 execution).

Spec §9.3: Runs at 14:00 UTC. Computes ensemble state, prices Shadow Book.
Steps 8-10 (market fetch, gates, orders) skipped in paper mode.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
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
    """Market-open pipeline — paper mode (steps 1-7 only)."""
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
        insert_pipeline_run(conn, pipeline_run_id, "market_open")
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

    # Target dates: today + next FORECAST_DAYS
    now_utc = datetime.now(timezone.utc)
    forecast_days = get_param_int("pipeline.forecast_days")
    target_dates = [(now_utc.date() + timedelta(days=d)).isoformat()
                    for d in range(forecast_days)]

    log.info("Market-open pipeline (PAPER MODE) run_id=%s, dates=%s",
             pipeline_run_id[:8], target_dates)

    status = "OK"
    error_msg = None
    total_ensemble = 0
    total_shadow = 0

    conn = get_conn()
    try:
        # Step 3: fetch_bankroll (paper mode stub)
        bankroll = 1000.0
        log.info("Step 3: paper mode bankroll=$%.2f", bankroll)

        # Step 4: fetch_metar (stub — Phase 3)
        log.info("Step 4: METAR fetch skipped (paper mode)")

        # Step 5: fetch_afd (stub — Phase 3)
        log.info("Step 5: AFD fetch skipped (paper mode)")

        # Step 6: compute_ensemble_state
        try:
            from kalshicast.processing.ensemble import compute_ensemble_state
            for td in target_dates:
                n = compute_ensemble_state(conn, td, pipeline_run_id)
                total_ensemble += n
            log.info("Step 6 OK: %d ensemble states across %d dates",
                     total_ensemble, len(target_dates))
        except Exception as e:
            log.error("Step 6 ERROR: ensemble computation failed: %s", e)
            status = "PARTIAL"

        # Step 7: price_shadow_book
        try:
            from kalshicast.pricing.shadow_book import price_shadow_book
            for td in target_dates:
                n = price_shadow_book(conn, td, pipeline_run_id)
                total_shadow += n
            log.info("Step 7 OK: %d shadow book rows across %d dates",
                     total_shadow, len(target_dates))
        except Exception as e:
            log.error("Step 7 ERROR: shadow book pricing failed: %s", e)
            status = "PARTIAL"

        # Steps 8-10: PAPER MODE — execution skipped
        log.info("Steps 8-10: SKIPPED (paper mode — no market fetch, gates, or orders)")

        # Step 11: update pipeline_day_health (stub)
        log.info("Step 11: pipeline_day_health update skipped (paper mode)")

        # Step 12: update_pipeline_run
        update_pipeline_run(
            conn, pipeline_run_id,
            status=status,
            rows_daily=total_shadow,
        )
        conn.commit()

    except Exception as e:
        log.exception("Market-open pipeline failed: %s", e)
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

    log.info("DONE — market_open: ensemble=%d shadow_book=%d status=%s",
             total_ensemble, total_shadow, status)


if __name__ == "__main__":
    main()
