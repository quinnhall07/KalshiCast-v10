# kalshicast/backfill/errors.py
"""Historical error computation and Kalman filter replay.

Runs strictly oldest-to-newest. Reuses:
  build_forecast_errors_for_date() from db/operations.py
  update_kalman_filters()          from processing/kalman.py

This is equivalent to running night.py for every day in the backfill window.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from kalshicast.db.operations import build_forecast_errors_for_date, new_run_id

log = logging.getLogger(__name__)


def _date_range(start_date: str, end_date: str):
    """Yield 'YYYY-MM-DD' strings from start to end inclusive, in order."""
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def _mark_backfill_errors(conn: Any, target_date: str) -> None:
    """Set LEAD_HOURS_APPROX = 1 on errors derived from backfill forecast rows."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE FORECAST_ERRORS fe
                SET fe.LEAD_HOURS_APPROX = 1
                WHERE fe.TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD')
                  AND EXISTS (
                      SELECT 1 FROM FORECAST_RUNS fr
                      WHERE fr.RUN_ID = fe.RUN_ID
                        AND fr.IS_BACKFILL = 1
                  )
            """, {"td": target_date})
    except Exception as ex:
        # Column may not exist in older schemas — non-fatal
        log.debug("[errors] LEAD_HOURS_APPROX update failed for %s: %s", target_date, ex)


def build_backfill_errors(
    conn: Any,
    start_date: str,
    end_date: str,
    skip_dates: set[str] | None = None,
) -> int:
    """Build FORECAST_ERRORS for all dates in window, oldest-to-newest.

    After each date's MERGE, marks backfill-derived rows with LEAD_HOURS_APPROX=1.
    Commits after each date so a restart picks up where it left off.

    Returns total rows written.
    """
    total = 0
    for d in _date_range(start_date, end_date):
        if skip_dates and d in skip_dates:
            log.debug("[errors] skipping %s (already exists)", d)
            continue
        n = build_forecast_errors_for_date(conn, d)
        _mark_backfill_errors(conn, d)
        conn.commit()
        if n:
            log.info("[errors] %s: %d error rows", d, n)
        total += n
    return total


def replay_kalman_filters(
    conn: Any,
    start_date: str,
    end_date: str,
    skip_dates: set[str] | None = None,
) -> int:
    """Replay Kalman filter updates for all dates, strictly chronological.

    Calls update_kalman_filters() for each date exactly once.
    This is the night pipeline's Step 6, run in batch.

    WARNING: Running this out of order corrupts Kalman state.
    The caller (orchestrator) must guarantee start_date is the earliest
    date not already in KALMAN_HISTORY.

    Returns count of filter-date updates applied.
    """
    from kalshicast.processing.kalman import update_kalman_filters

    total = 0
    run_id = new_run_id()  # single run_id for the entire replay

    for d in _date_range(start_date, end_date):
        if skip_dates and d in skip_dates:
            log.debug("[kalman_replay] skipping %s", d)
            continue
        try:
            n = update_kalman_filters(conn, d, run_id)
            total += n
            if n:
                log.info("[kalman_replay] %s: %d filters updated", d, n)
        except Exception as ex:
            log.warning("[kalman_replay] %s failed: %s", d, ex)

    log.info("[kalman_replay] complete: %d total filter-updates over window", total)
    return total