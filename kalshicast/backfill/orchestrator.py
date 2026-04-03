# kalshicast/backfill/orchestrator.py
"""Backfill orchestrator — enforces load order and idempotency.

Phase order (mandatory):
  1. Observations  (Iowa ASOS, then NWS CLI archive to overwrite with exact match)
  2. Forecasts     (OME Historical per model per station)
  3. Errors        (DB MERGE per date, chronological)
  4. Kalman replay (chronological, strict)
  5. BSS refresh   (once, after all errors exist)

Each phase checks DB coverage before running and skips dates already loaded.
Partial runs are safe to restart.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from kalshicast.backfill.config import (
    BACKFILL_START, BACKFILL_END, OME_HISTORICAL_MODELS,
    BACKFILL_MAX_WORKERS,
)
from kalshicast.backfill.errors import build_backfill_errors, replay_kalman_filters
from kalshicast.backfill.forecasts import load_ome_historical_forecasts
from kalshicast.backfill.observations import (
    load_nws_station_observations, load_cli_archive_observations,
)
from kalshicast.config import get_stations
from kalshicast.db.operations import get_backfill_coverage
from kalshicast.evaluation.bss_matrix import refresh_bss_matrix

log = logging.getLogger(__name__)


def _all_dates_in_window(start: str, end: str) -> set[str]:
    """Return the complete set of YYYY-MM-DD strings in [start, end]."""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    out = set()
    cur = s
    while cur <= e:
        out.add(cur.isoformat())
        cur += timedelta(days=1)
    return out


def load_all_observations(
    conn: Any,
    start_date: str,
    end_date: str,
    skip_dates: set[str],
) -> int:
    """Phase 1: Load historical observations for all active stations.

    Pass 1 (NWS station obs): hourly JSON chunked by month, aggregated to daily high/low.
    Pass 2 (CLI archive): walks the CLI product text archive, overwrites station-obs
    rows with exact bulletin values. CLI values always win — same source as live system.
    """
    stations = get_stations(active_only=True)
    total = 0

    log.info("[backfill] Phase 1a: NWS station observations (%d stations)", len(stations))
    for st in stations:
        n = load_nws_station_observations(conn, st["station_id"], start_date, end_date)
        total += n
        log.info("[backfill] NWS obs %s: %d dates", st["station_id"], n)

    log.info("[backfill] Phase 1b: NWS CLI archive (%d stations)", len(stations))
    for st in stations:
        n = load_cli_archive_observations(conn, st, start_date, end_date)
        total += n
        log.info("[backfill] CLI %s: %d dates", st["station_id"], n)

    return total


def load_all_forecasts(
    conn: Any,
    start_date: str,
    end_date: str,
    existing_forecast_dates: dict[str, set],
) -> int:
    """Phase 2: Load OME historical forecasts for all models and stations.

    Skips (source_id, date) pairs already in the DB.
    """
    stations = get_stations(active_only=True)
    total = 0

    for model_spec in OME_HISTORICAL_MODELS:
        source_id = model_spec["source_id"]
        ome_model  = model_spec["models"]
        already    = existing_forecast_dates.get(source_id, set())

        log.info("[backfill] Phase 2: %s (%d stations, %d dates already loaded)",
                 source_id, len(stations), len(already))

        for st in stations:
            n = load_ome_historical_forecasts(
                conn,
                station=st,
                source_id=source_id,
                ome_model=ome_model,
                start_date=start_date,
                end_date=end_date,
                skip_dates=already,
            )
            total += n
            if n:
                log.info("[backfill] %s/%s: %d daily rows", source_id, st["station_id"], n)

    return total


def run_backfill(
    conn: Any,
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    skip_phases: list[str] | None = None,
) -> dict:
    """Run the full backfill pipeline in the mandatory load order.

    Args:
        conn:         Active Oracle DB connection.
        start_date:   YYYY-MM-DD string. Defaults to BACKFILL_START.
        end_date:     YYYY-MM-DD string. Defaults to BACKFILL_END.
        skip_phases:  List of phase names to force-skip even if data is missing.
                      Values: "observations", "forecasts", "errors", "kalman", "bss"
                      Use only for debugging — skipping phases breaks load order.

    Returns:
        dict with keys: start_date, end_date, phases (dict of phase → rows_written)
    """
    start = start_date or BACKFILL_START.isoformat()
    end   = end_date   or BACKFILL_END.isoformat()
    skip  = set(skip_phases or [])

    log.info("=" * 70)
    log.info("[backfill] START: %s to %s", start, end)
    log.info("=" * 70)

    # Check coverage once at the start
    coverage = get_backfill_coverage(conn, start, end)
    all_dates = _all_dates_in_window(start, end)

    results: dict[str, int] = {}

    # ── Phase 1: Observations ─────────────────────────────────────────────────
    missing_obs = all_dates - coverage["observation_dates"]
    if "observations" in skip:
        log.info("[backfill] Phase 1 SKIPPED (in skip_phases)")
        results["observations"] = 0
    elif not missing_obs:
        log.info("[backfill] Phase 1 SKIPPED: all %d dates have observations", len(all_dates))
        results["observations"] = 0
    else:
        log.info("[backfill] Phase 1: %d dates need observations", len(missing_obs))
        n = load_all_observations(conn, start, end, coverage["observation_dates"])
        results["observations"] = n
        # Refresh coverage for subsequent phases
        coverage = get_backfill_coverage(conn, start, end)

    # ── Phase 2: Forecasts ────────────────────────────────────────────────────
    if "forecasts" in skip:
        log.info("[backfill] Phase 2 SKIPPED (in skip_phases)")
        results["forecasts"] = 0
    else:
        total_fc_dates = sum(len(v) for v in coverage["forecast_dates"].values())
        expected_fc_dates = len(all_dates) * len(OME_HISTORICAL_MODELS)
        if total_fc_dates >= expected_fc_dates:
            log.info("[backfill] Phase 2 SKIPPED: forecasts appear complete")
            results["forecasts"] = 0
        else:
            log.info("[backfill] Phase 2: loading OME historical forecasts")
            n = load_all_forecasts(conn, start, end, coverage["forecast_dates"])
            results["forecasts"] = n
            coverage = get_backfill_coverage(conn, start, end)

    # ── Phase 3: Errors ───────────────────────────────────────────────────────
    missing_err = all_dates - coverage["error_dates"]
    if "errors" in skip:
        log.info("[backfill] Phase 3 SKIPPED (in skip_phases)")
        results["errors"] = 0
    elif not missing_err:
        log.info("[backfill] Phase 3 SKIPPED: all dates have errors")
        results["errors"] = 0
    else:
        log.info("[backfill] Phase 3: computing errors for %d dates", len(missing_err))
        n = build_backfill_errors(conn, start, end, coverage["error_dates"])
        results["errors"] = n
        coverage = get_backfill_coverage(conn, start, end)

    # ── Phase 4: Kalman replay ────────────────────────────────────────────────
    if "kalman" in skip:
        log.info("[backfill] Phase 4 SKIPPED (in skip_phases)")
        results["kalman"] = 0
    else:
        # Kalman runs for dates that have errors AND are not yet in Kalman history
        error_dates = coverage["error_dates"]
        kalman_done = coverage["kalman_dates"]
        kalman_needed = error_dates - kalman_done

        if not kalman_needed:
            log.info("[backfill] Phase 4 SKIPPED: Kalman up to date")
            results["kalman"] = 0
        else:
            log.info("[backfill] Phase 4: Kalman replay for %d dates", len(kalman_needed))
            # replay_kalman_filters() must run oldest-to-newest — pass the full window
            # and let it skip already-processed dates
            n = replay_kalman_filters(conn, start, end, kalman_done)
            results["kalman"] = n

    # ── Phase 5: BSS matrix refresh ───────────────────────────────────────────
    if "bss" in skip:
        log.info("[backfill] Phase 5 SKIPPED (in skip_phases)")
        results["bss"] = 0
    else:
        log.info("[backfill] Phase 5: refreshing BSS matrix")
        n = refresh_bss_matrix(conn)
        results["bss"] = n

    log.info("=" * 70)
    log.info("[backfill] COMPLETE: %s", results)
    log.info("=" * 70)

    return {
        "start_date": start,
        "end_date":   end,
        "phases":     results,
    }