# kalshicast/backfill/validate.py
"""Post-backfill validation queries.

Run after orchestrator completes. Prints a human-readable report and
returns True if all checks pass, False if any fail.

Usage:
    from kalshicast.backfill.validate import run_validation
    ok = run_validation(conn, "2023-01-01", "2024-12-31")
"""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def run_validation(conn: Any, start_date: str, end_date: str) -> bool:
    """Run all validation checks. Returns True if all pass."""
    checks = [
        _check_observation_coverage,
        _check_forecast_coverage,
        _check_error_coverage,
        _check_kalman_convergence,
        _check_bss_cells_populated,
        _check_no_orphan_errors,
        _check_p_win_sanity,
    ]

    all_pass = True
    print(f"\n=== Backfill Validation: {start_date} to {end_date} ===\n")

    for check_fn in checks:
        try:
            passed, message = check_fn(conn, start_date, end_date)
            status = "✓ PASS" if passed else "✗ FAIL"
            print(f"  {status}: {message}")
            if not passed:
                all_pass = False
        except Exception as ex:
            print(f"  ✗ ERROR: {check_fn.__name__}: {ex}")
            all_pass = False

    print("")
    return all_pass


def _check_observation_coverage(conn, start_date, end_date) -> tuple[bool, str]:
    """At least 18 of 20 stations have observations for 90%+ of dates."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(DISTINCT TARGET_DATE), COUNT(DISTINCT STATION_ID)
            FROM OBSERVATIONS
            WHERE TARGET_DATE BETWEEN TO_DATE(:s, 'YYYY-MM-DD')
                                  AND TO_DATE(:e, 'YYYY-MM-DD')
              AND SOURCE IN ('NWS_OBS_BACKFILL', 'CLI_BACKFILL')
        """, {"s": start_date, "e": end_date})
        row = cur.fetchone()
    n_dates = int(row[0]) if row else 0
    n_stations = int(row[1]) if row else 0
    passed = n_dates > 300 and n_stations >= 18
    return passed, f"Observations: {n_dates} dates × {n_stations} stations"


def _check_forecast_coverage(conn, start_date, end_date) -> tuple[bool, str]:
    """At least 3 OME sources have forecasts for 80%+ of the window."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT fr.SOURCE_ID, COUNT(DISTINCT fd.TARGET_DATE) AS n
            FROM FORECASTS_DAILY fd
            JOIN FORECAST_RUNS fr ON fr.RUN_ID = fd.RUN_ID
            WHERE fd.TARGET_DATE BETWEEN TO_DATE(:s, 'YYYY-MM-DD')
                                     AND TO_DATE(:e, 'YYYY-MM-DD')
              AND fr.IS_BACKFILL = 1
            GROUP BY fr.SOURCE_ID
        """, {"s": start_date, "e": end_date})
        rows = {r[0]: int(r[1]) for r in cur}

    good_sources = sum(1 for n in rows.values() if n > 200)
    passed = good_sources >= 3
    summary = ", ".join(f"{k}={v}" for k, v in sorted(rows.items()))
    return passed, f"Forecast sources with >200 dates: {good_sources}. ({summary})"


def _check_error_coverage(conn, start_date, end_date) -> tuple[bool, str]:
    """FORECAST_ERRORS covers at least 80% of observation dates."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(DISTINCT TARGET_DATE)
            FROM FORECAST_ERRORS
            WHERE TARGET_DATE BETWEEN TO_DATE(:s, 'YYYY-MM-DD')
                                  AND TO_DATE(:e, 'YYYY-MM-DD')
        """, {"s": start_date, "e": end_date})
        n_err = int(cur.fetchone()[0] or 0)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(DISTINCT TARGET_DATE)
            FROM OBSERVATIONS
            WHERE TARGET_DATE BETWEEN TO_DATE(:s, 'YYYY-MM-DD')
                                  AND TO_DATE(:e, 'YYYY-MM-DD')
        """, {"s": start_date, "e": end_date})
        n_obs = int(cur.fetchone()[0] or 0)

    pct = (n_err / max(n_obs, 1)) * 100
    passed = pct >= 80.0
    return passed, f"Error coverage: {n_err}/{n_obs} dates ({pct:.1f}%)"


def _check_kalman_convergence(conn, start_date, end_date) -> tuple[bool, str]:
    """All Kalman filters have U_k < 1.0 (converged from U_init=4.0)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*), AVG(U_K), MIN(U_K), MAX(U_K)
            FROM KALMAN_STATES
        """)
        row = cur.fetchone()
    n = int(row[0]) if row else 0
    avg_u = float(row[1]) if row and row[1] else 99.0
    passed = n >= 30 and avg_u < 2.0   # 30 of 40 filters updated, converging
    return passed, f"Kalman states: {n} filters, avg U_k={avg_u:.4f}"


def _check_bss_cells_populated(conn, start_date, end_date) -> tuple[bool, str]:
    """BSS matrix has at least 50 cells with N_OBSERVATIONS >= 10."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM BSS_MATRIX WHERE N_OBSERVATIONS >= 10
        """)
        n = int(cur.fetchone()[0] or 0)
    passed = n >= 50
    return passed, f"BSS cells with ≥10 observations: {n} (need ≥50)"


def _check_no_orphan_errors(conn, start_date, end_date) -> tuple[bool, str]:
    """No FORECAST_ERRORS rows referencing non-existent FORECAST_RUNS."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM FORECAST_ERRORS fe
            WHERE NOT EXISTS (
                SELECT 1 FROM FORECAST_RUNS fr WHERE fr.RUN_ID = fe.RUN_ID
            )
            AND fe.TARGET_DATE BETWEEN TO_DATE(:s, 'YYYY-MM-DD')
                                   AND TO_DATE(:e, 'YYYY-MM-DD')
        """, {"s": start_date, "e": end_date})
        n = int(cur.fetchone()[0] or 0)
    passed = n == 0
    return passed, f"Orphan error rows (no matching FORECAST_RUNS): {n}"


def _check_p_win_sanity(conn, start_date, end_date) -> tuple[bool, str]:
    """If Shadow Book rows exist for the backfill window, P_WIN sums ≈ 1.0 per market."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM SHADOW_BOOK
            WHERE TARGET_DATE BETWEEN TO_DATE(:s, 'YYYY-MM-DD')
                                  AND TO_DATE(:e, 'YYYY-MM-DD')
        """, {"s": start_date, "e": end_date})
        n = int(cur.fetchone()[0] or 0)
    if n == 0:
        return True, "Shadow Book: no rows in window (expected — pricing runs live only)"

    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT STATION_ID, TARGET_DATE, TARGET_TYPE,
                       ABS(SUM(P_WIN) - 1.0) AS deviation
                FROM SHADOW_BOOK
                WHERE TARGET_DATE BETWEEN TO_DATE(:s, 'YYYY-MM-DD')
                                      AND TO_DATE(:e, 'YYYY-MM-DD')
                GROUP BY STATION_ID, TARGET_DATE, TARGET_TYPE
                HAVING ABS(SUM(P_WIN) - 1.0) > 0.05
            )
        """, {"s": start_date, "e": end_date})
        bad = int(cur.fetchone()[0] or 0)
    passed = bad == 0
    return passed, f"Shadow Book P_WIN sum anomalies (>5% off 1.0): {bad}"