"""Sigma computation — RMSE and Bayesian shrinkage.

Spec §5.3: Per-model RMSE → global RMSE → shrinkage blend → σ_eff.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from kalshicast.config.params_bootstrap import get_param_int, get_param_float

log = logging.getLogger(__name__)

# Absolute minimum sigma to prevent division by zero
SIGMA_FLOOR = 2.0


def compute_per_model_rmse(errors: list[float]) -> float:
    """RMSE = sqrt(mean(e²)). Returns 0.0 if no errors."""
    if not errors:
        return 0.0
    return math.sqrt(sum(e * e for e in errors) / len(errors))


def compute_global_rmse(station_rmses: dict[str, float]) -> float:
    """Compute the global RMSE prior across all stations.

    Averages the individual station RMSEs to use as the global baseline
    for Bayesian shrinkage.
    """
    if not station_rmses:
        return 0.0
    return sum(station_rmses.values()) / len(station_rmses)


def compute_global_rmse_sql(conn: Any, target_type: str, lead_bracket: str,
                            window_days: int) -> float:
    """Compute RMSE across all stations for a (target_type, lead_bracket) cell.

    Single SQL query — more efficient than per-station loop.
    Used as the prior in Bayesian shrinkage. Falls back to SIGMA_FLOOR
    if no cross-station errors exist.
    """
    sql = """
    SELECT SQRT(AVG(
        CASE WHEN ERROR_ADJUSTED IS NOT NULL THEN ERROR_ADJUSTED * ERROR_ADJUSTED
             ELSE ERROR_RAW * ERROR_RAW END
    )) AS GLOBAL_RMSE
    FROM FORECAST_ERRORS
    WHERE TARGET_TYPE = :tt AND LEAD_BRACKET = :lb
      AND TARGET_DATE >= TRUNC(SYSDATE) - :window
      AND (ERROR_ADJUSTED IS NOT NULL OR ERROR_RAW IS NOT NULL)
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"tt": target_type, "lb": lead_bracket,
                          "window": window_days})
        row = cur.fetchone()
    if row and row[0] is not None:
        return max(float(row[0]), 0.1)
    return SIGMA_FLOOR


def bayesian_shrinkage(station_rmse: float, global_rmse: float,
                       n: int, m_prior: int | None = None) -> float:
    """Bayesian shrinkage: blend station-specific with global estimate.

    σ_adj = (N × σ_station + m_prior × σ_global) / (N + m_prior)
    """
    if m_prior is None:
        m_prior = get_param_int("sigma.m_prior")

    if n + m_prior == 0:
        return global_rmse or SIGMA_FLOOR

    return (n * station_rmse + m_prior * global_rmse) / (n + m_prior)


def compute_sigma_for_pricing(conn: Any, station_id: str, target_type: str,
                              lead_bracket: str,
                              global_rmse_cache: dict[tuple[str, str], float] | None = None) -> float:
    """Compute σ_adj for a (station, type, bracket) cell.

    1. Fetch Kalman-corrected errors (use ERROR_ADJUSTED if available, else ERROR_RAW)
    2. Compute station-level RMSE
    3. Compute global RMSE across all stations for same (type, bracket)
    4. Apply Bayesian shrinkage
    5. Return σ_adj (floored at SIGMA_FLOOR if zero)

    Pass a shared ``global_rmse_cache`` dict (keyed by ``(target_type,
    lead_bracket)``) to avoid repeated per-station DB queries when called
    for every station inside a batch run.
    """
    from kalshicast.db.operations import get_forecast_errors_window

    window = get_param_int("sigma.rmse_window_days")
    m_prior = get_param_int("sigma.m_prior")

    # Get errors for this station
    errors_rows = get_forecast_errors_window(
        conn, station_id, None, target_type, lead_bracket, window
    )

    # Use adjusted errors if available, else raw
    errors = []
    for e in errors_rows:
        val = e.get("error_adjusted") if e.get("error_adjusted") is not None else e.get("error_raw")
        if val is not None:
            errors.append(float(val))

    if not errors:
        return SIGMA_FLOOR

    station_rmse = compute_per_model_rmse(errors)

    if station_rmse == 0:
        return SIGMA_FLOOR

    # Real global RMSE via single SQL query, with cache to avoid repeated calls
    cache_key = (target_type, lead_bracket)
    if global_rmse_cache is not None and cache_key in global_rmse_cache:
        global_rmse = global_rmse_cache[cache_key]
    else:
        global_rmse = compute_global_rmse_sql(conn, target_type, lead_bracket, window)
        if global_rmse_cache is not None:
            global_rmse_cache[cache_key] = global_rmse

    sigma_adj = bayesian_shrinkage(station_rmse, global_rmse, len(errors), m_prior)

    return max(sigma_adj, 0.1)  # never return zero
