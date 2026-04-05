"""Sigma computation — RMSE and Bayesian shrinkage.

Spec §5.3: Per-model RMSE → global RMSE → shrinkage blend → σ_eff.
"""

from __future__ import annotations

import logging
import math
import statistics
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
    """Global RMSE: median of station-level RMSEs with outlier exclusion.

    Outlier: > 2 × rolling mean. Use median (robust to outliers).
    """
    values = [v for v in station_rmses.values() if v > 0]
    if not values:
        return SIGMA_FLOOR

    if len(values) == 1:
        return values[0]

    # Outlier exclusion: remove > 2× median
    med = statistics.median(values)
    filtered = [v for v in values if v <= 2.0 * med]

    if not filtered:
        return med

    return statistics.median(filtered)


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
    from kalshicast.config import get_stations

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

    # Compute global RMSE across all stations for the same (type, bracket).
    # Use cache to avoid O(N_stations²) DB queries when called in a batch loop.
    cache_key = (target_type, lead_bracket)
    if global_rmse_cache is not None and cache_key in global_rmse_cache:
        global_rmse = global_rmse_cache[cache_key]
    else:
        stations = get_stations(active_only=True)
        station_rmses: dict[str, float] = {}
        for st in stations:
            sid = st["station_id"]
            if sid == station_id:
                station_rmses[sid] = station_rmse
                continue
            rows = get_forecast_errors_window(
                conn, sid, None, target_type, lead_bracket, window
            )
            errs = []
            for e in rows:
                val = e.get("error_adjusted") if e.get("error_adjusted") is not None else e.get("error_raw")
                if val is not None:
                    errs.append(float(val))
            if errs:
                station_rmses[sid] = compute_per_model_rmse(errs)

        global_rmse = compute_global_rmse(station_rmses) if station_rmses else SIGMA_FLOOR
        if global_rmse_cache is not None:
            global_rmse_cache[cache_key] = global_rmse

    sigma_adj = bayesian_shrinkage(station_rmse, global_rmse, len(errors), m_prior)

    return max(sigma_adj, 0.1)  # never return zero
