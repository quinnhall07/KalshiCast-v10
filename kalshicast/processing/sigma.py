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
                              lead_bracket: str) -> float:
    """Compute σ_adj for a (station, type, bracket) cell.

    1. Fetch Kalman-corrected errors (use ERROR_ADJUSTED if available, else ERROR_RAW)
    2. Compute station-level RMSE
    3. Compute global RMSE across all stations
    4. Apply Bayesian shrinkage
    5. Return σ_adj (floored at SIGMA_FLOOR if zero)
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

    # For global RMSE we'd need all stations — approximate with just this station
    # Full implementation would query all stations; for now use station RMSE directly
    # with shrinkage toward SIGMA_FLOOR as global estimate
    sigma_adj = bayesian_shrinkage(station_rmse, SIGMA_FLOOR, len(errors), m_prior)

    return max(sigma_adj, 0.1)  # never return zero
