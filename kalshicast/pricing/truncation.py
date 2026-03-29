"""METAR truncation — adjust price distributions for same-day markets.

Spec §6.4: When fresh METAR is available (within staleness_minutes),
truncate the distribution at observed extremes and renormalize.
"""

from __future__ import annotations

import logging
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float, get_param_int

log = logging.getLogger(__name__)


def apply_metar_truncation(
    bin_probs: list[dict],
    station_id: str,
    target_date: str,
    target_type: str,
    skewnorm_params: tuple,
    conn: Any = None,
    params: dict | None = None,
) -> list[dict]:
    """Truncate probability distribution using fresh METAR observations.

    For HIGH targets: bins below T_obs_max become impossible → set to 0, renormalize.
    For LOW targets: bins above T_obs_min become impossible → set to 0, renormalize.

    Returns bin_probs (possibly modified) with p_win values adjusted.
    """
    if conn is None:
        return bin_probs

    # Check master switch
    enabled = get_param_int("metar.truncation_enabled")
    if not enabled:
        return bin_probs

    lead_cutoff = get_param_float("metar.lead_hours_cutoff")
    staleness_min = get_param_int("metar.staleness_minutes")
    p_min_floor = get_param_float("pricing.p_min_floor")

    # Query METAR_DAILY_MAX for this station and date
    with conn.cursor() as cur:
        cur.execute("""
            SELECT T_OBS_MAX_F, T_OBS_MIN_F, LAST_OBS_AT
            FROM METAR_DAILY_MAX
            WHERE STATION_ID = :sid
              AND LOCAL_DATE = TO_DATE(:td, 'YYYY-MM-DD')
              AND LAST_OBS_AT >= SYSTIMESTAMP - NUMTODSINTERVAL(:stale, 'MINUTE')
        """, {"sid": station_id, "td": target_date, "stale": staleness_min})
        row = cur.fetchone()

    if row is None:
        return bin_probs

    t_obs_max = float(row[0]) if row[0] is not None else None
    t_obs_min = float(row[1]) if row[1] is not None else None

    if target_type == "HIGH" and t_obs_max is not None:
        # Temperature can only go higher — truncate bins below current observed max
        truncation_temp = t_obs_max
        for b in bin_probs:
            upper = b.get("bin_upper")
            if upper is not None and upper <= truncation_temp:
                b["p_win"] = 0.0
                b["metar_truncated"] = True

    elif target_type == "LOW" and t_obs_min is not None:
        # Temperature can only go lower — truncate bins above current observed min
        truncation_temp = t_obs_min
        for b in bin_probs:
            lower = b.get("bin_lower")
            if lower is not None and lower >= truncation_temp:
                b["p_win"] = 0.0
                b["metar_truncated"] = True

    else:
        return bin_probs

    # Renormalize remaining probabilities
    total = sum(b["p_win"] for b in bin_probs)
    if total <= 0:
        return bin_probs

    for b in bin_probs:
        b["p_win"] = b["p_win"] / total

    # Re-floor at p_min_floor
    for b in bin_probs:
        if b["p_win"] > 0 and b["p_win"] < p_min_floor:
            b["p_win"] = p_min_floor

    # Final renormalize after flooring
    total = sum(b["p_win"] for b in bin_probs)
    if total > 0:
        for b in bin_probs:
            b["p_win"] = b["p_win"] / total

    n_truncated = sum(1 for b in bin_probs if b.get("metar_truncated"))
    if n_truncated:
        log.info("[truncation] %s/%s %s: truncated %d bins (obs=%s)",
                 station_id, target_date, target_type, n_truncated,
                 t_obs_max if target_type == "HIGH" else t_obs_min)

    return bin_probs
