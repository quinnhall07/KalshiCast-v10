"""METAR truncation — STUB for Phase 2.

Spec §6.4: When fresh METAR is available for same-day markets,
truncate the distribution at observed extremes and renormalize.

Active in Phase 3 when collect_metar.py exists and METAR_DAILY_MAX is populated.
"""

from __future__ import annotations


def apply_metar_truncation(bin_probs: list[dict], station_id: str,
                           target_date: str, target_type: str,
                           skewnorm_params: tuple,
                           conn=None, params=None) -> list[dict]:
    """STUB: Returns bin_probs unchanged.

    Phase 3 implementation:
    1. Check METAR_DAILY_MAX for (station, date)
    2. If fresh obs (< staleness_minutes), truncate:
       - HIGH: truncate at T_obs_max, renormalize by survival function
       - LOW: truncate at T_obs_min, renormalize by CDF
    3. Re-floor and re-normalize all P(win) values
    """
    return bin_probs
