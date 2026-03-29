"""Shadow Book generation — skew-normal pricing → P(win) per bin.

Spec §6: (μ, σ_eff, G1_s) → (ξ, ω, α) → P(win) = CDF(b) - CDF(a).
"""

from __future__ import annotations

import logging
import math
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float, get_param_int

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Skew-normal parameterization (spec §6.1)
# ─────────────────────────────────────────────────────────────────────

def convert_to_skewnorm_params(mu: float, sigma_eff: float,
                                g1_s: float,
                                alpha_cap: float | None = None) -> tuple[float, float, float]:
    """Convert (μ, σ_eff, G1_s) → (ξ_s, ω_s, α_s) for scipy.stats.skewnorm.

    4-step conversion:
    1. δ from G1_s via moment-matching
    2. α (shape) = δ / √(1 - δ²), clipped
    3. ω (scale) = σ_eff / √(1 - 2δ²/π)
    4. ξ (location) = μ - ω × δ × √(2/π)

    Returns (xi_s, omega_s, alpha_s).
    """
    if alpha_cap is None:
        alpha_cap = get_param_float("pricing.alpha_cap")

    # Normal fallback when G1_s ≈ 0
    if abs(g1_s) < 1e-6:
        return mu, sigma_eff, 0.0

    # Clamp G1_s to skew-normal feasible range
    g1_clamped = max(-0.9952, min(0.9952, g1_s))

    # Step 1: δ from G1_s
    K = (abs(g1_clamped) * 2.0 / (4.0 - math.pi)) ** (2.0 / 3.0)
    delta_sq = (K * math.pi / 2.0) / (1.0 + K)

    # Guard: delta_sq must be in [0, 1)
    delta_sq = min(delta_sq, 0.999)
    delta_s = math.copysign(math.sqrt(delta_sq), g1_clamped)

    # Step 2: α (shape parameter)
    denom = 1.0 - delta_s * delta_s
    if denom <= 0:
        alpha_s = math.copysign(alpha_cap, delta_s)
    else:
        alpha_s = delta_s / math.sqrt(denom)

    alpha_s = max(-alpha_cap, min(alpha_cap, alpha_s))

    # Step 3: ω (scale parameter)
    omega_denom = 1.0 - 2.0 * delta_s * delta_s / math.pi
    if omega_denom <= 0:
        omega_s = sigma_eff
    else:
        omega_s = sigma_eff / math.sqrt(omega_denom)

    omega_s = max(omega_s, 0.01)  # prevent zero scale

    # Step 4: ξ (location parameter)
    xi_s = mu - omega_s * delta_s * math.sqrt(2.0 / math.pi)

    return xi_s, omega_s, alpha_s


# ─────────────────────────────────────────────────────────────────────
# P(win) computation (spec §6.3)
# ─────────────────────────────────────────────────────────────────────

def compute_p_win(bin_lower: float, bin_upper: float,
                  xi_s: float, omega_s: float, alpha_s: float,
                  p_min_floor: float | None = None) -> float:
    """P(win) = skewnorm.cdf(upper) - skewnorm.cdf(lower).

    Handles tail bins (lower=-inf or upper=+inf).
    Floors at p_min_floor to prevent log(0) in Kelly.
    """
    from scipy.stats import skewnorm

    if p_min_floor is None:
        p_min_floor = get_param_float("pricing.p_min_floor")

    if math.isinf(bin_lower) and bin_lower < 0:
        # Low tail: P = CDF(upper)
        p = skewnorm.cdf(bin_upper, a=alpha_s, loc=xi_s, scale=omega_s)
    elif math.isinf(bin_upper) and bin_upper > 0:
        # High tail: P = 1 - CDF(lower) = SF(lower)
        p = skewnorm.sf(bin_lower, a=alpha_s, loc=xi_s, scale=omega_s)
    else:
        # Interior bin
        p = (skewnorm.cdf(bin_upper, a=alpha_s, loc=xi_s, scale=omega_s) -
             skewnorm.cdf(bin_lower, a=alpha_s, loc=xi_s, scale=omega_s))

    return max(p, p_min_floor)


def normalize_probabilities(probs: list[float],
                            p_min_floor: float | None = None) -> list[float]:
    """Normalize probabilities to sum to 1.0 if deviation > 1%.

    Also re-applies floor.
    """
    if p_min_floor is None:
        p_min_floor = get_param_float("pricing.p_min_floor")

    total = sum(probs)
    if total <= 0:
        n = len(probs)
        return [1.0 / n] * n if n > 0 else []

    if abs(total - 1.0) > 0.01:
        log.warning("[shadow_book] P(win) sum=%.4f, renormalizing", total)
        probs = [p / total for p in probs]

    # Re-apply floor
    probs = [max(p, p_min_floor) for p in probs]

    # Final normalize
    total = sum(probs)
    if total > 0 and abs(total - 1.0) > 0.001:
        probs = [p / total for p in probs]

    return probs


# ─────────────────────────────────────────────────────────────────────
# Master pricing function
# ─────────────────────────────────────────────────────────────────────

def price_shadow_book(conn: Any, target_date: str, run_id: str) -> int:
    """Generate Shadow Book for all (station, date, type) with ensemble state.

    Steps per cell:
    1. Read ENSEMBLE_STATE for (μ_corrected, σ_eff, g1_s, top_model)
    2. Convert to skew-normal params (ξ, ω, α)
    3. Generate bins (paper mode: synthetic 2°F bins)
    4. Compute P(win) per bin
    5. Normalize
    6. Write SHADOW_BOOK + SHADOW_BOOK_HISTORY

    Returns count of shadow book rows written.
    """
    from kalshicast.db.operations import (
        upsert_shadow_book, insert_shadow_book_history,
        get_kalman_state,
    )
    from kalshicast.pricing.bin_convention import generate_station_bins
    from kalshicast.pricing.truncation import apply_metar_truncation

    # Read all ensemble state for this run
    with conn.cursor() as cur:
        cur.execute("""
            SELECT STATION_ID, TARGET_DATE, TARGET_TYPE,
                   F_TK_TOP, TOP_MODEL_ID, SIGMA_EFF, M_K
            FROM ENSEMBLE_STATE
            WHERE RUN_ID = :run_id
        """, {"run_id": run_id})
        cols = [c[0].lower() for c in cur.description]
        ensemble_rows = [dict(zip(cols, row)) for row in cur]

    if not ensemble_rows:
        log.warning("[shadow_book] no ensemble state for run %s", run_id)
        return 0

    sb_rows = []
    history_rows = []

    for er in ensemble_rows:
        station_id = er["station_id"]
        target_type = er["target_type"]
        f_top = float(er["f_tk_top"]) if er.get("f_tk_top") is not None else None
        sigma_eff = float(er["sigma_eff"]) if er.get("sigma_eff") is not None else 2.0
        top_model = er.get("top_model_id")

        if f_top is None:
            continue

        # Get Kalman bias correction
        ks = get_kalman_state(conn, station_id, target_type)
        b_k = ks["b_k"] if ks else 0.0
        mu = f_top + b_k

        # Get skewness from ensemble (stored in processing)
        # For now, compute from error history or use 0.0
        from kalshicast.db.operations import get_forecast_errors_window
        from kalshicast.processing.skewness import compute_skewness

        window = get_param_int("sigma.rmse_window_days")
        err_rows = get_forecast_errors_window(
            conn, station_id, top_model, target_type, "h2", window
        )
        err_vals = [
            float(e["error_adjusted"] if e.get("error_adjusted") is not None else e.get("error_raw"))
            for e in err_rows
            if (e.get("error_adjusted") is not None or e.get("error_raw") is not None)
        ]
        g1_s = compute_skewness(err_vals)

        # Convert to skew-normal params
        xi_s, omega_s, alpha_s = convert_to_skewnorm_params(mu, sigma_eff, g1_s)

        # Generate bins
        bins = generate_station_bins(station_id, target_date, mu, target_type)

        # Compute P(win) for each bin
        probs = []
        for b in bins:
            p = compute_p_win(b["bin_lower"], b["bin_upper"], xi_s, omega_s, alpha_s)
            probs.append(p)

        # Normalize
        probs = normalize_probabilities(probs)

        # Apply METAR truncation (stub in Phase 2)
        bin_probs = [{"bin": b, "p_win": p} for b, p in zip(bins, probs)]
        bin_probs = apply_metar_truncation(
            bin_probs, station_id, target_date, target_type,
            (xi_s, omega_s, alpha_s), conn
        )

        # Build rows
        for bp in bin_probs:
            b = bp["bin"]
            p_win = bp["p_win"]

            row = {
                "ticker": b["ticker"],
                "station_id": station_id,
                "target_date": target_date,
                "target_type": target_type,
                "bin_lower": b["bin_lower"] if not math.isinf(b["bin_lower"]) else -999.0,
                "bin_upper": b["bin_upper"] if not math.isinf(b["bin_upper"]) else 999.0,
                "mu": mu,
                "sigma_eff": sigma_eff,
                "g1_s": g1_s,
                "alpha_s": alpha_s,
                "xi_s": xi_s,
                "omega_s": omega_s,
                "p_win": p_win,
                "metar_truncated": False,
                "t_obs_max": None,
                "top_model_id": top_model,
                "pipeline_run_id": run_id,
            }
            sb_rows.append(row)

            history_rows.append({
                "ticker": b["ticker"],
                "p_win": p_win,
                "mu": mu,
                "sigma_eff": sigma_eff,
                "pipeline_run_id": run_id,
            })

    # Write to DB
    wrote = 0
    if sb_rows:
        wrote = upsert_shadow_book(conn, sb_rows)
    if history_rows:
        insert_shadow_book_history(conn, history_rows)

    conn.commit()
    log.info("[shadow_book] wrote %d rows for %s", wrote, target_date)
    return wrote
