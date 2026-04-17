"""Shadow Book generation — skew-normal pricing → P(win) per bin.

Spec §6: (μ, σ_eff, G1_s) → (ξ, ω, α) → P(win) = CDF(b) - CDF(a).
"""

from __future__ import annotations

import logging
import math
from typing import Any
from kalshicast.db.operations import get_kalshi_bins
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
                            p_min_floor: float | None = None,
                            *,
                            context: str | None = None,
                            warn_threshold: float = 0.10) -> list[float]:
    """Normalize probabilities to sum to 1.0 per spec §6.3/§6.4.

    Always renormalizes (never skips); deviations > ``warn_threshold`` (10%) are
    surfaced as WARN-level log entries so systematic bin-coverage issues remain
    diagnosable without blocking rows.
    """
    if p_min_floor is None:
        p_min_floor = get_param_float("pricing.p_min_floor")

    tag = f"[shadow_book {context}]" if context else "[shadow_book]"

    total = sum(probs)
    if total <= 0:
        n = len(probs)
        if n > 0:
            log.warning(
                "%s P(win) sum=%.4f ≤ 0 — falling back to uniform over %d bins",
                tag, total, n,
            )
        return [1.0 / n] * n if n > 0 else []

    if abs(total - 1.0) > warn_threshold:
        log.warning(
            "%s P(win) sum=%.4f (dev=%+.4f from 1.0), renormalizing over %d bins",
            tag, total, total - 1.0, len(probs),
        )
    if abs(total - 1.0) > 0.001:
        probs = [p / total for p in probs]

    # Re-apply floor
    probs = [max(p, p_min_floor) for p in probs]

    # Final normalize after flooring
    total = sum(probs)
    if total > 0 and abs(total - 1.0) > 0.001:
        probs = [p / total for p in probs]

    return probs


# ─────────────────────────────────────────────────────────────────────
# Bimodal mixture-of-normals (spec §6.5)
# ─────────────────────────────────────────────────────────────────────

def compute_p_win_bimodal(
    bin_lower: float, bin_upper: float,
    mu1: float, mu2: float, sigma: float,
    w1: float, w2: float,
    p_min_floor: float | None = None,
) -> float:
    """P(win) under a two-component Gaussian mixture (spec §6.5).

    Mixture CDF(x) = w1·Φ((x-μ1)/σ) + w2·Φ((x-μ2)/σ); the two components
    share a single σ (taken from σ_eff) so width-of-centroid-split drives
    multi-modality rather than one component swamping the other.

    Handles ±∞ tails analogously to ``compute_p_win``.
    """
    from scipy.stats import norm

    if p_min_floor is None:
        p_min_floor = get_param_float("pricing.p_min_floor")

    sigma = max(sigma, 0.01)

    def _cdf(x: float) -> float:
        return w1 * norm.cdf(x, loc=mu1, scale=sigma) + w2 * norm.cdf(x, loc=mu2, scale=sigma)

    if math.isinf(bin_lower) and bin_lower < 0:
        p = _cdf(bin_upper)
    elif math.isinf(bin_upper) and bin_upper > 0:
        p = 1.0 - _cdf(bin_lower)
    else:
        p = _cdf(bin_upper) - _cdf(bin_lower)

    return max(p, p_min_floor)


# ─────────────────────────────────────────────────────────────────────
# Master pricing function
# ─────────────────────────────────────────────────────────────────────

def price_shadow_book(conn: Any, target_date: str, run_id: str) -> int:
    """Generate Shadow Book for all (station, date, type) with ensemble state.
    
    Optimized for bulk DB fetching to prevent N+1 query latency.
    """
    from collections import defaultdict
    from kalshicast.db.operations import upsert_shadow_book, insert_shadow_book_history
    from kalshicast.pricing.bin_convention import generate_station_bins
    from kalshicast.pricing.truncation import apply_metar_truncation
    from kalshicast.processing.skewness import compute_skewness
    from kalshicast.processing.regime import detect_bimodal

    # 1. Read all ensemble state for this run
    with conn.cursor() as cur:
        cur.execute("""
            SELECT STATION_ID, TARGET_DATE, TARGET_TYPE,
                   F_TK_TOP, TOP_MODEL_ID, SIGMA_EFF, M_K, S_TK
            FROM ENSEMBLE_STATE
            WHERE RUN_ID = :run_id
        """, {"run_id": run_id})
        cols = [c[0].lower() for c in cur.description]
        ensemble_rows = [dict(zip(cols, row)) for row in cur]

    if not ensemble_rows:
        log.warning("[shadow_book] no ensemble state for run %s", run_id)
        return 0

    # Bulk-fetch per-source forecasts for this date so we can re-detect bimodal
    # regime per (station, target_type) and route pricing through §6.5 when
    # appropriate. Built once per run to avoid N queries inside the loop.
    fc_by_group: dict[str, list[float]] = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT STATION_ID, TARGET_DATE, HIGH_F, LOW_F
            FROM FORECASTS_DAILY
            WHERE TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD')
        """, {"td": target_date})
        for row in cur:
            sid, td, hi, lo = row[0], str(row[1])[:10], row[2], row[3]
            if hi is not None:
                fc_by_group[f"{sid}|{td}|HIGH"].append(float(hi))
            if lo is not None:
                fc_by_group[f"{sid}|{td}|LOW"].append(float(lo))

    # Build station_id → cli_site mapping for Kalshi ticker generation
    from kalshicast.config.stations import STATIONS as _ALL_STATIONS
    station_cli_map = {s["station_id"]: s.get("cli_site", s["station_id"].lstrip("K")) for s in _ALL_STATIONS}

    # 2. BULK FETCH: Kalman States
    # Fetch all Kalman states at once instead of querying per-station inside the loop
    kalman_cache: dict[str, float] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT STATION_ID, TARGET_TYPE, B_K FROM KALMAN_STATES")
        for row in cur:
            key = f"{row[0]}|{row[1]}"
            kalman_cache[key] = float(row[2]) if row[2] is not None else 0.0

    # 3. BULK FETCH: Forecast Errors (Last 90 days)
    # Fetch all historical errors at once to compute skewness efficiently
    window = get_param_int("sigma.rmse_window_days")
    error_cache = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT STATION_ID, SOURCE_ID, TARGET_TYPE, ERROR_ADJUSTED, ERROR_RAW
            FROM FORECAST_ERRORS
            WHERE LEAD_BRACKET = 'h2'
              AND TARGET_DATE >= CURRENT_DATE - :window
        """, {"window": window})
        for row in cur:
            sid, mid, tt, e_adj, e_raw = row[0], row[1], row[2], row[3], row[4]
            if e_adj is not None or e_raw is not None:
                val = float(e_adj if e_adj is not None else e_raw)
                error_cache[f"{sid}|{mid}|{tt}"].append(val)

    sb_rows = []
    history_rows = []
    n_skipped_no_bins = 0
    n_groups_processed = 0
    n_mixture_priced = 0

    n_missing_sigma = 0

    # 4. Process all rows in memory (Zero DB calls inside this loop!)
    for er in ensemble_rows:
        station_id = er["station_id"]
        target_type = er["target_type"]
        f_top = float(er["f_tk_top"]) if er.get("f_tk_top") is not None else None
        if er.get("sigma_eff") is None:
            sigma_eff = 2.0
            n_missing_sigma += 1
        else:
            sigma_eff = float(er["sigma_eff"])
        s_tk = float(er["s_tk"]) if er.get("s_tk") is not None else 0.0
        top_model = er.get("top_model_id")

        if f_top is None:
            continue

        n_groups_processed += 1

        # Get Kalman bias correction from local cache
        ks_key = f"{station_id}|{target_type}"
        b_k = kalman_cache.get(ks_key, 0.0)
        mu = f_top + b_k

        # Get historical errors from local cache to compute skewness
        err_key = f"{station_id}|{top_model}|{target_type}"
        err_vals = error_cache.get(err_key, [])
        g1_s = compute_skewness(err_vals)

        # Convert to skew-normal params (always — used for METAR truncation
        # even when the mixture path handles P(win))
        xi_s, omega_s, alpha_s = convert_to_skewnorm_params(mu, sigma_eff, g1_s)

        # Generate bins
        _cli_site = station_cli_map.get(station_id)
        bins = get_kalshi_bins(conn, station_id, target_date, target_type)

        if not bins:
            log.debug("No Kalshi markets for %s/%s/%s - skipping",
                    station_id, target_date, target_type)
            n_skipped_no_bins += 1
            continue

        # Spec §6.5 mixture-of-normals when the forecast distribution is
        # bimodal. Falls back to skew-normal when no bimodal signal or when
        # the forecast set is too small (detect_bimodal needs ≥4 forecasts).
        fc_list = fc_by_group.get(f"{station_id}|{target_date}|{target_type}", [])
        bimodal = detect_bimodal(fc_list, s_tk) if fc_list else None

        if bimodal is not None:
            n1 = bimodal["cluster_size_1"]
            n2 = bimodal["cluster_size_2"]
            denom = n1 + n2
            w1_mix = (n1 / denom) if denom else 0.5
            w2_mix = 1.0 - w1_mix
            mu1_corr = bimodal["centroid_1"] + b_k
            mu2_corr = bimodal["centroid_2"] + b_k
            probs = [
                compute_p_win_bimodal(
                    b["bin_lower"], b["bin_upper"],
                    mu1_corr, mu2_corr, sigma_eff,
                    w1_mix, w2_mix,
                )
                for b in bins
            ]
            n_mixture_priced += 1
            log.debug(
                "[shadow_book %s/%s/%s] mixture priced: c1=%.1f c2=%.1f "
                "w=%.2f/%.2f σ=%.2f",
                station_id, target_date, target_type,
                mu1_corr, mu2_corr, w1_mix, w2_mix, sigma_eff,
            )
        else:
            probs = [compute_p_win(b["bin_lower"], b["bin_upper"],
                                   xi_s, omega_s, alpha_s) for b in bins]

        # Spec §6.3: renormalize to 1.0 across listed bins. Large deviations
        # are logged but no longer block the group — a 0.85 or 1.15 sum
        # shrinks to 1.0 uniformly, which is preferable to dropping the bet.
        probs = normalize_probabilities(
            probs,
            context=f"{station_id}|{target_date}|{target_type}",
        )

        # Apply METAR truncation
        bin_probs = [{"bin": b, "p_win": p} for b, p in zip(bins, probs)]
        bin_probs = apply_metar_truncation(
            bin_probs, station_id, target_date, target_type,
            (xi_s, omega_s, alpha_s), conn
        )

        # Build rows
        for bp in bin_probs:
            b = bp["bin"]
            p_win = bp["p_win"]

            sb_rows.append({
                "ticker": b["ticker"],  # Real Kalshi ticker
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
            })

            history_rows.append({
                "ticker": b["ticker"],
                "p_win": p_win,
                "mu": mu,
                "sigma_eff": sigma_eff,
                "pipeline_run_id": run_id,
            })

    # 5. Bulk Write to DB
    wrote = 0
    if sb_rows:
        wrote = upsert_shadow_book(conn, sb_rows)
    if history_rows:
        insert_shadow_book_history(conn, history_rows)

    conn.commit()
    if wrote == 0 and n_skipped_no_bins == n_groups_processed and n_groups_processed > 0:
        log.info(
            "[shadow_book] %s: no Kalshi markets listed yet for any of %d "
            "ensemble groups — Kalshi typically lists weather markets "
            "24–48h ahead of expiry, so future-dated runs will often return 0 rows.",
            target_date, n_groups_processed,
        )
    else:
        log.info(
            "[shadow_book] wrote %d rows for %s "
            "(processed=%d, skipped_no_bins=%d, mixture_priced=%d, "
            "missing_sigma=%d)",
            wrote, target_date,
            n_groups_processed, n_skipped_no_bins, n_mixture_priced,
            n_missing_sigma,
        )
    return wrote
