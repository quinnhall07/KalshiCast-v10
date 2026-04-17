"""Ensemble aggregation — BSS-weighted models, spreads, σ_eff.

Spec §5.5: Top-model selection, entropy-regularized weights,
staleness decay, spread computation, sigma_eff assembly.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float, get_param_int
from kalshicast.processing.sigma import compute_sigma_for_pricing, SIGMA_FLOOR
from kalshicast.processing.skewness import compute_skewness

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Top model selection
# ─────────────────────────────────────────────────────────────────────

def select_top_model(conn: Any, station_id: str, lead_bracket: str,
                     target_type: str,
                     kalman_state: dict | None = None) -> str | None:
    """Select the model with highest BSS for this cell. §5.5.2.

    Returns source_id or None if BSS matrix empty (cold start).
    Pass kalman_state to avoid a redundant DB query.
    """
    from kalshicast.db.operations import get_bss_for_cell

    cell = get_bss_for_cell(conn, station_id, lead_bracket, target_type)
    if cell is None or cell.get("bss_1") is None:
        return None  # cold start — caller uses fallback

    ks = kalman_state
    if ks is None:
        from kalshicast.db.operations import get_kalman_state
        ks = get_kalman_state(conn, station_id, target_type)
    if ks and ks.get("top_model_id"):
        return ks["top_model_id"]

    return None


# ─────────────────────────────────────────────────────────────────────
# Weight computation
# ─────────────────────────────────────────────────────────────────────

def compute_weights(bss_scores: list[float], source_ids: list[str],
                    lambda_ent: float | None = None,
                    w_min_factor: float | None = None) -> list[float]:
    """Entropy-regularized BSS-weighted optimization. §5.5.4.

    Maximize: Σ w_m × BSS_m + λ × H(w)
    Subject to: Σ w_m = 1, w_m ≥ w_min

    Falls back to uniform weights if optimizer fails.
    """
    m = len(bss_scores)
    if m == 0:
        return []
    if m == 1:
        return [1.0]

    if lambda_ent is None:
        lambda_ent = get_param_float("ensemble.entropy_lambda")
    if w_min_factor is None:
        w_min_factor = get_param_float("ensemble.w_m_min_factor")

    w_min = w_min_factor / m
    uniform = [1.0 / m] * m

    # All BSS values are None/negative — use uniform
    valid_bss = [b for b in bss_scores if b is not None and b > 0]
    if not valid_bss:
        return uniform

    try:
        from scipy.optimize import minimize

        # Replace None with 0
        bss = [b if b is not None else 0.0 for b in bss_scores]

        def neg_objective(w):
            bss_term = sum(w[i] * bss[i] for i in range(m))
            entropy = -sum(w[i] * math.log(max(w[i], 1e-12)) for i in range(m))
            return -(bss_term + lambda_ent * entropy)

        constraints = [{"type": "eq", "fun": lambda w: sum(w) - 1.0}]
        bounds = [(w_min, 1.0)] * m
        x0 = uniform

        result = minimize(neg_objective, x0, method="SLSQP",
                          bounds=bounds, constraints=constraints,
                          options={"maxiter": 200, "ftol": 1e-9})

        if result.success:
            weights = list(result.x)
            # Ensure non-negative and normalized
            weights = [max(w, 0.0) for w in weights]
            total = sum(weights)
            if total > 0:
                return [w / total for w in weights]

    except Exception as e:
        log.warning("[ensemble] SLSQP failed, using uniform: %s", e)

    return uniform


def apply_staleness_decay(weights: list[float], ages_hours: list[float],
                          tau: float | None = None) -> tuple[list[float], list[bool]]:
    """Exponential staleness decay + renormalize. §5.5.5.

    w_m_stale = w_m × exp(-age / (τ × 24))
    Returns (new_weights, is_stale_flags).
    """
    if tau is None:
        tau = get_param_float("ensemble.staleness_tau")

    tau_hours = tau * 24.0
    decayed = []
    is_stale = []

    for w, age in zip(weights, ages_hours):
        if age > 0 and tau_hours > 0:
            factor = math.exp(-age / tau_hours)
            decayed.append(w * factor)
            is_stale.append(factor < 0.5)  # > 50% decayed = stale
        else:
            decayed.append(w)
            is_stale.append(False)

    total = sum(decayed)
    if total > 0:
        decayed = [d / total for d in decayed]
    else:
        n = len(weights)
        decayed = [1.0 / n] * n if n > 0 else []

    return decayed, is_stale


# ─────────────────────────────────────────────────────────────────────
# Spread computation
# ─────────────────────────────────────────────────────────────────────

def compute_spread(forecasts: list[float],
                   weights: list[float] | None = None) -> tuple[float, float]:
    """Compute unweighted and weighted ensemble spread. §5.5.6.

    S_unweighted = sqrt((1/(M-1)) × Σ(F_m - F̄)²)  [Bessel correction]
    S_weighted   = sqrt(Σ w_m × (F_m - F̄_w)²)

    Returns (S_unweighted, S_weighted).
    """
    m = len(forecasts)
    if m <= 1:
        return 0.0, 0.0

    # Unweighted
    f_bar = sum(forecasts) / m
    ss = sum((f - f_bar) ** 2 for f in forecasts)
    s_unweighted = math.sqrt(ss / (m - 1))

    # Weighted
    if weights is None or len(weights) != m:
        weights = [1.0 / m] * m

    f_bar_w = sum(w * f for w, f in zip(weights, forecasts))
    ss_w = sum(w * (f - f_bar_w) ** 2 for w, f in zip(weights, forecasts))
    s_weighted = math.sqrt(ss_w)

    return s_unweighted, s_weighted


def compute_sigma_eff(sigma_adj: float, s_weighted: float,
                      k_spread: float | None = None,
                      sigma_mod: float = 1.0) -> float:
    """Effective standard deviation. §5.5.7.

    σ_eff = sqrt(σ²_adj + k_spread × S²_weighted) × sigma_mod
    """
    if k_spread is None:
        k_spread = get_param_float("ensemble.k_spread")

    raw = math.sqrt(sigma_adj ** 2 + k_spread * s_weighted ** 2)
    return max(raw * sigma_mod, 0.1)


# ─────────────────────────────────────────────────────────────────────
# Per-source skill scoring
# ─────────────────────────────────────────────────────────────────────

def _compute_per_source_skill(conn: Any, station_id: str, target_type: str,
                               lead_bracket: str, source_ids: list[str],
                               window: int) -> list[float]:
    """Compute per-source skill scores from FORECAST_ERRORS.

    For each source, compute MSE from recent forecast errors.
    Convert to BSS-like skill: skill = max(0, 1 - MSE_src / MSE_worst).

    Returns list aligned with source_ids. Falls back to zeros
    (→ uniform weights via compute_weights) during cold start.
    """
    from kalshicast.db.operations import get_forecast_errors_window

    min_samples = 5
    source_mse: dict[str, float] = {}

    for src in source_ids:
        errors = get_forecast_errors_window(
            conn, station_id, src, target_type, lead_bracket, window
        )
        sq_errors = []
        for e in errors:
            val = (e.get("error_adjusted")
                   if e.get("error_adjusted") is not None
                   else e.get("error_raw"))
            if val is not None:
                sq_errors.append(float(val) ** 2)

        if len(sq_errors) >= min_samples:
            source_mse[src] = sum(sq_errors) / len(sq_errors)

    # Need at least 2 sources with data to differentiate
    if len(source_mse) < 2:
        # With exactly one source we know its performance — give it full skill
        if len(source_mse) == 1:
            only_src = next(iter(source_mse))
            return [1.0 if src == only_src else 0.0 for src in source_ids]
        return [0.0] * len(source_ids)

    mse_baseline = max(source_mse.values())
    if mse_baseline <= 0:
        return [0.0] * len(source_ids)

    return [
        max(0.0, 1.0 - source_mse[src] / mse_baseline)
        if src in source_mse else 0.0
        for src in source_ids
    ]


def _compute_per_source_skill_cached(
    error_cache: dict[str, list[float]],
    station_id: str, target_type: str,
    lead_bracket: str, source_ids: list[str],
    min_samples: int = 5,
) -> list[float]:
    """Batch-cached version of _compute_per_source_skill.

    Uses pre-fetched error_cache instead of per-source DB queries.
    """
    source_mse: dict[str, float] = {}

    for src in source_ids:
        errs = error_cache.get(f"rmse|{station_id}|{target_type}|{lead_bracket}|{src}", [])
        if len(errs) >= min_samples:
            source_mse[src] = sum(e * e for e in errs) / len(errs)

    if len(source_mse) < 2:
        if len(source_mse) == 1:
            only_src = next(iter(source_mse))
            return [1.0 if src == only_src else 0.0 for src in source_ids]
        return [0.0] * len(source_ids)

    mse_baseline = max(source_mse.values())
    if mse_baseline <= 0:
        return [0.0] * len(source_ids)

    return [
        max(0.0, 1.0 - source_mse[src] / mse_baseline)
        if src in source_mse else 0.0
        for src in source_ids
    ]


def _select_top_from_skill(skill_scores: list[float], source_ids: list[str],
                            source_forecasts: dict[str, float]) -> str | None:
    """Pick the source with highest skill score.

    Returns None if all scores are zero (cold start), letting the
    caller fall back to BSS_MATRIX or KALMAN_STATES.
    """
    if not skill_scores or max(skill_scores) <= 0:
        return None

    best_idx = max(range(len(skill_scores)), key=lambda i: skill_scores[i])
    src = source_ids[best_idx]
    return src if src in source_forecasts else None


# ─────────────────────────────────────────────────────────────────────
# Master ensemble computation
# ─────────────────────────────────────────────────────────────────────

def compute_ensemble_state(conn: Any, target_date: str, run_id: str) -> int:
    """Compute ensemble state for all (station, target_date, target_type) triples.

    For each:
    1. Fetch all forecasts
    2. Select top model from BSS_MATRIX
    3. Compute weights (entropy regularized)
    4. Apply staleness decay
    5. Compute spreads (S, S_weighted)
    6. Compute sigma (sigma.py)
    7. Compute skewness (skewness.py)
    8. Compute sigma_eff
    9. Write ENSEMBLE_STATE + MODEL_WEIGHTS

    Returns count of ensemble rows written.
    """
    from collections import defaultdict
    from kalshicast.db.operations import (
        get_latest_forecasts_for_date,
        upsert_ensemble_state, upsert_model_weights,
    )
    from kalshicast.config import get_stations
    from kalshicast.processing.sigma import compute_per_model_rmse, bayesian_shrinkage
    from kalshicast.processing.regime import detect_bimodal

    stations = get_stations(active_only=True)
    min_models = get_param_int("ensemble.min_models")
    window = get_param_int("sigma.rmse_window_days")

    # Fetch all forecasts for this date
    all_fc = get_latest_forecasts_for_date(conn, target_date)
    if not all_fc:
        log.warning("[ensemble] no forecasts found for %s", target_date)
        return 0

    # Group by (station, target_type)
    fc_by_cell: dict[tuple[str, str], list[dict]] = {}
    for fc in all_fc:
        for tt in ("HIGH", "LOW"):
            key = (fc["station_id"], tt)
            if key not in fc_by_cell:
                fc_by_cell[key] = []
            fc_by_cell[key].append(fc)

    # ── Batch pre-fetch: Kalman states ──
    kalman_cache: dict[str, dict] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT STATION_ID, TARGET_TYPE, B_K, TOP_MODEL_ID FROM KALMAN_STATES")
        for row in cur:
            key = f"{row[0]}|{row[1]}"
            kalman_cache[key] = {
                "b_k": float(row[2]) if row[2] is not None else 0.0,
                "top_model_id": row[3],
            }

    # ── Batch pre-fetch: all forecast errors for skewness + RMSE ──
    error_cache: dict[str, list[float]] = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT STATION_ID, SOURCE_ID, TARGET_TYPE, LEAD_BRACKET,
                   ERROR_ADJUSTED, ERROR_RAW
            FROM FORECAST_ERRORS
            WHERE TARGET_DATE >= TRUNC(SYSDATE) - :window
              AND (ERROR_ADJUSTED IS NOT NULL OR ERROR_RAW IS NOT NULL)
        """, {"window": window})
        for row in cur:
            sid, src, tt, lb_val, e_adj, e_raw = row
            val = float(e_adj if e_adj is not None else e_raw)
            # Key for skewness: station|source|type|bracket
            error_cache[f"{sid}|{src}|{tt}|{lb_val}"].append(val)
            # Key for per-source RMSE: station|type|bracket|source
            error_cache[f"rmse|{sid}|{tt}|{lb_val}|{src}"].append(val)

    # ── Batch pre-fetch: global RMSE per (target_type, lead_bracket) ──
    global_rmse_cache: dict[str, float] = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT TARGET_TYPE, LEAD_BRACKET,
                   SQRT(AVG(
                       CASE WHEN ERROR_ADJUSTED IS NOT NULL
                            THEN ERROR_ADJUSTED * ERROR_ADJUSTED
                            ELSE ERROR_RAW * ERROR_RAW END
                   )) AS GLOBAL_RMSE
            FROM FORECAST_ERRORS
            WHERE TARGET_DATE >= TRUNC(SYSDATE) - :window
              AND (ERROR_ADJUSTED IS NOT NULL OR ERROR_RAW IS NOT NULL)
            GROUP BY TARGET_TYPE, LEAD_BRACKET
        """, {"window": window})
        for row in cur:
            global_rmse_cache[f"{row[0]}|{row[1]}"] = float(row[2]) if row[2] else SIGMA_FLOOR

    ensemble_rows = []
    weight_rows = []
    count = 0

    for st in stations:
        station_id = st["station_id"]

        for target_type in ("HIGH", "LOW"):
            key = (station_id, target_type)
            forecasts_list = fc_by_cell.get(key, [])

            if not forecasts_list:
                continue

            # Extract forecast values per source
            source_forecasts: dict[str, float] = {}
            for fc in forecasts_list:
                src = fc.get("source_id")
                val = fc.get("high_f") if target_type == "HIGH" else fc.get("low_f")
                if src and val is not None:
                    source_forecasts[src] = float(val)

            source_ids = list(source_forecasts.keys())
            fc_values = [source_forecasts[s] for s in source_ids]
            m_k = len(source_ids)

            if m_k < 1:
                continue

            # Determine lead bracket from first forecast
            fc0 = forecasts_list[0]
            lb = fc0.get("lead_bracket_high") if target_type == "HIGH" else fc0.get("lead_bracket_low")
            lb = lb or "h2"

            # Kalman state from batch cache
            ks_key = f"{station_id}|{target_type}"
            ks = kalman_cache.get(ks_key)

            # Per-source skill from cached error history (cold start → uniform)
            bss_scores = _compute_per_source_skill_cached(
                error_cache, station_id, target_type, lb, source_ids
            )

            # Top model = highest skill source (consistent with weight derivation)
            top_model = _select_top_from_skill(
                bss_scores, source_ids, source_forecasts
            )
            if top_model is None:
                # Cold start: fall back to Kalman state → first available
                top_model = ks["top_model_id"] if ks and ks.get("top_model_id") else None
                if top_model is None or top_model not in source_forecasts:
                    top_model = source_ids[0]

            f_top = source_forecasts[top_model]

            weights = compute_weights(bss_scores, source_ids)

            # Staleness decay (all ages = 0 in paper mode since we just collected)
            ages = [0.0] * m_k
            weights, stale_flags = apply_staleness_decay(weights, ages)

            # Spreads
            s_unweighted, s_weighted = compute_spread(fc_values, weights)

            # Sigma from cached errors (replaces per-iteration DB call)
            station_errs = []
            for sid in source_ids:
                station_errs.extend(error_cache.get(
                    f"rmse|{station_id}|{target_type}|{lb}|{sid}", []))
            if station_errs:
                station_rmse = compute_per_model_rmse(station_errs)
                g_rmse = global_rmse_cache.get(f"{target_type}|{lb}", SIGMA_FLOOR)
                sigma_adj = bayesian_shrinkage(station_rmse, g_rmse, len(station_errs))
                sigma_adj = max(sigma_adj, 0.1)
            else:
                sigma_adj = SIGMA_FLOOR

            # Skewness from cached error history
            err_vals = error_cache.get(
                f"{station_id}|{top_model}|{target_type}|{lb}", [])
            g1_s = compute_skewness(err_vals)

            # Regime-aware σ handling when bimodal detected. Kept as a safety
            # net for extreme splits that the skew-normal alone can't absorb;
            # shadow_book.price_shadow_book also re-detects bimodal and, when
            # the split is tractable, routes pricing through the §6.5
            # mixture-of-normals path so the centroid separation is modelled
            # directly instead of hidden inside σ.
            bimodal = detect_bimodal(fc_values, s_unweighted)
            if bimodal is not None:
                centroid_dist = bimodal["centroid_distance"]
                sigma_inflation_cap = get_param_float("regime.sigma_inflation_cap")
                target_sigma = max(sigma_adj, centroid_dist / 2.0)
                if target_sigma > sigma_inflation_cap:
                    log.warning(
                        "[ensemble] %s/%s bimodal: centroid_dist=%.1f → "
                        "sigma_adj=%.2f exceeds cap=%.2f; clamping",
                        station_id, target_type, centroid_dist,
                        target_sigma, sigma_inflation_cap,
                    )
                    sigma_adj = sigma_inflation_cap
                else:
                    sigma_adj = target_sigma
                    log.info(
                        "[ensemble] %s/%s bimodal: inflating sigma_adj to %.2f",
                        station_id, target_type, sigma_adj,
                    )

            # σ_eff
            sigma_eff = compute_sigma_eff(sigma_adj, s_weighted)

            # Kalman-corrected μ per spec §5.6: μ = F_top + B_k, where B_k is
            # the estimated bias added back to the forecast (residual
            # convention is F_top + B_k − O, so B_k > 0 when observation runs
            # hotter than forecast). Previously this subtracted B_k, which
            # silently applied the bias in the wrong direction.
            b_k = ks["b_k"] if ks else 0.0
            mu = f_top + b_k

            # Build rows
            weight_json = {s: round(w, 6) for s, w in zip(source_ids, weights)}
            stale_ids = [s for s, st_flag in zip(source_ids, stale_flags) if st_flag]

            ensemble_rows.append({
                "run_id": run_id,
                "station_id": station_id,
                "target_date": target_date,
                "target_type": target_type,
                "f_tk_top": f_top,
                "top_model_id": top_model,
                "f_bar_tk": sum(w * f for w, f in zip(weights, fc_values)),
                "s_tk": s_unweighted,
                "s_weighted_tk": s_weighted,
                "sigma_eff": sigma_eff,
                "m_k": m_k,
                "weight_json": weight_json,
                "stale_model_ids": ",".join(stale_ids) if stale_ids else None,
            })

            for sid, w, bss, is_stale in zip(source_ids, weights, bss_scores, stale_flags):
                weight_rows.append({
                    "run_id": run_id,
                    "station_id": station_id,
                    "source_id": sid,
                    "lead_bracket": lb,
                    "w_m": w,
                    "bss_m": bss,
                    "is_stale": is_stale,
                    "stale_decay_factor": 1.0,
                })

            count += 1

    # Write to DB
    if ensemble_rows:
        upsert_ensemble_state(conn, ensemble_rows)
    if weight_rows:
        upsert_model_weights(conn, weight_rows)

    conn.commit()
    log.info("[ensemble] wrote %d ensemble states, %d weight rows for %s",
             count, len(weight_rows), target_date)
    return count
