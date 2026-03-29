"""Smirnov (1973) Kelly sizing for mutually exclusive outcomes.

Spec §7.4: Three-step algorithm for temperature bins (exactly one settles YES),
plus full sizing chain (cap → Φ → IBE → Γ → position → D_scale → jitter).
"""

from __future__ import annotations

import logging
import math
import random
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Smirnov (1973) Three-Step Kelly
# ────���────────────────────────────────────────────────────────────────

def smirnov_kelly(bins: list[dict]) -> list[dict]:
    """Compute optimal Kelly fractions for mutually exclusive bins.

    Each bin dict must have: p_win (model probability), c_market (market price).
    Returns bins that have positive edge with computed f_star fractions.

    Three steps:
    1. Filter by edge ratio e_i = p_i / c_i > 1.0
    2. Determine optimal bet set S via reserve rate
    3. Compute f_i* for each bin in S
    """
    # Step 1: Filter and sort by edge ratio
    candidates = []
    for b in bins:
        p = b["p_win"]
        c = b["c_market"]
        if c > 0 and p > 0:
            e = p / c
            if e > 1.0:
                candidates.append({**b, "edge_ratio": e})

    candidates.sort(key=lambda x: x["edge_ratio"], reverse=True)

    if not candidates:
        return []

    # Step 2: Determine optimal bet set S
    S: list[dict] = []
    sum_p_S = 0.0
    sum_c_S = 0.0

    for cand in candidates:
        p_i = cand["p_win"]
        c_i = cand["c_market"]

        denom = 1.0 - sum_c_S
        if denom <= 0:
            break

        reserve_rate = (1.0 - sum_p_S) / denom
        if cand["edge_ratio"] > reserve_rate:
            S.append(cand)
            sum_p_S += p_i
            sum_c_S += c_i
        else:
            break

    if not S:
        return []

    # Step 3: Compute f_i* for each bin in S
    total_p = sum(b["p_win"] for b in S)
    total_c = sum(b["c_market"] for b in S)
    remainder_ratio = (1.0 - total_p) / max(1.0 - total_c, 1e-9)

    results = []
    for b in S:
        f_star = b["p_win"] - b["c_market"] * remainder_ratio
        f_star = max(0.0, f_star)
        results.append({**b, "f_star": f_star})

    return results


# ─────────────────────────────────────────────────────────────────────
# Φ(BSS) Continuous Scaling
# ─────────────────────────────────────────────────────────────────────

def compute_phi_bss(bss: float) -> float:
    """Continuous scaling: Φ = clip(BSS / phi_bss_cap, phi_min, 1.0)."""
    phi_cap = get_param_float("kelly.phi_bss_cap")
    phi_min = get_param_float("kelly.phi_min")

    phi = min(1.0, bss / max(phi_cap, 1e-6))
    return max(phi_min, phi)


# ─────────────────────────────────────────────────────────────────────
# Drawdown Scale
# ─────────────────────────────────────────────────────────────────────

def compute_drawdown_scale(mdd: float) -> float:
    """D_scale = max(0, 1 - (MDD - MDD_safe) / (MDD_halt - MDD_safe)).

    MDD < safe: 1.0 (full sizing)
    MDD = midpoint: 0.5
    MDD ≥ halt: 0.0 (HALT all betting)
    """
    mdd_safe = get_param_float("drawdown.mdd_safe")
    mdd_halt = get_param_float("drawdown.mdd_halt")

    if mdd <= mdd_safe:
        return 1.0
    if mdd >= mdd_halt:
        return 0.0

    return max(0.0, 1.0 - (mdd - mdd_safe) / (mdd_halt - mdd_safe))


# ─────────────────────────────────────────────────────────────────────
# Market Convergence
# ─────────────────────────────────────────────────────────────────────

def compute_market_convergence(market_prices: dict[str, float], top_bin: str) -> tuple[float, float]:
    """Γ = P_market(top_bin) / max_j≠top P_market(j).

    Returns (gamma, scale_factor).
    Scale factor < 1.0 if Γ < gamma_threshold.
    """
    gamma_thresh = get_param_float("market.gamma_threshold")

    if not market_prices or top_bin not in market_prices:
        return 1.0, 1.0

    p_top = market_prices[top_bin]
    others = [v for k, v in market_prices.items() if k != top_bin and v > 0]
    if not others:
        return 1.0, 1.0

    max_other = max(others)
    if max_other <= 0:
        return 1.0, 1.0

    gamma = p_top / max_other

    if gamma < gamma_thresh:
        scale = gamma / gamma_thresh
    else:
        scale = 1.0

    return round(gamma, 4), round(scale, 4)


# ─────────────────────────────────────────────────────────────────────
# Full Sizing Chain
# ─────────────────────────────────────────────────────────────────────

def full_sizing_chain(
    f_star: float,
    bss: float,
    ibe_composite: float,
    gamma_scale: float,
    mdd: float,
    bankroll: float,
    remaining_capacity: float,
    c_market: float,
) -> dict:
    """Apply the 8-step sizing chain from raw Kelly fraction to final contracts.

    Steps:
    1. Kelly cap
    2. Φ(BSS) scaling
    3. IBE scaling
    4. Market convergence
    5. Position cap
    6. Drawdown scale
    7. Jitter
    8. Minimum check + round to contracts
    """
    fraction_cap = get_param_float("kelly.fraction_cap")
    min_bet_frac = get_param_float("kelly.min_bet_fraction")
    jitter_pct = get_param_float("kelly.jitter_pct")

    # Step 1: Kelly cap
    f = min(f_star, fraction_cap)

    # Step 2: Φ(BSS) scaling
    phi = compute_phi_bss(bss)
    f *= phi

    # Step 3: IBE scaling
    f *= ibe_composite

    # Step 4: Market convergence
    f *= gamma_scale

    # Step 5: Position cap
    f = min(f, remaining_capacity)

    # Step 6: Drawdown scale
    d_scale = compute_drawdown_scale(mdd)
    f *= d_scale

    # Step 7: Jitter
    jitter = 1.0 + random.uniform(-jitter_pct, jitter_pct)
    f *= jitter

    # Step 8: Minimum check + round
    if f < min_bet_frac:
        return {
            "f_final": 0.0, "contracts": 0, "skip": True,
            "reason": "below_minimum", "d_scale": d_scale, "phi": phi,
        }

    dollar_amount = f * bankroll
    contracts = int(dollar_amount / max(c_market, 0.01))

    if contracts < 1:
        return {
            "f_final": 0.0, "contracts": 0, "skip": True,
            "reason": "below_one_contract", "d_scale": d_scale, "phi": phi,
        }

    return {
        "f_star": round(f_star, 6),
        "f_final": round(f, 6),
        "contracts": contracts,
        "skip": False,
        "d_scale": round(d_scale, 4),
        "phi": round(phi, 4),
    }
