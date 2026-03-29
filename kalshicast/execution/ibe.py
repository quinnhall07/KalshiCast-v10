"""5 IBE signals + composite — Intelligent Bet Evaluation.

Spec §7.3: KCV, MPDS, HMAS, FCT, SCAS → composite modifier with veto tier.
All signal functions are pure (conn only for data reads); DB writes in pipeline.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float, get_param_int, get_param

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Signal 1: KCV — Kalman Convergence Velocity
# ─────────────────────────────────────────────────────────────────────

def compute_kcv(
    conn: Any,
    station_id: str,
    target_type: str,
) -> dict:
    """KCV = |B_k - B_{k-lookback}| / lookback_days, normalized by 90-day mean.

    Veto if KCV_normalized > kcv_veto_threshold (4.0).
    """
    lookback = get_param_int("ibe.kcv_lookback_days")
    norm_window = get_param_int("ibe.kcv_norm_window")
    veto_thresh = get_param_float("ibe.kcv_veto_threshold")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT B_K, CREATED_AT
            FROM KALMAN_HISTORY
            WHERE STATION_ID = :sid AND TARGET_TYPE = :tt
            ORDER BY CREATED_AT DESC
            FETCH FIRST :n ROWS ONLY
        """, {"sid": station_id, "tt": target_type, "n": norm_window})
        rows = [(float(r[0]) if r[0] is not None else 0.0, r[1]) for r in cur]

    if len(rows) < 2:
        return {"kcv_norm": 0.0, "kcv_mod": 1.0, "veto": False, "reason": "insufficient_data"}

    b_current = rows[0][0]
    b_lookback = rows[min(lookback, len(rows) - 1)][0]
    kcv_k = abs(b_current - b_lookback) / max(lookback, 1)

    # Normalize by trailing mean KCV
    kcv_values = []
    for i in range(len(rows) - 1):
        step = min(lookback, len(rows) - 1 - i)
        if step > 0:
            kcv_i = abs(rows[i][0] - rows[i + step][0]) / step
            kcv_values.append(kcv_i)

    mean_kcv = sum(kcv_values) / len(kcv_values) if kcv_values else 1.0
    kcv_norm = kcv_k / max(mean_kcv, 1e-6)

    # Modifier
    kcv_mod = max(0.5, 1.0 - 0.25 * (kcv_norm - 1.0))
    kcv_mod = min(kcv_mod, 1.25)

    veto = kcv_norm > veto_thresh

    return {
        "kcv_norm": round(kcv_norm, 4),
        "kcv_mod": round(kcv_mod, 4),
        "veto": veto,
        "reason": "KCV veto" if veto else None,
    }


# ─────────────────────────────────────────────────────────────────────
# Signal 2: MPDS — Model-Price Divergence Signal
# ─────────────────────────────────────────────────────────────────────

def compute_mpds(
    p_current: float,
    p_previous: float | None,
    c_current: float,
    c_previous: float | None,
) -> dict:
    """MPDS = (c_current - c_previous) - (p_current - p_previous).

    Positive MPDS = market converging to us (good).
    Negative MPDS = market diverging from us (bad).
    Veto if |MPDS| > mpds_veto_threshold.
    """
    veto_thresh = get_param_float("ibe.mpds_veto_threshold")
    pos_scale = get_param_float("ibe.mpds_positive_scale")
    neg_scale = get_param_float("ibe.mpds_negative_scale")

    if p_previous is None or c_previous is None:
        return {"mpds_k": 0.0, "mpds_mod": 1.0, "veto": False, "reason": "no_previous"}

    delta_p = p_current - p_previous
    delta_c = c_current - c_previous
    mpds_k = delta_c - delta_p

    # Modifier
    if mpds_k >= 0:
        mpds_mod = max(0.5, 1.0 - abs(mpds_k) * pos_scale)
    else:
        mpds_mod = max(0.3, 1.0 - abs(mpds_k) * neg_scale)

    veto = abs(mpds_k) > veto_thresh

    return {
        "mpds_k": round(mpds_k, 6),
        "mpds_mod": round(mpds_mod, 4),
        "veto": veto,
        "reason": "MPDS veto" if veto else None,
    }


# ─────────────────────────────────────────────────────────────────────
# Signal 3: HMAS — Historical Model Agreement Score
# ─────────────────────────────────────────────────────────────────────

def compute_hmas(
    conn: Any,
    station_id: str,
    lead_bracket: str,
    target_type: str,
    target_date: str,
    f_bar: float,
) -> dict:
    """HMAS = fraction of BSS-qualified models within consensus_f of ensemble mean.

    HMAS_mod = 0.7 + 0.6 × HMAS. Range: [0.7, 1.3]. No veto.
    """
    consensus_f = get_param_float("ibe.hmas_consensus_f")
    bss_exit = get_param_float("gate.bss_exit")

    # Get latest forecasts for this station/date/type
    with conn.cursor() as cur:
        cur.execute("""
            SELECT fd.SOURCE_ID, fd.HIGH_F, fd.LOW_F
            FROM FORECASTS_DAILY fd
            WHERE fd.STATION_ID = :sid
              AND fd.TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD')
            ORDER BY fd.CREATED_AT DESC
        """, {"sid": station_id, "td": target_date})
        forecasts = {}
        for row in cur:
            src = row[0]
            if src not in forecasts:
                val = float(row[1]) if target_type == "HIGH" and row[1] else (
                    float(row[2]) if target_type == "LOW" and row[2] else None)
                if val is not None:
                    forecasts[src] = val

    if not forecasts:
        return {"hmas": 0.0, "hmas_mod": 0.7, "veto": False}

    # Filter to BSS-qualified models
    qualified = []
    for src_id, f_val in forecasts.items():
        with conn.cursor() as cur:
            cur.execute("""
                SELECT BSS_1, IS_QUALIFIED FROM BSS_MATRIX
                WHERE STATION_ID = :sid AND TARGET_TYPE = :tt AND LEAD_BRACKET = :lb
            """, {"sid": station_id, "tt": target_type, "lb": lead_bracket})
            row = cur.fetchone()
            if row and row[1] and row[0] is not None and float(row[0]) > bss_exit:
                qualified.append(f_val)

    if not qualified:
        return {"hmas": 0.0, "hmas_mod": 0.7, "veto": False}

    in_consensus = sum(1 for f in qualified if abs(f - f_bar) <= consensus_f)
    hmas = in_consensus / len(qualified)

    hmas_mod = 0.7 + 0.6 * hmas

    return {
        "hmas": round(hmas, 4),
        "hmas_mod": round(hmas_mod, 4),
        "veto": False,
    }


# ─────────────────────────────────────────────────────────────────────
# Signal 4: FCT — Forecast Convergence Tracker
# ─────────────────────────────────────────────────────────────────────

def compute_fct(
    s_current: float,
    s_previous: float | None,
    sigma_hist: float,
) -> dict:
    """FCT = (S_current - S_previous) / σ_hist.

    Negative FCT = converging (good). Positive FCT = diverging (bad).
    Veto if FCT > fct_veto_threshold.
    """
    veto_thresh = get_param_float("ibe.fct_veto_threshold")

    if s_previous is None or sigma_hist <= 0:
        return {"fct": 0.0, "fct_mod": 1.0, "veto": False, "reason": "no_previous"}

    s_delta = s_current - s_previous
    fct = s_delta / sigma_hist

    # Modifier
    if fct < 0:
        fct_mod = min(1.4, 1.0 - fct * 0.4)
    else:
        fct_mod = max(0.5, 1.0 - fct * 0.6)

    veto = fct > veto_thresh

    return {
        "fct": round(fct, 4),
        "fct_mod": round(fct_mod, 4),
        "veto": veto,
        "reason": "FCT veto" if veto else None,
    }


# ─────────────────────────────────────────────────────────────────────
# Signal 5: SCAS — Seasonal Climatological Anomaly Score
# ─────────────────────────────────────────────────────────────────────

def compute_scas(
    conn: Any,
    station_id: str,
    target_type: str,
    b_k: float,
    sigma_hist: float,
) -> dict:
    """SCAS = |B_seasonal - B_k| / σ_hist.

    B_seasonal = rolling 15-day mean of historical Kalman biases for this DOY.
    SCAS_mod = max(0.6, 1 - scas_scale × SCAS). No veto.
    """
    scas_scale = get_param_float("ibe.scas_scale")

    # Get historical bias for seasonal comparison
    with conn.cursor() as cur:
        cur.execute("""
            SELECT AVG(B_K) FROM KALMAN_HISTORY
            WHERE STATION_ID = :sid AND TARGET_TYPE = :tt
              AND CREATED_AT >= SYSTIMESTAMP - INTERVAL '365' DAY
        """, {"sid": station_id, "tt": target_type})
        row = cur.fetchone()
        b_seasonal = float(row[0]) if row and row[0] is not None else 0.0

    if sigma_hist <= 0:
        return {"scas": 0.0, "scas_mod": 1.0, "veto": False}

    scas = abs(b_seasonal - b_k) / sigma_hist
    scas_mod = max(0.6, 1.0 - scas_scale * scas)

    return {
        "scas": round(scas, 4),
        "scas_mod": round(scas_mod, 4),
        "veto": False,
    }


# ─────────────────────────────────────────────────────────────────────
# Composite
# ─────────────────────────────────────────────────────────────────────

def compute_composite(mods: list[float], weights: list[float]) -> float:
    """Geometric weighted product, clipped to [clip_low, clip_high].

    COMPOSITE = Π(mod_i ^ weight_i), clipped.
    """
    clip_low = get_param_float("ibe.composite_clip_low")
    clip_high = get_param_float("ibe.composite_clip_high")

    if not mods or not weights:
        return 1.0

    log_composite = sum(w * math.log(max(m, 1e-6)) for m, w in zip(mods, weights))
    composite = math.exp(log_composite)
    return max(clip_low, min(clip_high, composite))


def evaluate_ibe(conn: Any, candidate: dict) -> dict:
    """Run all 5 IBE signals and compute composite.

    candidate keys: station_id, target_type, lead_bracket, target_date,
                    p_win, p_previous, c_market, c_previous, f_bar,
                    s_tk, s_previous, sigma_hist, b_k

    Returns: {kcv_norm, kcv_mod, mpds_k, mpds_mod, hmas, hmas_mod,
              fct, fct_mod, scas, scas_mod, composite, veto, veto_reason}
    """
    # Compute all 5 signals
    kcv = compute_kcv(conn, candidate["station_id"], candidate["target_type"])

    mpds = compute_mpds(
        candidate["p_win"],
        candidate.get("p_previous"),
        candidate["c_market"],
        candidate.get("c_previous"),
    )

    hmas = compute_hmas(
        conn,
        candidate["station_id"],
        candidate["lead_bracket"],
        candidate["target_type"],
        candidate["target_date"],
        candidate["f_bar"],
    )

    fct = compute_fct(
        candidate["s_tk"],
        candidate.get("s_previous"),
        candidate.get("sigma_hist", 3.0),
    )

    scas = compute_scas(
        conn,
        candidate["station_id"],
        candidate["target_type"],
        candidate.get("b_k", 0.0),
        candidate.get("sigma_hist", 3.0),
    )

    # Check veto tier first
    veto = False
    veto_reason = None
    for signal in [kcv, mpds, fct]:
        if signal.get("veto"):
            veto = True
            veto_reason = signal.get("reason")
            break

    # Compute composite (even if veto — logged for analysis)
    weights_str = get_param("ibe.composite_weights")
    weights = json.loads(weights_str)
    mods = [kcv["kcv_mod"], mpds["mpds_mod"], hmas["hmas_mod"],
            fct["fct_mod"], scas["scas_mod"]]
    composite = compute_composite(mods, weights)

    return {
        "kcv_norm": kcv["kcv_norm"],
        "kcv_mod": kcv["kcv_mod"],
        "mpds_k": mpds["mpds_k"],
        "mpds_mod": mpds["mpds_mod"],
        "hmas": hmas["hmas"],
        "hmas_mod": hmas["hmas_mod"],
        "fct": fct["fct"],
        "fct_mod": fct["fct_mod"],
        "scas": scas["scas"],
        "scas_mod": scas["scas_mod"],
        "composite": round(composite, 4),
        "veto": veto,
        "veto_reason": veto_reason,
    }
