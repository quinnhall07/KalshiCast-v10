"""Kalman filter — 1D bias tracker per (station, target_type).

40 independent filters (20 stations × 2 types).
Spec §5.6: predict → R_k → Q_k → K_k → update.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float, get_param_int

log = logging.getLogger(__name__)


@dataclass
class KalmanState:
    b_k: float = 0.0           # Bias estimate (°F)
    u_k: float = 4.0           # Estimate variance (°F²)
    q_base: float = 0.0        # EWM process noise base
    state_version: int = 1
    top_model_id: str | None = None
    last_observation_date: date | None = None


def init_kalman_state() -> KalmanState:
    """Initialize with spec defaults: B_0=0.0, U_0=4.0, version=1."""
    return KalmanState(
        b_k=get_param_float("kalman.B_init"),
        u_k=get_param_float("kalman.U_init"),
    )


def compute_R_k(f_top: float, f_bar: float, s_tk: float) -> float:
    """Dynamic measurement noise. §5.6.3.

    R_k = R_default × (1 + β × max(0, |F_top - F̄| / S - 1))
    Inflates when top model deviates from ensemble mean.
    """
    R_default = get_param_float("kalman.R_default")
    beta = get_param_float("kalman.beta")

    if s_tk <= 0:
        return R_default

    deviation_ratio = abs(f_top - f_bar) / s_tk
    return R_default * (1.0 + beta * max(0.0, deviation_ratio - 1.0))


def compute_Q_k(q_base: float, recent_innovations: list[float],
                b_prev: float) -> float:
    """Process noise with asymmetric innovation penalty. §5.6.4.

    Q_k = Q_base + γ × Σ v_j
    Where v_j is asymmetric: cold bias penalized more (λ_asym).
    """
    gamma = get_param_float("kalman.gamma")
    lambda_asym = get_param_float("kalman.lambda_asym")

    innovation_sum = 0.0
    for eps in recent_innovations[-5:]:  # max 5 recent
        d = eps - b_prev  # innovation residual
        if d >= 0:
            innovation_sum += d * d
        else:
            innovation_sum += lambda_asym * d * d

    return q_base + gamma * innovation_sum


def kalman_update(
    state: KalmanState,
    epsilon_k: float,
    R_k: float,
    Q_k: float,
    gap_days: int = 0,
) -> tuple[KalmanState, dict]:
    """Full 5-step Kalman update. Returns (new_state, history_row).

    Steps:
    1. Predict: B_k⁻ = B_{k-1}, U_k⁻ = U_{k-1} + Q_k (+ gap inflation)
    2. Kalman gain: K_k = U_k⁻ / (U_k⁻ + R_k)
    3. Update: B_k = B_k⁻ + K_k × (ε_k - B_k⁻)
    4. Uncertainty: U_k = (1 - K_k) × U_k⁻
    5. Version increment
    """
    # Step 1: Predict
    b_prior = state.b_k
    u_prior = state.u_k + Q_k

    # Gap inflation if days were missed
    if gap_days > 0:
        gap_factor = get_param_float("kalman.gap_inflate_factor")
        u_prior += gap_factor * Q_k * gap_days

    # Step 2: Kalman gain
    if u_prior + R_k == 0:
        k_k = 0.5
    else:
        k_k = u_prior / (u_prior + R_k)

    # Step 3: Update bias
    b_new = b_prior + k_k * (epsilon_k - b_prior)

    # Step 4: Update uncertainty
    u_new = (1.0 - k_k) * u_prior

    # Step 5: New state
    new_state = KalmanState(
        b_k=b_new,
        u_k=u_new,
        q_base=state.q_base,  # Updated separately via EWM
        state_version=state.state_version + 1,
        top_model_id=state.top_model_id,
        last_observation_date=state.last_observation_date,
    )

    history = {
        "b_k": b_new,
        "u_k": u_new,
        "q_k": Q_k,
        "r_k": R_k,
        "k_k": k_k,
        "epsilon_k": epsilon_k,
        "state_version": new_state.state_version,
        "is_amendment": False,
    }

    return new_state, history


def _compute_ewm_variance(delta_b_series: list[float], span: int = 90) -> float:
    """Exponentially weighted moving variance of ΔB series for Q_base."""
    if len(delta_b_series) < 2:
        return 0.01  # small default

    alpha = 2.0 / (span + 1)
    mean = 0.0
    var = 0.0

    for i, x in enumerate(delta_b_series):
        if i == 0:
            mean = x
            var = 0.0
            continue
        diff = x - mean
        mean = mean + alpha * diff
        var = (1.0 - alpha) * (var + alpha * diff * diff)

    return max(var, 1e-6)


def update_kalman_filters(conn: Any, target_date: str, run_id: str) -> int:
    """Update all Kalman filters for a given target_date.

    For each (station, target_type):
    1. Read current state from KALMAN_STATES
    2. Get observation error (epsilon_k = error_raw = forecast - observed)
    3. Get ensemble spread for R_k computation
    4. Run kalman_update
    5. Persist to KALMAN_STATES + KALMAN_HISTORY

    Returns count of filters updated.
    """
    from kalshicast.db.operations import (
        get_kalman_state, upsert_kalman_state, insert_kalman_history,
        get_forecast_errors_window,
    )
    from kalshicast.config import get_stations

    stations = get_stations(active_only=True)
    updated = 0

    for st in stations:
        station_id = st["station_id"]

        for target_type in ("HIGH", "LOW"):
            # Read current state
            state_dict = get_kalman_state(conn, station_id, target_type)
            if state_dict is None:
                state = init_kalman_state()
            else:
                state = KalmanState(
                    b_k=state_dict["b_k"],
                    u_k=state_dict["u_k"],
                    q_base=state_dict.get("q_base", 0.0),
                    state_version=state_dict.get("state_version", 0),
                    top_model_id=state_dict.get("top_model_id"),
                    last_observation_date=state_dict.get("last_observation_date"),
                )

            # Get the top model's error for this date
            errors = get_forecast_errors_window(
                conn, station_id, state.top_model_id,
                target_type, "h2", 1,  # today's error, default h2 bracket
            )
            # Find the error row matching target_date
            target_err = None
            for e in errors:
                td = e.get("target_date")
                if td and str(td)[:10] == str(target_date)[:10]:
                    target_err = e
                    break

            if target_err is None or target_err.get("error_raw") is None:
                log.debug("[kalman] %s/%s: no error for %s, skipping",
                          station_id, target_type, target_date)
                continue

            epsilon_k = float(target_err["error_raw"])
            f_raw = float(target_err["f_raw"]) if target_err.get("f_raw") is not None else 0.0

            # Compute R_k (need ensemble spread — use 0 as default if not available)
            R_k = compute_R_k(f_raw, f_raw, 0.0)  # simplified: spread not yet available in night

            # Compute gap days
            gap_days = 0
            if state.last_observation_date:
                td_parsed = date.fromisoformat(str(target_date)[:10])
                delta = (td_parsed - state.last_observation_date).days - 1
                gap_days = max(0, delta)

            # Get recent innovations for Q_k
            window = get_param_int("sigma.rmse_window_days")
            recent_errors = get_forecast_errors_window(
                conn, station_id, state.top_model_id,
                target_type, "h2", min(window, 30),
            )
            recent_innovations = [
                float(e["error_raw"]) for e in recent_errors
                if e.get("error_raw") is not None
            ]

            Q_k = compute_Q_k(state.q_base, recent_innovations, state.b_k)

            # Run Kalman update
            new_state, history = kalman_update(state, epsilon_k, R_k, Q_k, gap_days)
            new_state.last_observation_date = date.fromisoformat(str(target_date)[:10])

            # Update Q_base via EWM of ΔB series
            if len(recent_innovations) >= 2:
                delta_b = [recent_innovations[i] - recent_innovations[i - 1]
                           for i in range(1, len(recent_innovations))]
                new_state.q_base = _compute_ewm_variance(delta_b)

            # Persist
            upsert_kalman_state(conn, station_id, target_type, {
                "b_k": new_state.b_k,
                "u_k": new_state.u_k,
                "q_base": new_state.q_base,
                "state_version": new_state.state_version,
                "top_model_id": new_state.top_model_id,
                "last_observation_date": new_state.last_observation_date,
            })
            history["station_id"] = station_id
            history["target_type"] = target_type
            history["pipeline_run_id"] = run_id
            insert_kalman_history(conn, history)

            updated += 1

    conn.commit()
    log.info("[kalman] updated %d filters for %s", updated, target_date)
    return updated


def retroactive_kalman_correction(conn: Any, amended_stations: list[tuple],
                                   run_id: str) -> int:
    """Replay Kalman from amended observation forward. §5.6.8.

    amended_stations: list of (station_id, target_date) tuples with amended obs.
    Limited to amendment_lookback_days.

    Algorithm:
    1. For each (station, amended_date), load KALMAN_HISTORY from that date forward
    2. Reset state to the day *before* the amendment
    3. Re-read corrected observations and replay each day sequentially
    4. Mark replayed history rows with is_amendment=True
    5. Persist final corrected state

    Returns count of replayed filters.
    """
    from kalshicast.db.operations import (
        get_kalman_state, upsert_kalman_state, insert_kalman_history,
        get_forecast_errors_window,
    )

    if not amended_stations:
        return 0

    lookback = get_param_int("kalman.amendment_lookback_days")
    replayed = 0

    for station_id, amended_date_str in amended_stations:
        amended_date = date.fromisoformat(str(amended_date_str)[:10])
        today = date.today()

        if (today - amended_date).days > lookback:
            log.info("[kalman] skipping %s/%s — older than %d day lookback",
                     station_id, amended_date, lookback)
            continue

        for target_type in ("HIGH", "LOW"):
            # 1. Load state snapshot from the day before amendment
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT B_K, U_K, Q_K, STATE_VERSION, HISTORY_DATE
                    FROM KALMAN_HISTORY
                    WHERE STATION_ID = :sid AND TARGET_TYPE = :tt
                      AND HISTORY_DATE < TO_DATE(:ad, 'YYYY-MM-DD')
                    ORDER BY HISTORY_DATE DESC
                    FETCH FIRST 1 ROWS ONLY
                """, {"sid": station_id, "tt": target_type,
                      "ad": amended_date.isoformat()})
                pre_row = cur.fetchone()

            if pre_row is None:
                state = init_kalman_state()
                replay_from = amended_date
            else:
                state = KalmanState(
                    b_k=float(pre_row[0]),
                    u_k=float(pre_row[1]),
                    q_base=float(pre_row[2]) if pre_row[2] else 0.0,
                    state_version=int(pre_row[3]) if pre_row[3] else 0,
                    last_observation_date=pre_row[4].date() if pre_row[4] else None,
                )
                replay_from = amended_date

            # 2. Get all history dates from amendment forward (to know which days to replay)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT TRUNC(HISTORY_DATE) AS HD
                    FROM KALMAN_HISTORY
                    WHERE STATION_ID = :sid AND TARGET_TYPE = :tt
                      AND HISTORY_DATE >= TO_DATE(:ad, 'YYYY-MM-DD')
                    ORDER BY HD
                """, {"sid": station_id, "tt": target_type,
                      "ad": amended_date.isoformat()})
                replay_dates = [row[0].date() for row in cur]

            if not replay_dates:
                replay_dates = [amended_date]

            # 3. Delete old history rows that will be replayed
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM KALMAN_HISTORY
                    WHERE STATION_ID = :sid AND TARGET_TYPE = :tt
                      AND HISTORY_DATE >= TO_DATE(:ad, 'YYYY-MM-DD')
                """, {"sid": station_id, "tt": target_type,
                      "ad": amended_date.isoformat()})

            # 4. Replay each day sequentially
            for replay_date in replay_dates:
                errors = get_forecast_errors_window(
                    conn, station_id, state.top_model_id,
                    target_type, "h2", 1,
                )
                target_err = None
                for e in errors:
                    td = e.get("target_date")
                    if td and str(td)[:10] == replay_date.isoformat():
                        target_err = e
                        break

                if target_err is None or target_err.get("error_raw") is None:
                    continue

                epsilon_k = float(target_err["error_raw"])
                f_raw = float(target_err["f_raw"]) if target_err.get("f_raw") else 0.0

                R_k = compute_R_k(f_raw, f_raw, 0.0)

                gap_days = 0
                if state.last_observation_date:
                    delta = (replay_date - state.last_observation_date).days - 1
                    gap_days = max(0, delta)

                recent_errors = get_forecast_errors_window(
                    conn, station_id, state.top_model_id,
                    target_type, "h2", 30,
                )
                recent_innovations = [
                    float(e["error_raw"]) for e in recent_errors
                    if e.get("error_raw") is not None
                ]

                Q_k = compute_Q_k(state.q_base, recent_innovations, state.b_k)

                state, history = kalman_update(state, epsilon_k, R_k, Q_k, gap_days)
                state.last_observation_date = replay_date
                history["is_amendment"] = True

                # Update Q_base via EWM
                if len(recent_innovations) >= 2:
                    delta_b = [recent_innovations[i] - recent_innovations[i - 1]
                               for i in range(1, len(recent_innovations))]
                    state.q_base = _compute_ewm_variance(delta_b)

                # Persist replayed history
                history["station_id"] = station_id
                history["target_type"] = target_type
                history["pipeline_run_id"] = run_id
                insert_kalman_history(conn, history)

            # 5. Persist final corrected state
            upsert_kalman_state(conn, station_id, target_type, {
                "b_k": state.b_k,
                "u_k": state.u_k,
                "q_base": state.q_base,
                "state_version": state.state_version,
                "top_model_id": state.top_model_id,
                "last_observation_date": state.last_observation_date,
            })

            replayed += 1
            log.info("[kalman] replayed %s/%s from %s (%d days), B_k=%.3f U_k=%.3f",
                     station_id, target_type, amended_date, len(replay_dates),
                     state.b_k, state.u_k)

    conn.commit()
    log.info("[kalman] retroactive correction complete: %d filters replayed", replayed)
    return replayed
