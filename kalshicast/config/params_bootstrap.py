"""All tunable parameters with bootstrap defaults.

At runtime, config.load_params() reads the PARAMS table and overrides these
defaults. Python source files use get_param() which falls back to this file
if the DB is unavailable.

Every parameter from v10 spec Section 3 is listed here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_DB_OVERRIDES: dict[str, str] = {}


@dataclass(frozen=True)
class ParamDef:
    key: str
    default: str
    dtype: str          # int, float, str, json
    description: str


PARAM_DEFS: list[ParamDef] = [
    # --- Pipeline / Orchestration ---
    ParamDef("pipeline.morning_utc_hour", "12", "int", "Morning run scheduled UTC hour"),
    ParamDef("pipeline.night_utc_hour", "6", "int", "Night run scheduled UTC hour"),
    ParamDef("pipeline.market_open_utc_hour", "14", "int", "Market-open run scheduled UTC hour"),
    ParamDef("pipeline.amendment_lookback_days", "3", "int", "Re-fetch CLI observations for this many past days"),
    ParamDef("pipeline.health_heartbeat_sec", "300", "int", "Health check interval"),
    ParamDef("pipeline.max_workers", "10", "int", "ThreadPoolExecutor worker count"),
    ParamDef("pipeline.forecast_days", "4", "int", "Days of forecast to collect per run"),

    # --- Lead Time Brackets ---
    ParamDef("lead.h1_max", "12.0", "float", "h1: [0, h1_max) hours"),
    ParamDef("lead.h2_max", "24.0", "float", "h2: [h1_max, h2_max) hours"),
    ParamDef("lead.h3_max", "48.0", "float", "h3: [h2_max, h3_max) hours"),
    ParamDef("lead.h4_max", "72.0", "float", "h4: [h3_max, h4_max) hours"),
    ParamDef("lead.h5_max", "120.0", "float", "h5: [h4_max, h5_max) hours"),

    # --- Lead-time Anchor Hours ---
    ParamDef("lead.target_local_hour_high", "15", "int", "~3pm local — expected hour of daily high"),
    ParamDef("lead.target_local_hour_low", "7", "int", "~7am local — expected hour of daily low"),

    # --- Collection ---
    ParamDef("collection.max_retry_attempts", "5", "int", "Max retry attempts per collector call"),
    ParamDef("collection.base_sleep_seconds", "0.75", "float", "Base sleep for jittered exponential backoff"),
    ParamDef("collection.tom_concurrency", "1", "int", "Tomorrow.io concurrent requests"),
    ParamDef("collection.wapi_concurrency", "2", "int", "WeatherAPI concurrent requests"),
    ParamDef("collection.vcr_concurrency", "2", "int", "Visual Crossing concurrent requests"),
    ParamDef("collection.nws_concurrency", "4", "int", "NWS concurrent requests"),
    ParamDef("collection.ome_concurrency", "2", "int", "Open-Meteo concurrent requests"),
    ParamDef("collection.daily_batch_size", "500", "int", "Rows per MERGE batch for FORECASTS_DAILY"),
    ParamDef("collection.hourly_batch_size", "200", "int", "Rows per MERGE batch for FORECASTS_HOURLY"),

    # --- Open-Meteo Knobs ---
    ParamDef("ome.timeout_connect", "5.0", "float", "OME connect timeout seconds"),
    ParamDef("ome.timeout_read", "20.0", "float", "OME read timeout seconds"),
    ParamDef("ome.max_inflight", "4", "int", "OME host-level concurrency cap"),
    ParamDef("ome.max_attempts", "3", "int", "OME per-request retry attempts"),
    ParamDef("ome.backoff_base_s", "0.6", "float", "OME backoff base seconds"),

    # --- Ensemble Aggregation (L2) ---
    ParamDef("ensemble.w_m_min_factor", "0.05", "float", "w_m = max(w_m_min_factor / M, BSS_m)"),
    ParamDef("ensemble.entropy_lambda", "0.10", "float", "Entropy regularization lambda"),
    ParamDef("ensemble.staleness_tau", "3.0", "float", "Staleness decay tau (days)"),
    ParamDef("ensemble.k_spread", "0.50", "float", "sigma_eff = sqrt(sigma^2 + k_spread * S^2)"),
    ParamDef("ensemble.min_models", "3", "int", "Skip ensemble if fewer than this"),
    ParamDef("ensemble.ewm_span", "90", "int", "EWM span for Q_base"),

    # --- Kalman Filter (L2) ---
    ParamDef("kalman.R_default", "0.25", "float", "Base measurement noise (F^2)"),
    ParamDef("kalman.beta", "2.0", "float", "R_k inflation when top model disagrees"),
    ParamDef("kalman.gamma", "0.10", "float", "Q_k asymmetric innovation penalty"),
    ParamDef("kalman.lambda_asym", "1.5", "float", "Cold bias amplification factor"),
    ParamDef("kalman.Q_window_days", "180", "int", "Rolling window for Q_base"),
    ParamDef("kalman.gap_inflate_factor", "2.0", "float", "U_k inflation on state_version gap"),
    ParamDef("kalman.B_init", "0.0", "float", "Initial bias estimate (F)"),
    ParamDef("kalman.U_init", "4.0", "float", "Initial uncertainty (F^2)"),
    ParamDef("kalman.amendment_lookback_days", "7", "int", "Max days back for retroactive Kalman replay"),

    # --- Regime Detection ---
    ParamDef("regime.bimodal_iqr_threshold", "1.35", "float", "IQR/S threshold to trigger bimodal detection"),
    ParamDef("regime.min_centroid_dist", "1.5", "float", "Min centroid distance as multiple of S"),
    ParamDef("regime.min_cluster_frac", "0.15", "float", "Min fraction of smaller cluster"),

    # --- Sigma and Skewness (L2) ---
    ParamDef("sigma.m_prior", "20", "int", "Bayesian shrinkage prior weight"),
    ParamDef("sigma.min_samples", "10", "int", "Min N before per-station sigma"),
    ParamDef("sigma.rmse_window_days", "90", "int", "Rolling window for RMSE"),
    ParamDef("skewness.significance_factor", "2.0", "float", "Fallback if |G1| < factor * sqrt(6/N)"),

    # --- Pricing / Skew-Normal (L3) ---
    ParamDef("pricing.alpha_cap", "10.0", "float", "Clip alpha_s to [-cap, +cap]"),
    ParamDef("pricing.p_min_floor", "0.001", "float", "P(win) floor to prevent log(0)"),
    ParamDef("pricing.bimodal_iqr_threshold", "1.35", "float", "Trigger bimodal when IQR/S > threshold"),
    ParamDef("pricing.bimodal_centroid_min", "1.0", "float", "Confirm bimodal if centroid dist > factor * S"),

    # --- METAR Truncation (L3) ---
    ParamDef("metar.truncation_enabled", "1", "int", "Master switch for intraday truncation"),
    ParamDef("metar.lead_hours_cutoff", "6.0", "float", "Only truncate when lead_hours <= cutoff"),
    ParamDef("metar.staleness_minutes", "120", "int", "Ignore METAR obs older than this"),

    # --- Conviction Gates (L4) ---
    ParamDef("gate.ev_min_fraction", "0.025", "float", "Min EV_net as fraction of bankroll"),
    ParamDef("gate.epsilon_edge_base", "0.03", "float", "Base edge buffer"),
    ParamDef("gate.spread_max", "4.0", "float", "Reject if S > spread_max (F)"),
    ParamDef("gate.bss_enter", "0.07", "float", "Skill Gate entry threshold"),
    ParamDef("gate.bss_exit", "0.03", "float", "Skill Gate exit threshold"),
    ParamDef("gate.lead_ceiling_hours", "72.0", "float", "Max lead hours for bets"),

    # --- Kelly Sizing (L4) ---
    ParamDef("kelly.fraction_cap", "0.10", "float", "Max single-bet Kelly fraction"),
    ParamDef("kelly.min_bet_fraction", "0.025", "float", "Min bet as fraction of bankroll"),
    ParamDef("kelly.jitter_pct", "0.03", "float", "Uniform jitter +/- pct"),
    ParamDef("kelly.phi_bss_cap", "0.25", "float", "BSS value where Phi=1.0"),
    ParamDef("kelly.phi_min", "0.10", "float", "Min Phi above Skill Gate"),

    # --- Position Limits (L4) ---
    ParamDef("position.max_single_fraction", "0.10", "float", "Max single position / bankroll"),
    ParamDef("position.max_station_fraction", "0.25", "float", "Max exposure per station / bankroll"),
    ParamDef("position.max_correlated_fraction", "0.40", "float", "Max correlated station exposure / bankroll"),
    ParamDef("position.max_total_fraction", "0.80", "float", "Max total portfolio / bankroll"),
    ParamDef("position.max_station_day_fraction", "0.10", "float", "Max new exposure per station per day / bankroll"),

    # --- VWAP and Order (L4) ---
    ParamDef("vwap.depth_levels", "5", "int", "Order book depth levels for VWAP"),
    ParamDef("vwap.staleness_delta", "0.05", "float", "Alert if |c_VWAP - c_best| > delta"),
    ParamDef("vwap.tranche_threshold", "50", "int", "Split orders > this into tranches"),
    ParamDef("vwap.tranche_size", "25", "int", "Contracts per tranche"),
    ParamDef("vwap.tranche_delay_sec", "5", "int", "Seconds between tranches"),
    ParamDef("order.maker_timeout_sec", "300", "int", "Cancel unfilled maker after this"),
    ParamDef("order.retry_max", "3", "int", "Max API submission retries"),
    ParamDef("order.retry_backoff_sec", "2.0", "float", "Exponential backoff base"),

    # --- IBE Signals (L4) ---
    ParamDef("ibe.kcv_lookback_days", "7", "int", "KCV lookback days"),
    ParamDef("ibe.kcv_norm_window", "90", "int", "KCV normalization trailing window"),
    ParamDef("ibe.kcv_veto_threshold", "4.0", "float", "Veto if KCV_normalized > threshold"),
    ParamDef("ibe.mpds_veto_threshold", "0.12", "float", "Veto if |MPDS| > threshold"),
    ParamDef("ibe.mpds_positive_scale", "5.0", "float", "MPDS modifier scale positive"),
    ParamDef("ibe.mpds_negative_scale", "8.0", "float", "MPDS modifier scale negative"),
    ParamDef("ibe.hmas_consensus_f", "1.0", "float", "HMAS consensus threshold (F)"),
    ParamDef("ibe.fct_veto_threshold", "1.5", "float", "Veto if FCT > threshold"),
    ParamDef("ibe.scas_scale", "0.15", "float", "SCAS modifier scale"),
    ParamDef("ibe.composite_weights", "[0.25, 0.35, 0.15, 0.15, 0.10]", "json", "Exponent weights"),
    ParamDef("ibe.composite_clip_low", "0.25", "float", "f_conviction >= clip_low * f_final"),
    ParamDef("ibe.composite_clip_high", "1.50", "float", "f_conviction <= clip_high * f_final"),

    # --- Fees (L4) ---
    ParamDef("fee.taker_rate", "0.07", "float", "Kalshi taker fee rate"),
    ParamDef("fee.maker_rate", "0.0175", "float", "Kalshi maker fee rate"),
    ParamDef("fee.maker_fill_prob_h_half", "24.0", "float", "P_fill half-life hours"),

    # --- Market Convergence (L4) ---
    ParamDef("market.gamma_threshold", "0.75", "float", "Gamma convergence threshold"),
    ParamDef("market.ev_rho", "0.50", "float", "EV_threshold lead-time scaling"),

    # --- Drawdown (L4) ---
    ParamDef("drawdown.mdd_safe", "0.10", "float", "D_scale begins shrinking at this MDD"),
    ParamDef("drawdown.mdd_halt", "0.20", "float", "D_scale = 0 at this MDD"),

    # --- Evaluation (L5) ---
    ParamDef("eval.bss_window_days", "90", "int", "Rolling window for BSS matrix"),
    ParamDef("eval.calibration_buckets", "10", "int", "Probability buckets for CAL"),
    ParamDef("eval.sharpe_min_bets", "20", "int", "Min bets before Sharpe meaningful"),
    ParamDef("eval.adverse_selection_window", "90", "int", "Days for adverse selection test"),
    ParamDef("eval.pattern_check_interval_days", "7", "int", "BSS pattern classifier frequency"),
]

_DEFAULTS: dict[str, ParamDef] = {p.key: p for p in PARAM_DEFS}


def load_db_overrides(overrides: dict[str, str]) -> None:
    """Called once after DB connection to inject PARAMS table values."""
    _DB_OVERRIDES.update(overrides)


def get_param(key: str) -> str:
    """Return parameter value: DB override > bootstrap default."""
    if key in _DB_OVERRIDES:
        return _DB_OVERRIDES[key]
    p = _DEFAULTS.get(key)
    if p is None:
        raise KeyError(f"Unknown parameter: {key}")
    return p.default


def get_param_int(key: str) -> int:
    return int(get_param(key))


def get_param_float(key: str) -> float:
    return float(get_param(key))
