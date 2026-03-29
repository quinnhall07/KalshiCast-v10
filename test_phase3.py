"""Comprehensive unit tests for Phase 3 L4 Execution."""

print("=" * 60)
print("COMPREHENSIVE UNIT TESTS — Phase 3 L4 Execution")
print("=" * 60)

# --- 1. Conviction Gates ---
print("\n--- 1. Conviction Gates ---")

from kalshicast.execution.gates import (
    check_edge_gate, check_spread_gate, check_skill_gate,
    check_lead_gate, evaluate_all_gates,
)

r = check_edge_gate(p_win=0.65, c_market=0.45, bankroll=1000.0, n_bets=100)
assert r["pass"], f"Edge gate should pass: {r}"
print(f"  Edge (clear):  PASS  edge={r['edge']:.3f} ev_net={r['ev_net']:.1f}")

r = check_edge_gate(p_win=0.46, c_market=0.45, bankroll=1000.0, n_bets=100)
assert not r["pass"], f"Edge gate should fail: {r}"
print(f"  Edge (none):   FAIL  edge={r['edge']:.3f} (correct)")

r = check_spread_gate(2.5)
assert r["pass"]
r2 = check_spread_gate(5.0)
assert not r2["pass"]
print("  Spread (2.5):  PASS  Spread (5.0): FAIL (correct)")

r = check_skill_gate(0.08, was_qualified=False)
assert r["pass"]
r = check_skill_gate(0.05, was_qualified=False)
assert not r["pass"]
r = check_skill_gate(0.05, was_qualified=True)
assert r["pass"]
r = check_skill_gate(0.02, was_qualified=True)
assert not r["pass"]
print("  Skill hysteresis: enter@0.07 exit@0.03 -- all 4 cases correct")

r = check_lead_gate(48.0)
assert r["pass"]
r = check_lead_gate(80.0)
assert not r["pass"]
print("  Lead (48h):    PASS  Lead (80h): FAIL (correct)")

r = evaluate_all_gates({
    "p_win": 0.60, "c_market": 0.40, "bankroll": 1000, "n_bets": 50,
    "s_tk": 2.0, "bss": 0.15, "was_qualified": True, "lead_hours": 24.0,
})
assert r["pass"]
assert all(r["flags"].values())
print("  Full eval (good candidate): ALL PASS")

# --- 2. IBE Signals ---
print("\n--- 2. IBE Signals ---")

from kalshicast.execution.ibe import compute_mpds, compute_fct, compute_composite

r = compute_mpds(0.50, 0.48, 0.45, 0.44)
print(f"  MPDS: mpds_k={r['mpds_k']:.4f} mod={r['mpds_mod']:.4f} veto={r['veto']}")
assert not r["veto"]

r = compute_mpds(0.50, 0.48, 0.30, 0.44)
print(f"  MPDS (veto): mpds_k={r['mpds_k']:.4f} veto={r['veto']}")
assert r["veto"]

r = compute_fct(s_current=3.0, s_previous=2.5, sigma_hist=3.0)
print(f"  FCT: fct={r['fct']:.4f} mod={r['fct_mod']:.4f} veto={r['veto']}")
assert not r["veto"]

r = compute_fct(s_current=8.0, s_previous=2.0, sigma_hist=3.0)
print(f"  FCT (veto): fct={r['fct']:.4f} veto={r['veto']}")
assert r["veto"]

mods = [0.9, 0.8, 1.1, 1.0, 0.95]
weights = [0.25, 0.35, 0.15, 0.15, 0.10]
c = compute_composite(mods, weights)
print(f"  Composite: {c:.4f} (expected ~0.91)")
assert 0.85 < c < 0.95

mods_extreme = [0.1, 0.1, 0.1, 0.1, 0.1]
c = compute_composite(mods_extreme, weights)
print(f"  Composite (extreme low): {c:.4f} (clipped to 0.25)")
assert c == 0.25

# --- 3. Kelly Sizing ---
print("\n--- 3. Smirnov (1973) Kelly ---")

from kalshicast.execution.kelly import (
    smirnov_kelly, compute_phi_bss, compute_drawdown_scale,
    compute_market_convergence, full_sizing_chain,
)

bins = [
    {"p_win": 0.25, "c_market": 0.30, "ticker": "A"},
    {"p_win": 0.50, "c_market": 0.40, "ticker": "B"},
    {"p_win": 0.25, "c_market": 0.30, "ticker": "C"},
]
result = smirnov_kelly(bins)
assert len(result) == 1
assert result[0]["ticker"] == "B"
assert abs(result[0]["f_star"] - 0.1667) < 0.001
print(f"  3-bin: selected B, f*={result[0]['f_star']:.4f} (spec=0.1667)")

bins5 = [
    {"p_win": 0.10, "c_market": 0.15, "ticker": "1"},
    {"p_win": 0.30, "c_market": 0.20, "ticker": "2"},
    {"p_win": 0.25, "c_market": 0.18, "ticker": "3"},
    {"p_win": 0.20, "c_market": 0.22, "ticker": "4"},
    {"p_win": 0.15, "c_market": 0.25, "ticker": "5"},
]
result5 = smirnov_kelly(bins5)
total_f = sum(r["f_star"] for r in result5)
print(f"  5-bin: {len(result5)} selected, sum_f*={total_f:.4f} (<=1.0)")
assert total_f <= 1.0

assert compute_phi_bss(0.25) == 1.0
assert abs(compute_phi_bss(0.125) - 0.50) < 0.01
assert compute_phi_bss(0.01) == 0.10
print("  Phi(BSS): 0.25->1.0, 0.125->0.50, 0.01->0.10")

assert compute_drawdown_scale(0.05) == 1.0
assert abs(compute_drawdown_scale(0.15) - 0.50) < 0.01
assert compute_drawdown_scale(0.20) == 0.0
assert compute_drawdown_scale(0.25) == 0.0
print("  D_scale: 5%->1.0, 15%->0.5, 20%->0.0, 25%->0.0")

gamma, scale = compute_market_convergence({"A": 0.50, "B": 0.30, "C": 0.20}, "A")
print(f"  Gamma: {gamma:.4f}, scale: {scale:.4f}")
assert gamma > 1.0
assert scale == 1.0

sizing = full_sizing_chain(
    f_star=0.10, bss=0.20, ibe_composite=0.95, gamma_scale=1.0,
    mdd=0.05, bankroll=1000.0, remaining_capacity=0.25, c_market=0.45,
)
print(f"  Full chain: f_final={sizing['f_final']:.4f}, contracts={sizing['contracts']}, skip={sizing['skip']}")
assert not sizing["skip"]
assert sizing["contracts"] > 0

# --- 4. VWAP ---
print("\n--- 4. VWAP ---")

from kalshicast.execution.vwap import compute_vwap, check_staleness, split_tranches

book = {"yes": [
    {"price": 42, "quantity": 50},
    {"price": 43, "quantity": 30},
    {"price": 44, "quantity": 20},
]}
c_vwap, depth = compute_vwap(book, 60)
print(f"  VWAP(60 contracts): {c_vwap:.4f}, depth={depth}")
assert depth == 60

s = check_staleness(0.43, 0.42)
print(f"  Staleness: delta={s['delta']:.4f} alert={s['alert']} abort={s['abort']}")

t = split_tranches(10)
assert t == [10]
t = split_tranches(120)
assert sum(t) == 120
print(f"  Tranches: 10->{split_tranches(10)}, 120->{split_tranches(120)}")

# --- 5. Orders ---
print("\n--- 5. Order Logic ---")

from kalshicast.execution.orders import compute_ev_net, maker_or_taker

ev = compute_ev_net(0.60, 0.45, 0.07)
print(f"  EV_net (taker): {ev:.2f} cents")
assert ev > 0

decision = maker_or_taker(lead_hours=48.0, p_win=0.60, c_bid=0.43, c_ask=0.45)
print(f"  Maker/taker: {decision['order_type']} @ {decision['price']:.2f}, P_fill={decision['p_fill']:.3f}")

# --- 6. Position Limits ---
print("\n--- 6. Position Limits ---")

from kalshicast.execution.positions import check_single_limit

assert check_single_limit(0.05) == 0.05
assert check_single_limit(0.15) == 0.10
print("  Single limit: 0.05->0.05, 0.15->0.10 (capped)")

# --- 7. METAR Parsing ---
print("\n--- 7. METAR Parsing ---")

from kalshicast.collection.collectors.collect_metar import (
    _parse_temperature_f, _parse_dewpoint_f, _parse_wind,
)

t = _parse_temperature_f("KJFK 291456Z 18012G20KT 10SM SCT040 BKN250 22/14 A2998")
dp = _parse_dewpoint_f("KJFK 291456Z 18012G20KT 10SM SCT040 BKN250 22/14 A2998")
ws, wd = _parse_wind("KJFK 291456Z 18012G20KT 10SM SCT040 BKN250 22/14 A2998")
print(f"  Temp: {t}F (expect 71.6), Dew: {dp}F (expect 57.2)")
print(f"  Wind: {ws}kt @ {wd} deg")
assert abs(t - 71.6) < 0.1
assert abs(dp - 57.2) < 0.1
assert ws == 12
assert wd == 180

t = _parse_temperature_f("KORD 291456Z 31015KT M02/M08 A3012")
print(f"  Negative temp: {t}F (expect 28.4F = -2C)")
assert abs(t - 28.4) < 0.1

# --- 8. AFD Signal Extraction ---
print("\n--- 8. AFD Signal Extraction ---")

from kalshicast.collection.collectors.collect_afd import _extract_signals

sig = _extract_signals("High confidence in the forecast with models in good agreement.")
print(f"  High conf: {sig}")
assert sig["confidence_flag"] == "HIGH"
assert sig["model_disagreement_flag"] == 0
assert sig["sigma_multiplier"] == 1.00

sig = _extract_signals("This is a challenging forecast. Models disagree with large model spread.")
print(f"  Low conf+disagree: {sig}")
assert sig["confidence_flag"] == "LOW"
assert sig["model_disagreement_flag"] == 1
assert sig["sigma_multiplier"] == 1.25

sig = _extract_signals("Temperatures will be warmer than normal this week.")
print(f"  Warm bias: {sig}")
assert sig["directional_note"] == "WARM_BIAS"

# --- 9. Truncation ---
print("\n--- 9. METAR Truncation ---")

from kalshicast.pricing.truncation import apply_metar_truncation

bins_t = [
    {"bin_lower": 70, "bin_upper": 72, "p_win": 0.2},
    {"bin_lower": 72, "bin_upper": 74, "p_win": 0.3},
    {"bin_lower": 74, "bin_upper": 76, "p_win": 0.3},
    {"bin_lower": 76, "bin_upper": 78, "p_win": 0.2},
]
result = apply_metar_truncation(bins_t, "KNYC", "2026-03-29", "HIGH", (75, 3, 0))
assert result == bins_t
print("  No-conn passthrough: OK")

# --- 10. Kalshi Client ---
print("\n--- 10. Kalshi API Client ---")

from kalshicast.execution.kalshi_api import KalshiClient

client = KalshiClient(api_key_id="test", private_key_pem=None, base_url="https://test.example.com")
assert client.api_key_id == "test"
assert client.base_url == "https://test.example.com"
print(f"  Client init: OK (base_url={client.base_url})")

# --- 11. Skew-Normal Pricing (Phase 2 regression) ---
print("\n--- 11. Skew-Normal Pricing (regression) ---")

from kalshicast.pricing.shadow_book import convert_to_skewnorm_params, compute_p_win

xi, omega, alpha = convert_to_skewnorm_params(mu=75.0, sigma_eff=3.0, g1_s=0.3)
print(f"  Skew-normal: xi={xi:.3f}, omega={omega:.3f}, alpha={alpha:.4f}")
assert omega > 0

p = compute_p_win(74.0, 76.0, xi, omega, alpha)
print(f"  P(74-76): {p:.4f}")
assert 0.0 < p < 1.0

# --- 12. Kalman (Phase 2 regression) ---
print("\n--- 12. Kalman Filter (regression) ---")

from kalshicast.processing.kalman import KalmanState, kalman_update

state = KalmanState()
print(f"  Init: b_k={state.b_k}, u_k={state.u_k}")
assert state.b_k == 0.0
assert state.u_k == 4.0

# --- 13. Complete file count ---
print("\n--- 13. Project Stats ---")

import os
py_count = 0
py_lines = 0
for root, dirs, files in os.walk("kalshicast"):
    for f in files:
        if f.endswith(".py"):
            py_count += 1
            with open(os.path.join(root, f), encoding="utf-8", errors="ignore") as fh:
                py_lines += sum(1 for _ in fh)

print(f"  Python files: {py_count}")
print(f"  Total lines:  {py_lines}")

print()
print("=" * 60)
print("ALL 13 TEST SUITES PASSED")
print("=" * 60)
