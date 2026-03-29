"""Integration tests — verify cross-layer interactions without DB."""

import pytest
from kalshicast.processing.kalman import KalmanState, kalman_update, compute_R_k, compute_Q_k
from kalshicast.processing.regime import detect_bimodal
from kalshicast.pricing.shadow_book import convert_to_skewnorm_params, compute_p_win
from kalshicast.execution.gates import evaluate_all_gates
from kalshicast.execution.kelly import smirnov_kelly, full_sizing_chain
from kalshicast.execution.ibe import compute_composite
from kalshicast.execution.vwap import compute_vwap
from kalshicast.execution.orders import compute_ev_net, maker_or_taker
from kalshicast.tests.generators import generate_shadow_book, generate_orderbook


class TestKalmanToPricing:
    """Verify Kalman bias output feeds correctly into pricing."""

    def test_bias_adjusts_distribution(self):
        state = KalmanState(b_k=0.0, u_k=4.0)
        new_state, _ = kalman_update(state, epsilon_k=2.0, R_k=2.0, Q_k=0.01)

        mu_base = 75.0
        mu_adjusted = mu_base - new_state.b_k

        xi_base, omega, alpha = convert_to_skewnorm_params(mu_base, 3.0, 0.0)
        xi_adj, _, _ = convert_to_skewnorm_params(mu_adjusted, 3.0, 0.0)

        p_base = compute_p_win(74.0, 76.0, xi_base, omega, alpha)
        p_adj = compute_p_win(74.0, 76.0, xi_adj, omega, alpha)

        assert p_base != p_adj


class TestPricingToExecution:
    """Verify shadow book bins flow through gates and sizing."""

    def test_full_pipeline_flow(self):
        bins = generate_shadow_book(n_bins=5, center=75.0)
        best_bin = max(bins, key=lambda b: b["p_win"])

        gates = evaluate_all_gates({
            "p_win": best_bin["p_win"],
            "c_market": best_bin["p_win"] * 0.8,
            "bankroll": 1000, "n_bets": 50,
            "s_tk": 2.0, "bss": 0.15,
            "was_qualified": True, "lead_hours": 24.0,
        })

        kelly_result = smirnov_kelly([
            {"p_win": b["p_win"], "c_market": b["p_win"] * 0.85, "ticker": b["ticker"]}
            for b in bins
        ])

        assert isinstance(kelly_result, list)


class TestOrderbookToExecution:
    """Verify orderbook → VWAP → maker/taker → sizing chain."""

    def test_orderbook_flow(self):
        book = generate_orderbook(depth=5, base_price=42)
        c_vwap, depth = compute_vwap(book, 30)
        assert depth > 0

        decision = maker_or_taker(
            lead_hours=36.0, p_win=0.55,
            c_bid=c_vwap - 0.01, c_ask=c_vwap + 0.01,
        )
        assert decision["order_type"] in ("MAKER", "TAKER")

        ev = compute_ev_net(0.55, decision["price"], 0.07)
        assert isinstance(ev, float)


class TestSizingChain:
    """Verify full sizing chain produces valid output."""

    def test_chain_produces_contracts(self):
        sizing = full_sizing_chain(
            f_star=0.10, bss=0.20, ibe_composite=0.95, gamma_scale=1.0,
            mdd=0.05, bankroll=1000.0, remaining_capacity=0.25, c_market=0.45,
        )
        assert not sizing["skip"]
        assert sizing["contracts"] > 0
        assert sizing["f_final"] > 0

    def test_high_mdd_kills_sizing(self):
        sizing = full_sizing_chain(
            f_star=0.10, bss=0.20, ibe_composite=0.95, gamma_scale=1.0,
            mdd=0.20, bankroll=1000.0, remaining_capacity=0.25, c_market=0.45,
        )
        assert sizing["skip"]

    def test_low_bss_reduces(self):
        s_high = full_sizing_chain(
            f_star=0.10, bss=0.25, ibe_composite=1.0, gamma_scale=1.0,
            mdd=0.05, bankroll=1000.0, remaining_capacity=0.25, c_market=0.45,
        )
        s_low = full_sizing_chain(
            f_star=0.10, bss=0.05, ibe_composite=1.0, gamma_scale=1.0,
            mdd=0.05, bankroll=1000.0, remaining_capacity=0.25, c_market=0.45,
        )
        assert s_high["f_final"] >= s_low["f_final"]


class TestRegimeToPricing:
    """Verify bimodal detection integrates with pricing."""

    def test_bimodal_not_triggered_normal(self):
        forecasts = [74.0, 75.0, 74.5, 75.5, 74.8, 75.2, 74.3, 75.7]
        result = detect_bimodal(forecasts, s_tk=1.0)
        assert result is None

    def test_composite_with_varied_signals(self):
        mods = [1.0, 0.9, 1.1, 0.95, 1.05]
        weights = [0.25, 0.35, 0.15, 0.15, 0.10]
        c = compute_composite(mods, weights)
        assert 0.25 <= c <= 1.50
