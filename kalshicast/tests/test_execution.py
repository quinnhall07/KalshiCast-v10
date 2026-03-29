"""L4 Execution unit tests."""

import pytest
from kalshicast.execution.gates import (
    check_edge_gate, check_spread_gate, check_skill_gate,
    check_lead_gate, evaluate_all_gates,
)
from kalshicast.execution.ibe import compute_mpds, compute_fct, compute_composite
from kalshicast.execution.kelly import (
    smirnov_kelly, compute_phi_bss, compute_drawdown_scale,
    compute_market_convergence, full_sizing_chain,
)
from kalshicast.execution.vwap import compute_vwap, check_staleness, split_tranches
from kalshicast.execution.orders import compute_ev_net, maker_or_taker
from kalshicast.execution.positions import check_single_limit


class TestEdgeGate:
    def test_clear_edge_passes(self):
        r = check_edge_gate(p_win=0.65, c_market=0.45, bankroll=1000.0, n_bets=100)
        assert r["pass"]

    def test_no_edge_fails(self):
        r = check_edge_gate(p_win=0.46, c_market=0.45, bankroll=1000.0, n_bets=100)
        assert not r["pass"]

    def test_negative_edge_fails(self):
        r = check_edge_gate(p_win=0.30, c_market=0.50, bankroll=1000.0, n_bets=50)
        assert not r["pass"]


class TestSpreadGate:
    def test_narrow_passes(self):
        assert check_spread_gate(2.5)["pass"]

    def test_wide_fails(self):
        assert not check_spread_gate(5.0)["pass"]

    def test_boundary(self):
        assert check_spread_gate(4.0)["pass"]
        assert not check_spread_gate(4.01)["pass"]


class TestSkillGate:
    def test_enter_qualified(self):
        assert check_skill_gate(0.08, was_qualified=False)["pass"]

    def test_below_enter_fails(self):
        assert not check_skill_gate(0.05, was_qualified=False)["pass"]

    def test_hysteresis_holds(self):
        assert check_skill_gate(0.05, was_qualified=True)["pass"]

    def test_below_exit_fails(self):
        assert not check_skill_gate(0.02, was_qualified=True)["pass"]


class TestLeadGate:
    def test_within_limit(self):
        assert check_lead_gate(48.0)["pass"]

    def test_exceeds_limit(self):
        assert not check_lead_gate(80.0)["pass"]


class TestAllGates:
    def test_good_candidate(self):
        r = evaluate_all_gates({
            "p_win": 0.60, "c_market": 0.40, "bankroll": 1000, "n_bets": 50,
            "s_tk": 2.0, "bss": 0.15, "was_qualified": True, "lead_hours": 24.0,
        })
        assert r["pass"]
        assert all(r["flags"].values())


class TestMPDS:
    def test_no_veto(self):
        r = compute_mpds(0.50, 0.48, 0.45, 0.44)
        assert not r["veto"]

    def test_veto(self):
        r = compute_mpds(0.50, 0.48, 0.30, 0.44)
        assert r["veto"]


class TestFCT:
    def test_no_veto(self):
        r = compute_fct(s_current=3.0, s_previous=2.5, sigma_hist=3.0)
        assert not r["veto"]

    def test_veto(self):
        r = compute_fct(s_current=8.0, s_previous=2.0, sigma_hist=3.0)
        assert r["veto"]


class TestComposite:
    def test_normal_range(self):
        mods = [0.9, 0.8, 1.1, 1.0, 0.95]
        weights = [0.25, 0.35, 0.15, 0.15, 0.10]
        c = compute_composite(mods, weights)
        assert 0.85 < c < 0.95

    def test_extreme_clipped(self):
        mods = [0.1, 0.1, 0.1, 0.1, 0.1]
        weights = [0.25, 0.35, 0.15, 0.15, 0.10]
        c = compute_composite(mods, weights)
        assert c == 0.25


class TestSmirnovKelly:
    def test_three_bin(self):
        bins = [
            {"p_win": 0.25, "c_market": 0.30, "ticker": "A"},
            {"p_win": 0.50, "c_market": 0.40, "ticker": "B"},
            {"p_win": 0.25, "c_market": 0.30, "ticker": "C"},
        ]
        result = smirnov_kelly(bins)
        assert len(result) == 1
        assert result[0]["ticker"] == "B"
        assert abs(result[0]["f_star"] - 0.1667) < 0.001

    def test_total_fraction_bounded(self):
        bins = [
            {"p_win": 0.10, "c_market": 0.15, "ticker": "1"},
            {"p_win": 0.30, "c_market": 0.20, "ticker": "2"},
            {"p_win": 0.25, "c_market": 0.18, "ticker": "3"},
            {"p_win": 0.20, "c_market": 0.22, "ticker": "4"},
            {"p_win": 0.15, "c_market": 0.25, "ticker": "5"},
        ]
        result = smirnov_kelly(bins)
        total = sum(r["f_star"] for r in result)
        assert total <= 1.0


class TestPhiBSS:
    def test_high_bss(self):
        assert compute_phi_bss(0.25) == 1.0

    def test_medium_bss(self):
        assert abs(compute_phi_bss(0.125) - 0.50) < 0.01

    def test_low_bss(self):
        assert compute_phi_bss(0.01) == 0.10


class TestDrawdownScale:
    def test_safe(self):
        assert compute_drawdown_scale(0.05) == 1.0

    def test_midpoint(self):
        assert abs(compute_drawdown_scale(0.15) - 0.50) < 0.01

    def test_halt(self):
        assert compute_drawdown_scale(0.20) == 0.0

    def test_beyond_halt(self):
        assert compute_drawdown_scale(0.25) == 0.0


class TestVWAP:
    def test_basic(self):
        book = {"yes": [
            {"price": 42, "quantity": 50},
            {"price": 43, "quantity": 30},
            {"price": 44, "quantity": 20},
        ]}
        c_vwap, depth = compute_vwap(book, 60)
        assert depth == 60
        assert 42.0 < c_vwap * 100 < 44.0

    def test_insufficient_depth(self):
        book = {"yes": [{"price": 42, "quantity": 10}]}
        c_vwap, depth = compute_vwap(book, 50)
        assert depth == 10


class TestStaleness:
    def test_small_delta(self):
        s = check_staleness(0.43, 0.42)
        assert not s["abort"]

    def test_large_delta(self):
        s = check_staleness(0.55, 0.42)
        assert s["abort"]


class TestTranches:
    def test_small_order(self):
        assert split_tranches(10) == [10]

    def test_large_order(self):
        t = split_tranches(120)
        assert sum(t) == 120
        assert all(c <= 25 for c in t[:-1])


class TestEVNet:
    def test_positive(self):
        ev = compute_ev_net(0.60, 0.45, 0.07)
        assert ev > 0

    def test_negative(self):
        ev = compute_ev_net(0.40, 0.50, 0.07)
        assert ev < 0


class TestMakerTaker:
    def test_decision_returned(self):
        d = maker_or_taker(lead_hours=48.0, p_win=0.60, c_bid=0.43, c_ask=0.45)
        assert d["order_type"] in ("MAKER", "TAKER")
        assert d["price"] > 0
        assert 0.0 <= d["p_fill"] <= 1.0


class TestPositionLimits:
    def test_within_limit(self):
        assert check_single_limit(0.05) == 0.05

    def test_exceeds_limit(self):
        assert check_single_limit(0.15) == 0.10
