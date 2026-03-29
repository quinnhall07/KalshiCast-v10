"""L3 Pricing unit tests."""

import pytest
from kalshicast.pricing.shadow_book import (
    convert_to_skewnorm_params, compute_p_win,
)
from kalshicast.pricing.truncation import apply_metar_truncation
from kalshicast.tests.generators import generate_shadow_book


class TestSkewNormal:
    def test_zero_skew(self):
        xi, omega, alpha = convert_to_skewnorm_params(mu=75.0, sigma_eff=3.0, g1_s=0.0)
        assert omega > 0
        assert abs(alpha) < 0.01

    def test_positive_skew(self):
        xi, omega, alpha = convert_to_skewnorm_params(mu=75.0, sigma_eff=3.0, g1_s=0.5)
        assert alpha > 0

    def test_negative_skew(self):
        xi, omega, alpha = convert_to_skewnorm_params(mu=75.0, sigma_eff=3.0, g1_s=-0.5)
        assert alpha < 0


class TestPWin:
    def test_center_bin(self):
        xi, omega, alpha = convert_to_skewnorm_params(mu=75.0, sigma_eff=3.0, g1_s=0.0)
        p = compute_p_win(74.0, 76.0, xi, omega, alpha)
        assert 0.1 < p < 0.5

    def test_tail_bin(self):
        xi, omega, alpha = convert_to_skewnorm_params(mu=75.0, sigma_eff=3.0, g1_s=0.0)
        p = compute_p_win(85.0, 87.0, xi, omega, alpha)
        assert p < 0.01

    def test_full_range(self):
        xi, omega, alpha = convert_to_skewnorm_params(mu=75.0, sigma_eff=3.0, g1_s=0.0)
        p = compute_p_win(-100.0, 200.0, xi, omega, alpha)
        assert abs(p - 1.0) < 0.01

    def test_probabilities_sum_to_one(self):
        xi, omega, alpha = convert_to_skewnorm_params(mu=75.0, sigma_eff=3.0, g1_s=0.3)
        total = 0.0
        for lower in range(60, 90, 2):
            total += compute_p_win(float(lower), float(lower + 2), xi, omega, alpha)
        assert abs(total - 1.0) < 0.05


class TestTruncation:
    def test_no_conn_passthrough(self):
        bins = generate_shadow_book(n_bins=5)
        result = apply_metar_truncation(bins, "KNYC", "2026-03-29", "HIGH", (75, 3, 0))
        assert result == bins

    def test_preserves_probabilities(self):
        bins = generate_shadow_book(n_bins=5)
        total = sum(b["p_win"] for b in bins)
        assert abs(total - 1.0) < 0.01


class TestShadowBookGen:
    def test_generates_correct_count(self):
        bins = generate_shadow_book(n_bins=7)
        assert len(bins) == 7

    def test_probabilities_sum(self):
        bins = generate_shadow_book(n_bins=5)
        total = sum(b["p_win"] for b in bins)
        assert abs(total - 1.0) < 0.01

    def test_bins_contiguous(self):
        bins = generate_shadow_book(n_bins=5, center=80.0, bin_width=3.0)
        for i in range(len(bins) - 1):
            assert abs(bins[i]["bin_upper"] - bins[i + 1]["bin_lower"]) < 0.01
