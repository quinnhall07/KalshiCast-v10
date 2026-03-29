"""L5 Evaluation unit tests."""

import math
import pytest
from kalshicast.evaluation.calibration import (
    compute_bic, _generate_grid, get_calibration_candidates,
)
from kalshicast.evaluation.adverse_selection import compute_fill_quality_delta
from kalshicast.tests.generators import generate_brier_scores


class TestBIC:
    def test_basic(self):
        bic = compute_bic(n=100, rss=10.0, k=3)
        expected = 100 * math.log(10.0 / 100) + 3 * math.log(100)
        assert abs(bic - expected) < 0.01

    def test_zero_n(self):
        assert compute_bic(0, 10.0, 3) == float("inf")

    def test_zero_rss(self):
        assert compute_bic(100, 0.0, 3) == float("inf")

    def test_lower_is_better(self):
        bic_good = compute_bic(100, 5.0, 2)
        bic_bad = compute_bic(100, 50.0, 2)
        assert bic_good < bic_bad


class TestGenerateGrid:
    def test_float_grid(self):
        grid = _generate_grid("0.5", "float", n_points=5)
        assert len(grid) == 5
        assert all(isinstance(float(v), float) for v in grid)

    def test_int_grid(self):
        grid = _generate_grid("10", "int", n_points=5)
        assert len(grid) == 5
        assert all(isinstance(int(v), int) for v in grid)

    def test_zero_float(self):
        grid = _generate_grid("0.0", "float")
        assert len(grid) == 5
        assert "0.0" in grid

    def test_small_int(self):
        grid = _generate_grid("1", "int")
        assert len(grid) == 5


class TestCalibrationCandidates:
    def test_returns_list(self):
        candidates = get_calibration_candidates()
        assert isinstance(candidates, list)
        for c in candidates:
            assert "key" in c
            assert "dtype" in c

    def test_correct_prefixes(self):
        candidates = get_calibration_candidates()
        for c in candidates:
            assert c["key"].startswith(
                ("kalman.", "ensemble.", "sigma.", "skewness.", "pricing.")
            )


class TestGenerators:
    def test_brier_scores(self):
        scores = generate_brier_scores(n=20)
        assert len(scores) == 20
        for s in scores:
            assert 0.0 <= s["brier_score"] <= 1.0
