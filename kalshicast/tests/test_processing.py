"""L2 Processing unit tests."""

import pytest
from unittest.mock import patch, MagicMock
from kalshicast.processing.kalman import (
    KalmanState, init_kalman_state, compute_R_k, compute_Q_k,
    kalman_update, _compute_ewm_variance,
)
from kalshicast.processing.regime import detect_bimodal, _iqr, _kmeans_2
from kalshicast.processing.ensemble import _compute_per_source_skill


class TestKalmanState:
    def test_default_init(self):
        state = KalmanState()
        assert state.b_k == 0.0
        assert state.u_k == 4.0
        assert state.state_version == 1

    def test_custom_init(self):
        state = KalmanState(b_k=1.5, u_k=2.0, state_version=10)
        assert state.b_k == 1.5
        assert state.state_version == 10


class TestKalmanUpdate:
    def test_basic_update(self):
        state = KalmanState(b_k=0.0, u_k=4.0)
        new_state, history = kalman_update(state, epsilon_k=2.0, R_k=2.0, Q_k=0.01)
        assert new_state.b_k != 0.0
        assert new_state.u_k < state.u_k + 0.01
        assert new_state.state_version == 2
        assert not history["is_amendment"]

    def test_gain_bounded(self):
        state = KalmanState(b_k=0.0, u_k=4.0)
        _, history = kalman_update(state, 1.0, R_k=2.0, Q_k=0.0)
        assert 0.0 < history["k_k"] < 1.0

    def test_gap_inflates_uncertainty(self):
        state = KalmanState(b_k=0.0, u_k=4.0)
        s1, _ = kalman_update(state, 1.0, R_k=2.0, Q_k=0.1, gap_days=0)
        s2, _ = kalman_update(state, 1.0, R_k=2.0, Q_k=0.1, gap_days=5)
        # With gap, gain should be higher (more uncertain prior)
        assert s2.u_k != s1.u_k

    def test_version_increments(self):
        state = KalmanState(state_version=5)
        new_state, _ = kalman_update(state, 0.5, R_k=2.0, Q_k=0.01)
        assert new_state.state_version == 6


class TestRk:
    def test_no_deviation(self):
        r = compute_R_k(f_top=75.0, f_bar=75.0, s_tk=3.0)
        assert r > 0

    def test_deviation_inflates(self):
        r_low = compute_R_k(f_top=75.0, f_bar=75.0, s_tk=3.0)
        r_high = compute_R_k(f_top=82.0, f_bar=75.0, s_tk=3.0)
        assert r_high >= r_low


class TestQk:
    def test_no_innovations(self):
        q = compute_Q_k(0.01, [], 0.0)
        assert q == 0.01

    def test_innovations_increase(self):
        q_base = compute_Q_k(0.01, [1.0, 2.0, 3.0], 0.0)
        q_none = compute_Q_k(0.01, [], 0.0)
        assert q_base >= q_none


class TestEWMVariance:
    def test_constant_series(self):
        var = _compute_ewm_variance([1.0, 1.0, 1.0, 1.0])
        assert var < 0.01

    def test_variable_series(self):
        var = _compute_ewm_variance([0.0, 5.0, 0.0, 5.0, 0.0])
        assert var > 0.1

    def test_short_series(self):
        var = _compute_ewm_variance([1.0])
        assert var == 0.01


class TestBimodal:
    def test_unimodal_returns_none(self):
        forecasts = [74.0, 75.0, 74.5, 75.5, 74.8, 75.2, 74.3, 75.7]
        result = detect_bimodal(forecasts, s_tk=1.0)
        assert result is None

    def test_bimodal_detected(self):
        # Two clear clusters
        forecasts = [70.0, 70.5, 71.0, 70.2, 80.0, 80.5, 81.0, 80.2]
        result = detect_bimodal(forecasts, s_tk=2.0)
        assert result is not None
        assert result["centroid_1"] < result["centroid_2"]
        assert result["centroid_distance"] > 5.0

    def test_too_few_forecasts(self):
        assert detect_bimodal([70.0, 80.0], s_tk=2.0) is None

    def test_zero_spread(self):
        assert detect_bimodal([70.0, 75.0, 80.0, 85.0], s_tk=0.0) is None


class TestIQR:
    def test_basic(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        iqr = _iqr(values)
        assert iqr > 0

    def test_empty(self):
        assert _iqr([]) == 0.0


class TestKMeans2:
    def test_two_clusters(self):
        values = [1.0, 1.5, 2.0, 10.0, 10.5, 11.0]
        c1, c2, s1, s2 = _kmeans_2(values)
        assert c1 < c2
        assert s1 == 3
        assert s2 == 3

    def test_single_value(self):
        c1, c2, s1, s2 = _kmeans_2([5.0])
        assert c1 == 5.0


# ─────────────────────────────────────────────────────────────────────
# Per-source skill scoring tests
# ─────────────────────────────────────────────────────────────────────

def _make_errors(values: list[float]) -> list[dict]:
    """Build a list of error-row dicts suitable for mocking DB results."""
    return [{"error_adjusted": None, "error_raw": v} for v in values]


class TestPerSourceSkill:
    """Tests for _compute_per_source_skill."""

    def _call(self, side_effects, source_ids):
        """Call _compute_per_source_skill with a mocked DB operation."""
        with patch(
            "kalshicast.db.operations.get_forecast_errors_window"
        ) as mock_get:
            mock_get.side_effect = side_effects
            conn = MagicMock()
            return _compute_per_source_skill(
                conn, "KORD", "HIGH", "h2", source_ids, window=30
            )

    def test_cold_start_no_sources_have_data(self):
        """Returns all-zero scores when no source meets min_samples."""
        # Each source returns fewer than 5 samples
        side_effects = [
            _make_errors([1.0, 2.0]),  # src_a — only 2 samples
            _make_errors([0.5]),        # src_b — only 1 sample
        ]
        scores = self._call(side_effects, ["src_a", "src_b"])
        assert scores == [0.0, 0.0]

    def test_single_source_with_history_gets_full_skill(self):
        """When exactly one source meets min_samples, it gets skill 1.0."""
        side_effects = [
            _make_errors([1.0] * 10),  # src_a — 10 samples
            _make_errors([]),          # src_b — 0 samples
        ]
        scores = self._call(side_effects, ["src_a", "src_b"])
        assert scores[0] == 1.0
        assert scores[1] == 0.0

    def test_two_sources_scores_normalized_correctly(self):
        """Best source gets skill > 0; worst source gets 0."""
        # src_a has lower MSE (better), src_b higher MSE (worse)
        side_effects = [
            _make_errors([1.0] * 10),  # src_a: MSE = 1.0
            _make_errors([3.0] * 10),  # src_b: MSE = 9.0
        ]
        scores = self._call(side_effects, ["src_a", "src_b"])
        # src_a: 1 - (1/9) ≈ 0.888, src_b: 1 - (9/9) = 0.0
        assert scores[1] == pytest.approx(0.0)
        assert scores[0] > 0.0
        assert all(s >= 0.0 for s in scores)

    def test_equal_mse_produces_equal_scores(self):
        """Two sources with identical MSE get the same skill score."""
        side_effects = [
            _make_errors([2.0] * 10),
            _make_errors([2.0] * 10),
        ]
        scores = self._call(side_effects, ["src_a", "src_b"])
        assert scores[0] == pytest.approx(scores[1])

    def test_source_missing_from_history_scores_zero(self):
        """A source with no history rows scores 0 even when others have data."""
        side_effects = [
            _make_errors([1.0] * 10),  # src_a
            _make_errors([2.0] * 10),  # src_b
            _make_errors([]),          # src_c — no history
        ]
        scores = self._call(side_effects, ["src_a", "src_b", "src_c"])
        assert scores[2] == 0.0
        assert scores[0] > 0.0 or scores[1] > 0.0
