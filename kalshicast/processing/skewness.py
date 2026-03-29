"""Skewness computation — unbiased G1_s with significance test.

Spec §5.4: g_s (biased) → G1_s (unbiased) → significance → clamp or use.
"""

from __future__ import annotations

import math

from kalshicast.config.params_bootstrap import get_param_float


def compute_biased_skewness(errors: list[float]) -> float:
    """Biased sample skewness g_s = m3 / m2^(3/2).

    Returns 0.0 if variance is zero or fewer than 3 samples.
    """
    n = len(errors)
    if n < 3:
        return 0.0

    mean = sum(errors) / n
    m2 = sum((e - mean) ** 2 for e in errors) / n
    m3 = sum((e - mean) ** 3 for e in errors) / n

    if m2 <= 0:
        return 0.0

    return m3 / (m2 ** 1.5)


def compute_unbiased_skewness(g_s: float, n: int) -> float:
    """Unbiased sample skewness G1_s = [√(N(N-1)) / (N-2)] × g_s.

    Requires N ≥ 3. Returns 0.0 for N < 3.
    """
    if n < 3:
        return 0.0

    correction = math.sqrt(n * (n - 1)) / (n - 2)
    return correction * g_s


def apply_significance_test(g1_s: float, n: int,
                            factor: float | None = None) -> float:
    """If |G1_s| < factor × √(6/N), clamp to 0.0 (normal fallback).

    Default factor = 2.0 (spec §5.4.4).
    """
    if factor is None:
        factor = get_param_float("skewness.significance_factor")

    if n < 3:
        return 0.0

    threshold = factor * math.sqrt(6.0 / n)

    if abs(g1_s) < threshold:
        return 0.0

    return g1_s


def compute_skewness(errors: list[float]) -> float:
    """Full pipeline: zero-var check → biased → unbiased → significance test.

    Returns G1_s suitable for skew-normal pricing (0.0 = normal fallback).
    """
    n = len(errors)
    if n < 3:
        return 0.0

    g_s = compute_biased_skewness(errors)
    if g_s == 0.0:
        return 0.0

    g1_s = compute_unbiased_skewness(g_s, n)
    return apply_significance_test(g1_s, n)
