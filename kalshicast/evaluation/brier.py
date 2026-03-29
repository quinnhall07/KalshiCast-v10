"""Brier score grading — probabilistic accuracy assessment.

Spec §8.1: BS = (P_yes - A)², BSS = 1 - BS_model / BS_baseline.
"""

from __future__ import annotations

import logging
from typing import Any

from kalshicast.db.operations import grade_brier_scores as _grade_db

log = logging.getLogger(__name__)


def grade_brier_scores(conn: Any, target_date: str) -> int:
    """Grade Shadow Book predictions against observations.

    For each ticker with target_date:
    - Outcome = 1 if observed value in [bin_lower, bin_upper), else 0
    - Brier Score = (P_win - outcome)²

    Returns count graded.
    """
    n = _grade_db(conn, target_date)
    conn.commit()
    if n:
        log.info("[brier] graded %d predictions for %s", n, target_date)
    else:
        log.info("[brier] no predictions to grade for %s", target_date)
    return n


def compute_bss(bs_model: float, bs_baseline: float) -> float | None:
    """Brier Skill Score = 1 - BS_model / BS_baseline.

    Returns None if baseline is zero. Positive = better than baseline.
    """
    if bs_baseline <= 0:
        return None
    return 1.0 - bs_model / bs_baseline
