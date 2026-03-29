"""Financial metrics — minimal Phase 2 implementation.

Spec §8.3: SR_$, MDD, FDR, EUR, CAL, etc.
Phase 2 has no real positions, so most metrics are zero.
Only CAL (probability calibration) is computed from Brier scores.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from kalshicast.db.operations import upsert_financial_metrics

log = logging.getLogger(__name__)


def compute_financial_metrics(conn: Any, metric_date: date | str) -> None:
    """Compute and persist daily financial metrics.

    Phase 2: Most metrics are zero (no real positions).
    Computes: CAL from BRIER_SCORES (probability calibration).
    """
    md = str(metric_date)[:10]

    # Compute CAL: mean |predicted - actual| across probability buckets
    cal = _compute_cal(conn)

    row = {
        "metric_date": md,
        "bankroll": 1000.0,  # paper mode
        "portfolio_value": 1000.0,
        "daily_pnl": 0.0,
        "cumulative_pnl": 0.0,
        "mdd_alltime": 0.0,
        "mdd_rolling_90": 0.0,
        "sr_dollar": 0.0,
        "sr_simple": 0.0,
        "sharpe_rolling_30": 0.0,
        "fdr": 0.0,
        "eur": 0.0,
        "cal": cal,
        "market_cal": None,
        "n_bets_total": 0,
        "n_bets_won": 0,
        "n_bets_lost": 0,
        "gross_profit": 0.0,
        "net_profit": 0.0,
        "total_fees": 0.0,
    }

    upsert_financial_metrics(conn, row)
    conn.commit()
    log.info("[financial] metrics for %s: CAL=%.4f", md, cal or 0)


def _compute_cal(conn: Any, n_buckets: int = 10) -> float | None:
    """Compute probability calibration (CAL) from BRIER_SCORES.

    CAL = (1/B) × Σ_b |f̄_b - ō_b|
    where f̄_b = mean predicted in bucket, ō_b = actual outcome frequency.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT P_WIN_AT_GRADING, OUTCOME
            FROM BRIER_SCORES
            WHERE P_WIN_AT_GRADING IS NOT NULL AND OUTCOME IS NOT NULL
        """)
        data = [(float(row[0]), int(row[1])) for row in cur]

    if len(data) < 10:
        return None

    # Bucket by predicted probability
    buckets: dict[int, list[tuple[float, int]]] = {i: [] for i in range(n_buckets)}
    for p, o in data:
        b = min(int(p * n_buckets), n_buckets - 1)
        buckets[b].append((p, o))

    cal_sum = 0.0
    n_nonempty = 0
    for b_idx, items in buckets.items():
        if not items:
            continue
        f_bar = sum(p for p, _ in items) / len(items)
        o_bar = sum(o for _, o in items) / len(items)
        cal_sum += abs(f_bar - o_bar)
        n_nonempty += 1

    if n_nonempty == 0:
        return None

    return cal_sum / n_nonempty
