"""Adverse selection test — monitor maker fill quality vs taker.

Spec §8.5: Compare fill quality between maker and taker orders over
a rolling window. Alert if maker fills consistently worse.
"""

from __future__ import annotations

import logging
from typing import Any

from kalshicast.config.params_bootstrap import get_param_int

log = logging.getLogger(__name__)


def compute_fill_quality_delta(conn: Any, window_days: int | None = None) -> dict:
    """Compare maker vs taker fill quality over rolling window.

    fill_quality = (VWAP at entry - actual fill price) / VWAP
    Negative delta means makers are getting worse fills.
    """
    if window_days is None:
        window_days = get_param_int("eval.adverse_selection_window")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT ORDER_TYPE,
                   COUNT(*) AS n,
                   AVG(FILL_QUALITY) AS avg_fill_quality
            FROM POSITIONS
            WHERE STATUS IN ('FILLED', 'SETTLED')
              AND SUBMITTED_AT >= SYSTIMESTAMP - NUMTODSINTERVAL(:days, 'DAY')
              AND FILL_QUALITY IS NOT NULL
            GROUP BY ORDER_TYPE
        """, {"days": window_days})

        results = {}
        for row in cur:
            results[row[0]] = {"n": int(row[1]), "avg_fill_quality": float(row[2])}

    maker = results.get("MAKER", {"n": 0, "avg_fill_quality": 0.0})
    taker = results.get("TAKER", {"n": 0, "avg_fill_quality": 0.0})

    delta = maker["avg_fill_quality"] - taker["avg_fill_quality"]

    return {
        "maker_n": maker["n"],
        "taker_n": taker["n"],
        "maker_avg_quality": round(maker["avg_fill_quality"], 6),
        "taker_avg_quality": round(taker["avg_fill_quality"], 6),
        "delta": round(delta, 6),
        "window_days": window_days,
    }


