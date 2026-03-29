"""Adverse selection test — monitor maker fill quality vs taker.

Spec §8.5: Compare fill quality between maker and taker orders over
a rolling window. Alert if maker fills consistently worse.
"""

from __future__ import annotations

import logging
from typing import Any

from kalshicast.config.params_bootstrap import get_param_int
from kalshicast.db.operations import insert_system_alert

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


def check_adverse_selection(conn: Any) -> dict:
    """Run adverse selection test.

    WARNING if delta < -0.02, CRITICAL if delta < -0.05.
    """
    result = compute_fill_quality_delta(conn)

    min_samples = 20
    total_n = result["maker_n"] + result["taker_n"]

    if total_n < min_samples:
        result["status"] = "INSUFFICIENT_DATA"
        log.info("[adverse_selection] insufficient data (%d/%d samples)", total_n, min_samples)
        return result

    delta = result["delta"]

    if delta < -0.05:
        result["status"] = "CRITICAL"
        insert_system_alert(conn, {
            "alert_type": "ADVERSE_SELECTION_CRITICAL",
            "severity_score": 0.9,
            "details": result,
        })
        conn.commit()
        log.warning("[adverse_selection] CRITICAL: maker fill delta=%.4f", delta)

    elif delta < -0.02:
        result["status"] = "WARNING"
        insert_system_alert(conn, {
            "alert_type": "ADVERSE_SELECTION_WARNING",
            "severity_score": 0.6,
            "details": result,
        })
        conn.commit()
        log.warning("[adverse_selection] WARNING: maker fill delta=%.4f", delta)

    else:
        result["status"] = "OK"
        log.info("[adverse_selection] OK: maker fill delta=%.4f", delta)

    return result
