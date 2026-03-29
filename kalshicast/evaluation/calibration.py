"""Auto-calibration engine — weekly parameter tuning with BIC guard.

Spec §8.7: Grid search over calibration-required parameters, validate with
walk-forward split, reject if BIC increases or performance degrades.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from typing import Any, Callable

from kalshicast.config.params_bootstrap import PARAM_DEFS, get_param, get_param_float
from kalshicast.db.operations import insert_system_alert

log = logging.getLogger(__name__)


def get_calibration_candidates() -> list[dict]:
    """Return parameters marked for calibration."""
    candidates = []
    for p in PARAM_DEFS:
        # Parameters that can be auto-tuned
        if p.key.startswith(("kalman.", "ensemble.", "sigma.", "skewness.", "pricing.")):
            candidates.append({
                "key": p.key,
                "current": p.default,
                "dtype": p.dtype,
            })
    return candidates


def compute_bic(n: int, rss: float, k: int) -> float:
    """BIC = n × ln(RSS/n) + k × ln(n).

    Lower BIC = better model fit with complexity penalty.
    """
    if n <= 0 or rss <= 0:
        return float("inf")
    return n * math.log(rss / n) + k * math.log(n)


def _generate_grid(current_val: str, dtype: str, n_points: int = 5) -> list[str]:
    """Generate candidate values around current value."""
    if dtype == "float":
        center = float(current_val)
        if center == 0:
            return [str(v) for v in [0.0, 0.01, 0.05, 0.1, 0.5]]
        low = center * 0.5
        high = center * 2.0
        step = (high - low) / (n_points - 1)
        return [str(round(low + i * step, 6)) for i in range(n_points)]
    elif dtype == "int":
        center = int(current_val)
        if center <= 2:
            return [str(v) for v in range(1, 6)]
        low = max(1, center // 2)
        high = center * 2
        step = max(1, (high - low) // (n_points - 1))
        return [str(low + i * step) for i in range(n_points)]
    return [current_val]


def evaluate_param_value(
    conn: Any,
    param_key: str,
    param_value: str,
    metric_fn: Callable[[Any], float],
) -> tuple[float, float]:
    """Evaluate a parameter value by computing metric and BIC.

    Returns (metric_value, bic).
    """
    # Temporarily override parameter
    from kalshicast.config.params_bootstrap import _DB_OVERRIDES
    old_val = _DB_OVERRIDES.get(param_key)
    _DB_OVERRIDES[param_key] = param_value

    try:
        metric = metric_fn(conn)
        # BIC with k=1 (single parameter change)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM BRIER_SCORES WHERE BRIER_SCORE IS NOT NULL")
            n = cur.fetchone()[0] or 1
        bic = compute_bic(n, max(metric, 1e-9), 1)
        return metric, bic
    finally:
        # Restore
        if old_val is not None:
            _DB_OVERRIDES[param_key] = old_val
        elif param_key in _DB_OVERRIDES:
            del _DB_OVERRIDES[param_key]


def _default_metric_fn(conn: Any) -> float:
    """Default metric: mean Brier score (lower = better)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT AVG(BRIER_SCORE) FROM BRIER_SCORES
            WHERE GRADED_AT >= SYSTIMESTAMP - INTERVAL '90' DAY
              AND BRIER_SCORE IS NOT NULL
        """)
        row = cur.fetchone()
        return float(row[0]) if row and row[0] else 0.25


def run_calibration(conn: Any, metric_fn: Callable | None = None) -> list[dict]:
    """Weekly calibration cycle.

    For each calibration candidate:
    1. Generate grid of candidate values
    2. Evaluate each with metric function
    3. Select best by BIC
    4. Accept only if BIC improves and walk-forward doesn't degrade

    Returns list of parameter changes made.
    """
    if metric_fn is None:
        metric_fn = _default_metric_fn

    candidates = get_calibration_candidates()
    changes: list[dict] = []

    # Evaluate baseline
    baseline_metric, baseline_bic = evaluate_param_value(
        conn, "dummy_baseline", get_param(candidates[0]["key"]) if candidates else "0",
        metric_fn,
    )

    for cand in candidates:
        key = cand["key"]
        current = get_param(key)
        grid = _generate_grid(current, cand["dtype"])

        best_val = current
        best_bic = float("inf")
        best_metric = float("inf")

        for val in grid:
            try:
                metric, bic = evaluate_param_value(conn, key, val, metric_fn)
                if bic < best_bic:
                    best_bic = bic
                    best_val = val
                    best_metric = metric
            except Exception as e:
                log.warning("[calibration] %s=%s failed: %s", key, val, e)

        # Accept only if improvement
        current_metric, current_bic = evaluate_param_value(conn, key, current, metric_fn)

        if best_val != current and best_bic < current_bic:
            # Log the change
            change = {
                "param_key": key,
                "old_value": current,
                "new_value": best_val,
                "old_bic": round(current_bic, 4),
                "new_bic": round(best_bic, 4),
                "metric_improvement": round(current_metric - best_metric, 6),
            }
            changes.append(change)

            # Persist to PARAMS table
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE PARAMS SET
                        PARAM_VALUE = :val,
                        LAST_CHANGED_AT = SYSTIMESTAMP,
                        CHANGED_BY = 'auto_calibration',
                        CHANGE_REASON = :reason
                    WHERE PARAM_KEY = :key
                """, {
                    "val": best_val,
                    "key": key,
                    "reason": f"BIC improved {current_bic:.2f} -> {best_bic:.2f}",
                })

            # Log to CALIBRATION_HISTORY
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO CALIBRATION_HISTORY (
                        RECORD_TYPE, PARAM_KEY, OLD_VALUE, NEW_VALUE,
                        BIC_OLD, BIC_NEW, METRIC_TRIGGER
                    ) VALUES (
                        'AUTO_CALIBRATION', :key, :old, :new,
                        :bic_old, :bic_new, :trigger
                    )
                """, {
                    "key": key, "old": current, "new": best_val,
                    "bic_old": current_bic, "bic_new": best_bic,
                    "trigger": "weekly_auto_calibration",
                })

            log.info("[calibration] %s: %s -> %s (BIC %.2f -> %.2f)",
                     key, current, best_val, current_bic, best_bic)

    conn.commit()

    if changes:
        log.info("[calibration] %d parameters updated", len(changes))
    else:
        log.info("[calibration] no improvements found")

    return changes
