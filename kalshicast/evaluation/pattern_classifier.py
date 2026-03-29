"""BSS pattern classifier — detects degradation patterns in the skill matrix.

Spec §8.6: 4 patterns scanned every pattern_check_interval_days.
Emits SYSTEM_ALERTS on detection.
"""

from __future__ import annotations

import logging
import statistics
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float
from kalshicast.db.operations import get_bss_matrix_all, insert_system_alert

log = logging.getLogger(__name__)


def run_pattern_classifier(conn: Any) -> list[dict]:
    """Scan BSS_MATRIX for 4 degradation patterns.

    Returns list of alert dicts emitted.
    """
    cells = get_bss_matrix_all(conn)
    if not cells:
        log.info("[patterns] BSS matrix empty, nothing to classify")
        return []

    exit_threshold = get_param_float("gate.bss_exit")
    alerts: list[dict] = []

    alerts.extend(_check_row_degradation(cells, exit_threshold))
    alerts.extend(_check_column_degradation(cells, exit_threshold))
    alerts.extend(_check_diagonal_degradation(cells))
    alerts.extend(_check_weight_convergence(conn))

    for alert in alerts:
        insert_system_alert(conn, alert)

    if alerts:
        conn.commit()
        log.warning("[patterns] emitted %d alerts", len(alerts))
    else:
        log.info("[patterns] no degradation patterns detected")

    return alerts


def _check_row_degradation(cells: list[dict], threshold: float) -> list[dict]:
    """Pattern 1: Station poor across all brackets. §8.6.1."""
    by_station: dict[str, list[float]] = {}
    for c in cells:
        bss = c.get("bss_1")
        if bss is not None:
            sid = c["station_id"]
            by_station.setdefault(sid, []).append(float(bss))

    alerts = []
    for sid, values in by_station.items():
        if len(values) >= 3 and statistics.mean(values) < threshold:
            alerts.append({
                "alert_type": "BSS_ROW_DEGRADATION",
                "station_id": sid,
                "severity_score": 0.6,
                "details": {
                    "pattern": "row_degradation",
                    "station_id": sid,
                    "mean_bss": round(statistics.mean(values), 4),
                    "n_cells": len(values),
                },
            })
    return alerts


def _check_column_degradation(cells: list[dict], threshold: float) -> list[dict]:
    """Pattern 2: Lead bracket poor across all stations. §8.6.2."""
    by_bracket: dict[str, list[float]] = {}
    for c in cells:
        bss = c.get("bss_1")
        if bss is not None:
            lb = c["lead_bracket"]
            by_bracket.setdefault(lb, []).append(float(bss))

    alerts = []
    for lb, values in by_bracket.items():
        if len(values) >= 5 and statistics.mean(values) < threshold:
            alerts.append({
                "alert_type": "BSS_COLUMN_DEGRADATION",
                "severity_score": 0.7,
                "details": {
                    "pattern": "column_degradation",
                    "lead_bracket": lb,
                    "mean_bss": round(statistics.mean(values), 4),
                    "n_stations": len(values),
                },
            })
    return alerts


def _check_diagonal_degradation(cells: list[dict]) -> list[dict]:
    """Pattern 3: Monotone BSS decrease across brackets per station. §8.6.3."""
    bracket_order = {"h1": 1, "h2": 2, "h3": 3, "h4": 4, "h5": 5}

    by_station: dict[str, dict[str, float]] = {}
    for c in cells:
        bss = c.get("bss_1")
        if bss is not None:
            sid = c["station_id"]
            lb = c["lead_bracket"]
            by_station.setdefault(sid, {})[lb] = float(bss)

    alerts = []
    for sid, bracket_bss in by_station.items():
        if len(bracket_bss) < 3:
            continue

        # Sort by bracket order
        sorted_pairs = sorted(bracket_bss.items(), key=lambda x: bracket_order.get(x[0], 99))
        bss_vals = [v for _, v in sorted_pairs]

        # Check monotone decrease
        if all(bss_vals[i] >= bss_vals[i + 1] for i in range(len(bss_vals) - 1)):
            if len(bss_vals) >= 2:
                slope = (bss_vals[-1] - bss_vals[0]) / (len(bss_vals) - 1)
                if slope < -0.05:
                    alerts.append({
                        "alert_type": "BSS_DIAGONAL_DEGRADATION",
                        "station_id": sid,
                        "severity_score": 0.4,
                        "details": {
                            "pattern": "diagonal_degradation",
                            "station_id": sid,
                            "slope": round(slope, 4),
                            "brackets": dict(sorted_pairs),
                        },
                    })
    return alerts


def _check_weight_convergence(conn: Any) -> list[dict]:
    """Pattern 4: Single model dominates (max weight > 0.60). §8.6.4."""
    alerts = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT STATION_ID, LEAD_BRACKET, SOURCE_ID, W_M
                FROM (
                  SELECT STATION_ID, LEAD_BRACKET, SOURCE_ID, W_M,
                         ROW_NUMBER() OVER (
                           PARTITION BY STATION_ID, LEAD_BRACKET
                           ORDER BY W_M DESC
                         ) AS rn
                  FROM MODEL_WEIGHTS
                  WHERE COMPUTED_AT >= SYSTIMESTAMP - INTERVAL '7' DAY
                ) WHERE rn = 1 AND W_M > 0.60
            """)
            for row in cur:
                alerts.append({
                    "alert_type": "BSS_WEIGHT_CONVERGENCE",
                    "station_id": row[0],
                    "source_id": row[2],
                    "severity_score": 0.3,
                    "details": {
                        "pattern": "weight_convergence",
                        "station_id": row[0],
                        "lead_bracket": row[1],
                        "dominant_source": row[2],
                        "weight": round(float(row[3]), 4),
                    },
                })
    except Exception as e:
        log.warning("[patterns] weight convergence check failed: %s", e)

    return alerts
