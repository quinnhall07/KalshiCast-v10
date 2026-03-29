"""Bimodal regime detection — K-means(k=2) on forecast values.

Spec §5.7: Trigger when IQR/S > bimodal_iqr_threshold (default 1.35).
Method: K-means(k=2), confirm if centroid distance > min_centroid_dist × S.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float, get_param_int

log = logging.getLogger(__name__)


def _iqr(values: list[float]) -> float:
    """Compute interquartile range."""
    if len(values) < 4:
        return 0.0
    s = sorted(values)
    n = len(s)
    q1 = s[n // 4]
    q3 = s[(3 * n) // 4]
    return q3 - q1


def _kmeans_2(values: list[float], max_iter: int = 50) -> tuple[float, float, int, int]:
    """Simple K-means with k=2 on 1D data.

    Returns (centroid_1, centroid_2, size_1, size_2) where centroid_1 < centroid_2.
    """
    if len(values) < 2:
        v = values[0] if values else 0.0
        return v, v, len(values), 0

    s = sorted(values)
    c1 = s[len(s) // 4]
    c2 = s[(3 * len(s)) // 4]

    for _ in range(max_iter):
        cluster1: list[float] = []
        cluster2: list[float] = []

        for v in values:
            if abs(v - c1) <= abs(v - c2):
                cluster1.append(v)
            else:
                cluster2.append(v)

        if not cluster1 or not cluster2:
            break

        new_c1 = sum(cluster1) / len(cluster1)
        new_c2 = sum(cluster2) / len(cluster2)

        if abs(new_c1 - c1) < 1e-6 and abs(new_c2 - c2) < 1e-6:
            c1, c2 = new_c1, new_c2
            break

        c1, c2 = new_c1, new_c2

    # Re-assign final clusters
    size1 = sum(1 for v in values if abs(v - c1) <= abs(v - c2))
    size2 = len(values) - size1

    if c1 > c2:
        c1, c2 = c2, c1
        size1, size2 = size2, size1

    return c1, c2, size1, size2


def detect_bimodal(forecasts: list[float], s_tk: float) -> dict | None:
    """Detect bimodal regime in forecast distribution.

    Steps:
    1. Compute IQR of forecasts
    2. If IQR/S > threshold, run K-means(k=2)
    3. Confirm if centroid distance > min_centroid_dist × S
    4. Return bimodal params or None

    Parameters:
        forecasts: list of forecast values (e.g., model predictions in °F)
        s_tk: ensemble spread (standard deviation)

    Returns:
        dict with centroid_1, centroid_2, cluster_size_1, cluster_size_2, iqr_ratio
        or None if not bimodal.
    """
    iqr_threshold = get_param_float("regime.bimodal_iqr_threshold")
    min_centroid_dist = get_param_float("regime.min_centroid_dist")
    min_cluster_frac = get_param_float("regime.min_cluster_frac")

    if len(forecasts) < 4 or s_tk <= 0:
        return None

    iqr = _iqr(forecasts)
    iqr_ratio = iqr / s_tk

    if iqr_ratio <= iqr_threshold:
        return None

    c1, c2, size1, size2 = _kmeans_2(forecasts)
    centroid_dist = abs(c2 - c1)

    if centroid_dist < min_centroid_dist * s_tk:
        log.debug("[regime] IQR/S=%.2f triggered but centroid dist %.2f < %.2f×S",
                  iqr_ratio, centroid_dist, min_centroid_dist)
        return None

    total = size1 + size2
    if total == 0:
        return None

    smaller_frac = min(size1, size2) / total
    if smaller_frac < min_cluster_frac:
        log.debug("[regime] bimodal rejected: smaller cluster %.1f%% < %.1f%% min",
                  smaller_frac * 100, min_cluster_frac * 100)
        return None

    result = {
        "centroid_1": round(c1, 2),
        "centroid_2": round(c2, 2),
        "cluster_size_1": size1,
        "cluster_size_2": size2,
        "centroid_distance": round(centroid_dist, 2),
        "iqr_ratio": round(iqr_ratio, 3),
    }

    log.info("[regime] BIMODAL detected: c1=%.1f c2=%.1f dist=%.1f IQR/S=%.2f sizes=%d/%d",
             c1, c2, centroid_dist, iqr_ratio, size1, size2)

    return result


def flag_regime(conn: Any, station_id: str, target_type: str,
                target_date: str, bimodal: dict, run_id: str) -> None:
    """Write bimodal regime detection result to REGIME_FLAGS table."""
    with conn.cursor() as cur:
        cur.execute("""
            MERGE INTO REGIME_FLAGS tgt
            USING DUAL ON (
                tgt.STATION_ID = :sid AND tgt.TARGET_TYPE = :tt
                AND tgt.TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD')
            )
            WHEN MATCHED THEN UPDATE SET
                IS_BIMODAL = 1,
                CENTROID_1 = :c1, CENTROID_2 = :c2,
                CLUSTER_SIZE_1 = :s1, CLUSTER_SIZE_2 = :s2,
                IQR_RATIO = :iqr, CENTROID_DISTANCE = :dist,
                PIPELINE_RUN_ID = :rid,
                UPDATED_AT = SYSTIMESTAMP
            WHEN NOT MATCHED THEN INSERT (
                STATION_ID, TARGET_TYPE, TARGET_DATE, IS_BIMODAL,
                CENTROID_1, CENTROID_2, CLUSTER_SIZE_1, CLUSTER_SIZE_2,
                IQR_RATIO, CENTROID_DISTANCE, PIPELINE_RUN_ID
            ) VALUES (
                :sid, :tt, TO_DATE(:td, 'YYYY-MM-DD'), 1,
                :c1, :c2, :s1, :s2, :iqr, :dist, :rid
            )
        """, {
            "sid": station_id, "tt": target_type, "td": target_date,
            "c1": bimodal["centroid_1"], "c2": bimodal["centroid_2"],
            "s1": bimodal["cluster_size_1"], "s2": bimodal["cluster_size_2"],
            "iqr": bimodal["iqr_ratio"], "dist": bimodal["centroid_distance"],
            "rid": run_id,
        })
