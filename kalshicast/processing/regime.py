"""Bimodal regime detection — STUB.

Spec §5.7: Architecture defined but not live until 90 days of validation data.
Trigger: IQR/S > bimodal_iqr_threshold (default 1.35).
Method: K-means(k=2) on forecast values.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def detect_bimodal(forecasts: list[float], s_tk: float) -> dict | None:
    """Detect bimodal regime. STUB — returns None (not activated).

    When live (Phase 3+):
    1. Compute IQR of forecasts
    2. If IQR/S > threshold, run K-means(k=2)
    3. Confirm if centroid distance > min × S
    4. Return {centroid_1, centroid_2, cluster_size_1, cluster_size_2}
    """
    return None
