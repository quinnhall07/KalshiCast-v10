"""Dashboard stats — rolling bias/RMSE/MAE/percentile refresh.

Thin wrapper around db.operations.update_dashboard_stats for standard windows.
"""

from __future__ import annotations

import logging
from typing import Any

from kalshicast.db.operations import update_dashboard_stats

log = logging.getLogger(__name__)

WINDOWS = [2, 3, 7, 14, 30, 90]


def refresh_dashboard_stats(conn: Any) -> None:
    """Refresh DASHBOARD_STATS for all standard windows."""
    for w in WINDOWS:
        update_dashboard_stats(conn, w)
        log.info("[dashboard] refreshed window=%d days", w)
