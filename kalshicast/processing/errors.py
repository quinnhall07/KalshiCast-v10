"""Forecast error computation — bridge between L1 data and L2 processing.

Wraps the DB MERGE that joins FORECASTS_DAILY with OBSERVATIONS
to produce FORECAST_ERRORS rows.
"""

from __future__ import annotations

import logging
from typing import Any

from kalshicast.db.operations import build_forecast_errors_for_date

log = logging.getLogger(__name__)


def build_forecast_errors(conn: Any, target_date: str, run_id: str | None = None) -> int:
    """Build FORECAST_ERRORS for a target date.

    Unpivots HIGH/LOW into separate rows.
    Error = Forecast - Observed (positive = model too warm).

    Returns count of rows inserted/updated.
    """
    n = build_forecast_errors_for_date(conn, target_date)
    if n:
        log.info("[errors] %s: wrote %d error rows", target_date, n)
    else:
        log.warning("[errors] %s: no error rows written (missing forecasts or observations?)", target_date)
    return n
