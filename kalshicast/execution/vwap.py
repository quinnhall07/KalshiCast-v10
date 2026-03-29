"""VWAP computation, staleness detection, tranche splitting.

Spec §7.6: Volume-Weighted Average Price from order book depth.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float, get_param_int

log = logging.getLogger(__name__)


def compute_vwap(orderbook: dict, contracts_needed: int) -> tuple[float, int]:
    """Compute volume-weighted average price from order book.

    Walks YES book levels from best ask upward until contracts are filled.
    Returns (c_vwap, depth_available).
    """
    yes_book = orderbook.get("yes", [])
    if not yes_book:
        return 0.0, 0

    total_cost = 0.0
    total_qty = 0
    remaining = contracts_needed

    for level in yes_book:
        price = level.get("price", 0) / 100.0  # Kalshi prices in cents
        qty = level.get("quantity", 0)

        fill = min(qty, remaining)
        total_cost += price * fill
        total_qty += fill
        remaining -= fill

        if remaining <= 0:
            break

    if total_qty == 0:
        return 0.0, 0

    c_vwap = total_cost / total_qty
    return round(c_vwap, 4), total_qty


def check_staleness(c_vwap: float, c_best: float) -> dict:
    """Check if order book is stale.

    Alert if |c_VWAP - c_best| > staleness_delta.
    Abort if delta > 2 × staleness_delta.
    """
    staleness_delta = get_param_float("vwap.staleness_delta")

    delta = abs(c_vwap - c_best)
    alert = delta > staleness_delta
    abort = delta > 2 * staleness_delta

    return {
        "delta": round(delta, 4),
        "alert": alert,
        "abort": abort,
        "c_vwap": c_vwap,
        "c_best": c_best,
    }


def split_tranches(contracts: int) -> list[int]:
    """Split large orders into tranches.

    Returns list of tranche sizes. Single tranche if below threshold.
    """
    threshold = get_param_int("vwap.tranche_threshold")
    tranche_size = get_param_int("vwap.tranche_size")

    if contracts <= threshold:
        return [contracts]

    tranches = []
    remaining = contracts
    while remaining > 0:
        t = min(tranche_size, remaining)
        tranches.append(t)
        remaining -= t

    return tranches
