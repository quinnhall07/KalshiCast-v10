"""Bin boundary conventions for Kalshi temperature contracts.

Spec §6.2: ticker → [bin_lower, bin_upper) half-open intervals.
Paper mode generates synthetic bins centered on μ.
"""

from __future__ import annotations

import math
import re


def ticker_to_boundaries(ticker: str) -> tuple[float, float] | None:
    """Parse a Kalshi ticker string to (bin_lower, bin_upper).

    Patterns:
    - KXHIGH-KNYC-26MAR28-B75  → center=75, bin [74.5, 76.5)
    - KXHIGH-KNYC-26MAR28-T80  → above=80, bin [79.5, +∞)
    - KXLOW-KNYC-26MAR28-B30   → center=30, bin [29.5, 31.5)

    Returns None if ticker format not recognized.
    """
    # Try -B<number> (center bin, 2°F wide)
    m = re.search(r'-B(\d+)$', ticker)
    if m:
        center = int(m.group(1))
        return (center - 0.5, center + 1.5)

    # Try -T<number> (tail bin, one-sided)
    m = re.search(r'-T(\d+)(ABOVE|BELOW)?$', ticker, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        direction = (m.group(2) or "").upper()
        if direction == "BELOW":
            return (float('-inf'), val + 0.5)
        else:
            return (val - 0.5, float('inf'))

    return None


def generate_paper_bins(mu: float, target_type: str,
                        bin_width: float = 2.0,
                        n_bins: int = 15) -> list[dict]:
    """Generate synthetic bin boundaries for paper mode.

    Centered on mu with 2°F-wide bins and tail bins at edges.
    Returns list of {ticker, bin_lower, bin_upper, is_tail_low, is_tail_high}.
    """
    # Round mu to nearest even integer for clean bin alignment
    center = round(mu / bin_width) * bin_width

    # Generate interior bins
    half = (n_bins - 2) // 2  # reserve 2 for tail bins
    bins = []

    # Low tail bin
    low_edge = center - half * bin_width - 0.5
    date_stub = "PAPER"
    type_prefix = "KXHIGH" if target_type == "HIGH" else "KXLOW"

    bins.append({
        "ticker": f"{type_prefix}-PAPER-T{int(low_edge + 0.5)}BELOW",
        "bin_lower": float('-inf'),
        "bin_upper": low_edge,
        "is_tail_low": True,
        "is_tail_high": False,
    })

    # Interior bins
    for i in range(-half, half + 1):
        lo = center + i * bin_width - 0.5
        hi = lo + bin_width
        b_val = int(lo + 0.5)
        bins.append({
            "ticker": f"{type_prefix}-PAPER-B{b_val}",
            "bin_lower": lo,
            "bin_upper": hi,
            "is_tail_low": False,
            "is_tail_high": False,
        })

    # High tail bin
    high_edge = center + half * bin_width + bin_width - 0.5
    bins.append({
        "ticker": f"{type_prefix}-PAPER-T{int(high_edge - 0.5)}ABOVE",
        "bin_lower": high_edge,
        "bin_upper": float('inf'),
        "is_tail_low": False,
        "is_tail_high": True,
    })

    return bins


def generate_station_bins(station_id: str, target_date: str,
                          mu: float, target_type: str,
                          bin_width: float = 2.0,
                          n_bins: int = 15) -> list[dict]:
    """Generate bins with station-specific ticker format for paper mode.

    Ticker format: KXHIGH-{STATION}-{DATE}-B{VAL}
    """
    raw_bins = generate_paper_bins(mu, target_type, bin_width, n_bins)
    date_str = str(target_date).replace("-", "")

    type_prefix = "KXHIGH" if target_type == "HIGH" else "KXLOW"

    for b in raw_bins:
        if b["is_tail_low"]:
            val = int(b["bin_upper"] + 0.5)
            b["ticker"] = f"{type_prefix}-{station_id}-{date_str}-T{val}BELOW"
        elif b["is_tail_high"]:
            val = int(b["bin_lower"] - 0.5)
            b["ticker"] = f"{type_prefix}-{station_id}-{date_str}-T{val}ABOVE"
        else:
            val = int(b["bin_lower"] + 0.5)
            b["ticker"] = f"{type_prefix}-{station_id}-{date_str}-B{val}"

    return raw_bins
