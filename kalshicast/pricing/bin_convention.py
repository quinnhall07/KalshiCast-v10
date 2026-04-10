"""Bin boundary conventions for Kalshi temperature contracts.

Spec §6.2: ticker → [bin_lower, bin_upper) half-open intervals.

Kalshi ticker format: KX{HIGH|LOW}{CITY}-{YY}{MMM}{DD}-{BIN}
    Examples: KXHIGHNYC-26APR08-B75, KXLOWMIA-26MAR23-T80ABOVE

The city code comes from STATIONS.cli_site (e.g., NYC, MIA, MDW).
"""

from __future__ import annotations

import math
import re
from datetime import datetime


_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
           "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

_MONTH_TO_NUM = {m: i + 1 for i, m in enumerate(_MONTHS)}


def _format_kalshi_date(target_date: str) -> str:
    """Convert 'YYYY-MM-DD' → 'YYMMMDD' (e.g., '2026-04-08' → '26APR08')."""
    dt = datetime.strptime(str(target_date)[:10], "%Y-%m-%d")
    return f"{dt.year % 100:02d}{_MONTHS[dt.month - 1]}{dt.day:02d}"


def ticker_to_boundaries(ticker: str) -> tuple[float, float] | None:
    """Parse a Kalshi ticker string to (bin_lower, bin_upper).

    Handles both Kalshi native format and legacy internal format:
        KXHIGHNYC-26APR08-B75   → center=75, bin [74.5, 76.5)
        KXHIGH-KNYC-20260408-B75 → same (legacy, for backward compat)
        KXLOWMIA-26APR08-T80ABOVE → above=80, bin [79.5, +∞)

    Returns None if ticker format not recognized.
    """
    # Extract bin label from the end (works for both formats)
    m = re.search(r'-(B\d+)$', ticker)
    if m:
        center = int(m.group(1)[1:])
        return (center - 0.5, center + 1.5)

    m = re.search(r'-(T\d+)(ABOVE|BELOW)?$', ticker, re.IGNORECASE)
    if m:
        val = int(m.group(1)[1:])
        direction = (m.group(2) or "ABOVE").upper()
        if direction == "BELOW":
            return (float('-inf'), val + 0.5)
        else:
            return (val - 0.5, float('inf'))

    return None


def generate_station_bins(station_id: str, target_date: str,
                          mu: float, target_type: str,
                          cli_site: str | None = None,
                          bin_width: float = 2.0,
                          n_bins: int = 15) -> list[dict]:
    """Generate bins with Kalshi-native ticker format.

    Ticker format: KX{HIGH|LOW}{CITY}-{YYMMMDD}-B{VAL}
        e.g., KXHIGHNYC-26APR08-B75

    Args:
        station_id: ICAO code, e.g., "KNYC"
        target_date: "YYYY-MM-DD"
        mu: Kalman-corrected expected temperature (°F)
        target_type: "HIGH" or "LOW"
        cli_site: Kalshi city code, e.g., "NYC". If None, strips K-prefix
                  from station_id as fallback.
        bin_width: Width of each bin in °F (default 2.0)
        n_bins: Total number of bins including tail bins (default 15)
    """
    # Resolve city code for Kalshi ticker
    if cli_site:
        city = cli_site
    else:
        # Fallback: strip K-prefix from ICAO code
        city = station_id.lstrip("K")

    date_part = _format_kalshi_date(target_date)
    prefix = f"KX{target_type.upper()}"

    # Round mu to nearest even integer for clean bin alignment
    center = round(mu / bin_width) * bin_width

    half = (n_bins - 2) // 2  # reserve 2 for tail bins
    bins = []

    # Low tail bin
    low_edge = center - half * bin_width - 0.5
    val = int(low_edge + 0.5)
    bins.append({
        "ticker": f"{prefix}{city}-{date_part}-T{val}BELOW",
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
            "ticker": f"{prefix}{city}-{date_part}-B{b_val}",
            "bin_lower": lo,
            "bin_upper": hi,
            "is_tail_low": False,
            "is_tail_high": False,
        })

    # High tail bin
    high_edge = center + half * bin_width + bin_width - 0.5
    val = int(high_edge - 0.5)
    bins.append({
        "ticker": f"{prefix}{city}-{date_part}-T{val}ABOVE",
        "bin_lower": high_edge,
        "bin_upper": float('inf'),
        "is_tail_low": False,
        "is_tail_high": True,
    })

    return bins