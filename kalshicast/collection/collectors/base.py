"""Shared helpers for weather data collectors."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def to_float(x: Any) -> Optional[float]:
    """Safely convert a value to float, returning None on failure."""
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def reindex_axis(axis: List[str], m: Dict[str, float]) -> List[Optional[float]]:
    """Map a dict keyed by time-string onto a uniform time axis."""
    return [float(m[t]) if t in m else None for t in axis]


def backfill_daily_from_hourly_temps(
    target_dates: List[str],
    axis: List[str],
    temps: List[Optional[float]],
    daily_by_date: Dict[str, Dict[str, Optional[float]]],
) -> None:
    """Derive daily high/low from hourly temps when daily data is missing."""
    if not axis or not temps or len(axis) != len(temps):
        return

    per: Dict[str, List[float]] = {}
    for t, v in zip(axis, temps):
        if v is None:
            continue
        d = t[:10]
        if d in target_dates:
            per.setdefault(d, []).append(float(v))

    for d in target_dates:
        rec = daily_by_date.setdefault(d, {"high_f": None, "low_f": None})
        if rec.get("high_f") is not None and rec.get("low_f") is not None:
            continue
        vals = per.get(d) or []
        if not vals:
            continue
        if rec.get("high_f") is None:
            rec["high_f"] = max(vals)
        if rec.get("low_f") is None:
            rec["low_f"] = min(vals)
