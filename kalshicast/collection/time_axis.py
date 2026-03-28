"""Hourly/daily forecast axis generation.

Ported from utils/time_axis.py — import paths updated, logic unchanged.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import List, Set, Tuple
from zoneinfo import ZoneInfo


def _utc_now_trunc_hour() -> datetime:
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _dt_to_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def build_hourly_axis_z(days: int, *, start_utc: datetime | None = None) -> List[str]:
    d = int(days) if isinstance(days, int) else 1
    if d < 1:
        d = 1

    t0 = start_utc
    if t0 is None:
        t0 = _utc_now_trunc_hour()
    if t0.tzinfo is None:
        t0 = t0.replace(tzinfo=timezone.utc)
    t0 = t0.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

    n = d * 24
    return [_dt_to_z(t0 + timedelta(hours=i)) for i in range(n)]


def hourly_axis_set(axis: List[str]) -> Set[str]:
    return set(axis or [])


def axis_start_end(axis: List[str]) -> Tuple[datetime, datetime]:
    if not axis:
        t0 = _utc_now_trunc_hour()
        return t0, t0

    def _parse_z(s: str) -> datetime:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

    start = _parse_z(axis[0])
    end = _parse_z(axis[-1])
    return start, end


def daily_targets_from_axis(axis: List[str], tz_name: str = "UTC") -> List[str]:
    """Produce daily target dates (YYYY-MM-DD) based on the station's LOCAL timezone."""
    if not axis:
        return []

    seen: set[str] = set()
    out: List[str] = []
    tz = ZoneInfo(tz_name)

    for t in axis:
        if t.endswith("Z"):
            dt_utc = datetime.fromisoformat(t[:-1] + "+00:00")
        else:
            dt_utc = datetime.fromisoformat(t)
            if dt_utc.tzinfo is None:
                dt_utc = dt_utc.replace(tzinfo=timezone.utc)

        dt_local = dt_utc.astimezone(tz)
        d = dt_local.date().isoformat()

        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def build_daily_targets(days: int, tz_name: str = "UTC", *, start_utc: datetime | None = None) -> List[str]:
    axis = build_hourly_axis_z(days, start_utc=start_utc)
    targets = daily_targets_from_axis(axis, tz_name)
    if len(targets) > days:
        targets = targets[:days]
    return targets


def truncate_issued_at_to_hour_z(ts: str | datetime | None) -> str | None:
    if ts is None:
        return None

    dt: datetime | None = None

    if isinstance(ts, datetime):
        dt = ts
    elif isinstance(ts, str):
        s = ts.strip()
        if not s:
            return None
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
        except Exception:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    dt = dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return _dt_to_z(dt)
