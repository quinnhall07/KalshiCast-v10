"""Lead-time computation and bracket classification.

Ported from etl_utils.py with added classify_lead_hours() from v10 spec.
"""

from __future__ import annotations

from datetime import datetime, date, time, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from kalshicast.config.params_bootstrap import get_param_int, get_param_float


def utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_dt(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_local_date(target_date: str) -> date:
    return date.fromisoformat(target_date[:10])


def compute_lead_hours(
    *,
    station_tz: str,
    issued_at: str,
    target_date: str,
    kind: str,
    hourly_rows: List[Dict[str, Any]] | None = None,
) -> float:
    """Dynamically calculates lead time based on the projected hour of the high/low.

    If no hourly data matches, falls back to static anchor hours.
    """
    tz = ZoneInfo(station_tz)
    issued = parse_iso_dt(issued_at)

    target_local_dt = None

    if hourly_rows:
        valid_hours = []
        for row in hourly_rows:
            vt_str = row.get("valid_time")
            temp = row.get("temperature_f")

            if not vt_str or temp is None:
                continue

            dt_utc = parse_iso_dt(vt_str)
            dt_local = dt_utc.astimezone(tz)

            if dt_local.date().isoformat() == target_date:
                valid_hours.append((dt_local, temp))

        if valid_hours:
            if kind == "high":
                best = max(valid_hours, key=lambda x: x[1])
            else:
                best = min(valid_hours, key=lambda x: x[1])
            target_local_dt = best[0]

    if not target_local_dt:
        d = to_local_date(target_date)
        anchor_hour = (
            get_param_int("lead.target_local_hour_high")
            if kind == "high"
            else get_param_int("lead.target_local_hour_low")
        )
        target_local_dt = datetime.combine(d, time(anchor_hour, 0), tzinfo=tz)

    target_utc = target_local_dt.astimezone(timezone.utc)
    lead = (target_utc - issued).total_seconds() / 3600.0

    return lead


def classify_lead_hours(hours: float) -> str:
    """Map continuous float hours to lead-time bracket h1–h5.

    h1: [0, 12)  h2: [12, 24)  h3: [24, 48)  h4: [48, 72)  h5: [72, 120)
    """
    h1 = get_param_float("lead.h1_max")
    h2 = get_param_float("lead.h2_max")
    h3 = get_param_float("lead.h3_max")
    h4 = get_param_float("lead.h4_max")

    if hours < h1:
        return "h1"
    if hours < h2:
        return "h2"
    if hours < h3:
        return "h3"
    if hours < h4:
        return "h4"
    return "h5"
