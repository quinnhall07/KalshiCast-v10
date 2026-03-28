"""Abstract collector interface and ForecastBundle type."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DailyRow:
    target_date: str          # YYYY-MM-DD
    high_f: float | None
    low_f: float | None
    lead_hours_high: float | None = None
    lead_hours_low: float | None = None


@dataclass
class HourlyRow:
    valid_time_utc: str       # ISO-8601 UTC
    temperature_f: float | None = None
    dewpoint_f: float | None = None
    humidity_pct: float | None = None
    wind_speed_mph: float | None = None
    wind_dir_deg: int | None = None
    cloud_cover_pct: int | None = None
    precip_prob_pct: int | None = None
    precip_type_code: int | None = None


@dataclass
class ForecastBundle:
    source_id: str
    station_id: str
    issued_at: str            # ISO-8601 UTC
    init_time: str | None = None
    daily: list[DailyRow] = field(default_factory=list)
    hourly: list[HourlyRow] = field(default_factory=list)


def validate_bundle(b: ForecastBundle) -> list[str]:
    """Return list of warnings (empty = valid)."""
    warnings = []
    if not b.source_id:
        warnings.append("missing source_id")
    if not b.station_id:
        warnings.append("missing station_id")
    if not b.issued_at:
        warnings.append("missing issued_at")
    if len(b.daily) == 0:
        warnings.append("no daily rows")
    return warnings
