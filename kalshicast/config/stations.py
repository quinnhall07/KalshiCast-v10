"""20 active NWS ASOS observation stations with Kalshi market mappings."""

from __future__ import annotations

from typing import Any

STATIONS: list[dict[str, Any]] = [
    {
        "station_id": "KNYC",
        "cli_site": "NYC",
        "kalshi_high_series": "KXHIGHNY",
        "kalshi_low_series": "KXLOWNY",
        "name": "NYC Central Park",
        "state": "NY",
        "city": "New York City",
        "timezone": "America/New_York",
        "lat": 40.78,
        "lon": -73.97,
        "elevation_ft": 154,
        "wfo_id": "OKX",
        "is_active": True,
    },
    {
        "station_id": "KMIA",
        "cli_site": "MIA",
        "kalshi_high_series": "KXHIGHMIA",
        "kalshi_low_series": "KXLOWMIA",
        "name": "Miami International Airport",
        "state": "FL",
        "city": "Miami",
        "timezone": "America/New_York",
        "lat": 25.79,
        "lon": -80.32,
        "elevation_ft": 10,
        "wfo_id": "MFL",
        "is_active": True,
    },
    {
        "station_id": "KMSY",
        "cli_site": "MSY",
        "kalshi_high_series": "KXHIGHTNOLA",
        "kalshi_low_series": "KXLOWTNOLA",
        "name": "New Orleans International Airport",
        "state": "LA",
        "city": "New Orleans",
        "timezone": "America/Chicago",
        "lat": 29.99,
        "lon": -90.25,
        "elevation_ft": 3,
        "wfo_id": "LIX",
        "is_active": True,
    },
    {
        "station_id": "KPHL",
        "cli_site": "PHL",
        "kalshi_high_series": "KXHIGHPHIL",
        "kalshi_low_series": "KXLOWTPHIL",
        "name": "Philadelphia International Airport",
        "state": "PA",
        "city": "Philadelphia",
        "timezone": "America/New_York",
        "lat": 39.87,
        "lon": -75.23,
        "elevation_ft": 7,
        "wfo_id": "PHI",
        "is_active": True,
    },
    {
        "station_id": "KMDW",
        "cli_site": "MDW",
        "kalshi_high_series": "KXHIGHCHI",
        "kalshi_low_series": "KXLOWCHI",
        "name": "Chicago Midway Airport",
        "state": "IL",
        "city": "Chicago",
        "timezone": "America/Chicago",
        "lat": 41.78,
        "lon": -87.76,
        "elevation_ft": 617,
        "wfo_id": "LOT",
        "is_active": True,
    },
    {
        "station_id": "KLAX",
        "cli_site": "LAX",
        "kalshi_high_series": "KXHIGHLAX",
        "kalshi_low_series": "KXLOWLAX",
        "name": "Los Angeles International Airport",
        "state": "CA",
        "city": "Los Angeles",
        "timezone": "America/Los_Angeles",
        "lat": 33.93806,
        "lon": -118.38889,
        "elevation_ft": 125,
        "wfo_id": "LOX",
        "is_active": True,
    },
    {
        "station_id": "KAUS",
        "cli_site": "AUS",
        "kalshi_high_series": "KXHIGHAUS",
        "kalshi_low_series": "KXLOWAUS",
        "name": "Austin-Bergstrom International Airport",
        "state": "TX",
        "city": "Austin",
        "timezone": "America/Chicago",
        "lat": 30.18,
        "lon": -97.68,
        "elevation_ft": 486,
        "wfo_id": "EWX",
        "is_active": True,
    },
    {
        "station_id": "KDEN",
        "cli_site": "DEN",
        "kalshi_high_series": "KXHIGHDEN",
        "kalshi_low_series": "KXLOWDEN",
        "name": "Denver International Airport",
        "state": "CO",
        "city": "Denver",
        "timezone": "America/Denver",
        "lat": 39.85,
        "lon": -104.66,
        "elevation_ft": 5404,
        "wfo_id": "BOU",
        "is_active": True,
    },
    {
        "station_id": "KSEA",
        "cli_site": "SEA",
        "kalshi_high_series": "KXHIGHTSEA",
        "kalshi_low_series": "KXLOWTSEA",
        "name": "Seattle-Tacoma International Airport",
        "state": "WA",
        "city": "Seattle",
        "timezone": "America/Los_Angeles",
        "lat": 47.44472,
        "lon": -122.31361,
        "elevation_ft": 427,
        "wfo_id": "SEW",
        "is_active": True,
    },
    {
        "station_id": "KLAS",
        "cli_site": "LAS",
        "kalshi_high_series": "KXHIGHTLV",
        "kalshi_low_series": "KXLOWTLV",
        "name": "Harry Reid International Airport",
        "state": "NV",
        "city": "Las Vegas",
        "timezone": "America/Los_Angeles",
        "lat": 36.07188,
        "lon": -115.1634,
        "elevation_ft": 2180,
        "wfo_id": "VEF",
        "is_active": True,
    },
    {
        "station_id": "KSFO",
        "cli_site": "SFO",
        "kalshi_high_series": "KXHIGHTSFO",
        "kalshi_low_series": "KXLOWTSFO",
        "name": "San Francisco International Airport",
        "state": "CA",
        "city": "San Francisco",
        "timezone": "America/Los_Angeles",
        "lat": 37.61961,
        "lon": -122.36558,
        "elevation_ft": 10,
        "wfo_id": "MTR",
        "is_active": True,
    },
    {
        "station_id": "KDCA",
        "cli_site": "DCA",
        "kalshi_high_series": "KXHIGHTDC",
        "kalshi_low_series": "KXLOWTDC",
        "name": "Reagan National Airport",
        "state": "VA",
        "city": "Washington DC",
        "timezone": "America/New_York",
        "lat": 38.85,
        "lon": -77.03,
        "elevation_ft": 13,
        "wfo_id": "LWX",
        "is_active": True,
    },
    {
        "station_id": "KBOS",
        "cli_site": "BOS",
        "kalshi_high_series": "KXHIGHTBOS",
        "kalshi_low_series": "KXLOWTBOS",
        "name": "Logan International Airport",
        "state": "MA",
        "city": "Boston",
        "timezone": "America/New_York",
        "lat": 42.36,
        "lon": -71.01,
        "elevation_ft": 20,
        "wfo_id": "BOX",
        "is_active": True,
    },
    {
        "station_id": "KATL",
        "cli_site": "ATL",
        "kalshi_high_series": "KXHIGHTATL",
        "kalshi_low_series": "KXLOWTATL",
        "name": "Jackson Atlanta International Airport",
        "state": "GA",
        "city": "Atlanta",
        "timezone": "America/New_York",
        "lat": 33.64,
        "lon": -84.43,
        "elevation_ft": 1027,
        "wfo_id": "FFC",
        "is_active": True,
    },
    {
        "station_id": "KPHX",
        "cli_site": "PHX",
        "kalshi_high_series": "KXHIGHTPHX",
        "kalshi_low_series": "KXLOWTPHX",
        "name": "Phoenix Sky Harbor International Airport",
        "state": "AZ",
        "city": "Phoenix",
        "timezone": "America/Phoenix",
        "lat": 33.427799,
        "lon": -112.003465,
        "elevation_ft": 1115,
        "wfo_id": "PSR",
        "is_active": True,
    },
    {
        "station_id": "KSAT",
        "cli_site": "SAT",
        "kalshi_high_series": "KXHIGHTSATX",
        "kalshi_low_series": "KXLOWTSATX",
        "name": "San Antonio International Airport",
        "state": "TX",
        "city": "San Antonio",
        "timezone": "America/Chicago",
        "lat": 29.53,
        "lon": -98.46,
        "elevation_ft": 807,
        "wfo_id": "EWX",
        "is_active": True,
    },
    {
        "station_id": "KDFW",
        "cli_site": "DFW",
        "kalshi_high_series": "KXHIGHTDAL",
        "kalshi_low_series": "KXLOWTDAL",
        "name": "Dallas/Fort Worth International Airport",
        "state": "TX",
        "city": "Dallas",
        "timezone": "America/Chicago",
        "lat": 32.9,
        "lon": -97.02,
        "elevation_ft": 541,
        "wfo_id": "FWD",
        "is_active": True,
    },
    {
        "station_id": "KMSP",
        "cli_site": "MSP",
        "kalshi_high_series": "KXHIGHTMIN",
        "kalshi_low_series": "KXLOWTMIN",
        "name": "Minneapolis-St. Paul International Airport",
        "state": "MN",
        "city": "Minneapolis",
        "timezone": "America/Chicago",
        "lat": 44.88,
        "lon": -93.23,
        "elevation_ft": 541,
        "wfo_id": "MPX",
        "is_active": True,
    },
    {
        "station_id": "KHOU",
        "cli_site": "HOU",
        "kalshi_high_series": "KXHIGHHOU",
        "kalshi_low_series": "KXLOWTHOU",
        "name": "Houston Hobby Airport",
        "state": "TX",
        "city": "Houston",
        "timezone": "America/Chicago",
        "lat": 29.64,
        "lon": -95.28,
        "elevation_ft": 46,
        "wfo_id": "HGX",
        "is_active": True,
    },
    {
        "station_id": "KOKC",
        "cli_site": "OKC",
        "kalshi_high_series": "KXHIGHTOKC",
        "kalshi_low_series": "KXLOWTOKC",
        "name": "Will Rogers World Airport",
        "state": "OK",
        "city": "Oklahoma City",
        "timezone": "America/Chicago",
        "lat": 35.39,
        "lon": -97.6,
        "elevation_ft": 1293,
        "wfo_id": "OUN",
        "is_active": True,
    },
]


# ─────────────────────────────────────────────────────────────────────
# Lookup Functions
# ─────────────────────────────────────────────────────────────────────

def get_stations(*, active_only: bool = True) -> list[dict[str, Any]]:
    """Return list of station configs."""
    if active_only:
        return [s for s in STATIONS if s.get("is_active")]
    return list(STATIONS)


def get_station(station_id: str) -> dict[str, Any]:
    """Get station config by station_id. Raises KeyError if not found."""
    for s in STATIONS:
        if s["station_id"] == station_id:
            return s
    raise KeyError(f"Unknown station: {station_id}")


def get_station_by_kalshi_series(series_ticker: str) -> dict[str, Any] | None:
    """Find station by Kalshi series ticker (e.g., KXHIGHNY, KXLOWTSFO).
    
    Checks both kalshi_high_series and kalshi_low_series.
    Returns None if no match.
    """
    series_upper = series_ticker.upper()
    for s in STATIONS:
        if s.get("kalshi_high_series", "").upper() == series_upper:
            return s
        if s.get("kalshi_low_series", "").upper() == series_upper:
            return s
    return None


def get_all_kalshi_series() -> list[str]:
    """Return all known Kalshi series tickers for active stations."""
    series = []
    for s in STATIONS:
        if not s.get("is_active"):
            continue
        if s.get("kalshi_high_series"):
            series.append(s["kalshi_high_series"])
        if s.get("kalshi_low_series"):
            series.append(s["kalshi_low_series"])
    return series


# Legacy compatibility
def get_kalshi_city(station_id: str) -> str:
    """Deprecated: Get Kalshi city code. Use kalshi_high_series/kalshi_low_series instead."""
    station = get_station(station_id)
    # Extract city from series for backward compatibility
    high_series = station.get("kalshi_high_series", "")
    if high_series.startswith("KXHIGHT"):
        return high_series[7:]
    elif high_series.startswith("KXHIGH"):
        return high_series[6:]
    return station["cli_site"]


def get_station_by_kalshi_city(kalshi_city: str) -> dict[str, Any] | None:
    """Deprecated: Find station by Kalshi city code."""
    for s in STATIONS:
        city = get_kalshi_city(s["station_id"])
        if city.upper() == kalshi_city.upper():
            return s
    return None