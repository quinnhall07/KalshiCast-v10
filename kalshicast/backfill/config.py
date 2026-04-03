# kalshicast/backfill/config.py
"""Backfill configuration — date window, source list, station mappings."""
from __future__ import annotations
from datetime import date

# ── Date window ───────────────────────────────────────────────────────────────
# Start 2 years back. End: yesterday (observations may not yet be final for today).
from datetime import date, timedelta

BACKFILL_END   = date.today() - timedelta(days=1)
BACKFILL_START = date(BACKFILL_END.year - 2, BACKFILL_END.month, BACKFILL_END.day)

# ── Iowa State ASOS station IDs ───────────────────────────────────────────────
# Mesonet uses ICAO IDs with the leading K stripped for CONUS stations.
# e.g., KNYC → "NYC"  (note: some airport codes differ from CLI site codes)
STATION_ASOS_MAP: dict[str, str] = {
    "KNYC": "NYC",
    "KMIA": "MIA",
    "KMSY": "MSY",
    "KPHL": "PHL",
    "KMDW": "MDW",
    "KLAX": "LAX",
    "KAUS": "AUS",
    "KDEN": "DEN",
    "KSEA": "SEA",
    "KLAS": "LAS",
    "KSFO": "SFO",
    "KDCA": "DCA",
    "KBOS": "BOS",
    "KATL": "ATL",
    "KPHX": "PHX",
    "KSAT": "SAT",
    "KDFW": "DFW",
    "KMSP": "MSP",
    "KHOU": "HOU",
    "KOKC": "OKC",
}

# ── Open-Meteo Historical Forecast API models ────────────────────────────────
# Each entry maps to a live SOURCES entry so the same source_id is used in the
# FORECAST_RUNS table. The `models` param is the OME API model selector.
OME_HISTORICAL_MODELS: list[dict] = [
    {"source_id": "OME_BASE",  "models": "best_match"},
    {"source_id": "OME_GFS",   "models": "gfs_seamless"},
    {"source_id": "OME_EC",    "models": "ecmwf_ifs025"},
    {"source_id": "OME_ICON",  "models": "icon_seamless"},
    {"source_id": "OME_GEM",   "models": "gem_seamless"},
]

# Base URL for historical forecast API (different from live forecast URL)
OME_HISTORICAL_BASE_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

# ── Lead time approximation ───────────────────────────────────────────────────
# When we load a forecast issued on date D for target_date T,
# the approximate lead hours = (T - D).days * 24 + 12 hours.
# We issue at noon UTC: issued_at = D 12:00 UTC.
# We simulate two lead-time passes:
#   PASS_OFFSETS = [1, 2, 3] means we treat each historical forecast
#   as having been issued 1, 2, and 3 days before the target date.
#   This populates h2, h3, h4 brackets respectively.
BACKFILL_LEAD_OFFSETS_DAYS = [1, 2, 3]   # days before target_date

# ── Iowa ASOS API ─────────────────────────────────────────────────────────────
ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
ASOS_CHUNK_DAYS = 365    # pull 1 year per API call to stay within response limits

# ── NWS CLI product archive ───────────────────────────────────────────────────
NWS_PRODUCTS_URL = "https://api.weather.gov/products/types/CLI/locations"
CLI_MAX_PRODUCTS_PER_STATION = 1000   # walk back this many CLI products

# ── Concurrency / rate limits ─────────────────────────────────────────────────
BACKFILL_MAX_WORKERS = 4    # threads for concurrent station fetches
ASOS_REQUEST_SLEEP   = 0.5  # seconds between Iowa ASOS requests
OME_REQUEST_SLEEP    = 1.0  # seconds between OME historical requests
NWS_REQUEST_SLEEP    = 0.3  # seconds between NWS product fetches