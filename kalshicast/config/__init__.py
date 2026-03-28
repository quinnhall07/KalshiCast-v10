"""Configuration layer — stations, sources, and tunable parameters."""

from kalshicast.config.stations import STATIONS, get_stations, get_station
from kalshicast.config.sources import SOURCES, get_enabled_sources
from kalshicast.config.params_bootstrap import get_param, get_param_int, get_param_float

HEADERS = {
    "User-Agent": "KalshiCast/10.0 (contact: kalshicast@example.com)"
}

__all__ = [
    "STATIONS", "get_stations", "get_station",
    "SOURCES", "get_enabled_sources",
    "get_param", "get_param_int", "get_param_float",
    "HEADERS",
]
