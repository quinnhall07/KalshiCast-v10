"""9 weather model source definitions."""

from __future__ import annotations

from typing import Any

SOURCES: dict[str, dict[str, Any]] = {
    "NWS": {
        "name": "National Weather Service",
        "enabled": True,
        "module": "kalshicast.collection.collectors.collect_nws",
        "func": "fetch_nws_forecast",
        "provider_group": "NWS",
        "update_cycle_hours": 12,
    },
    "OME_BASE": {
        "name": "Open-Meteo (default)",
        "enabled": True,
        "module": "kalshicast.collection.collectors.collect_ome",
        "func": "fetch_ome_forecast",
        "params": {"model": "best"},
        "provider_group": "OME",
        "update_cycle_hours": 6,
    },
    "OME_GFS": {
        "name": "Open-Meteo GFS",
        "enabled": True,
        "module": "kalshicast.collection.collectors.collect_ome_model",
        "func": "fetch_ome_model_forecast",
        "params": {"models": "gfs_seamless"},
        "provider_group": "OME",
        "update_cycle_hours": 6,
    },
    "OME_EC": {
        "name": "Open-Meteo ECMWF",
        "enabled": True,
        "module": "kalshicast.collection.collectors.collect_ome_model",
        "func": "fetch_ome_model_forecast",
        "params": {"models": "ecmwf_ifs025"},
        "provider_group": "OME",
        "update_cycle_hours": 12,
    },
    "OME_ICON": {
        "name": "Open-Meteo ICON",
        "enabled": True,
        "module": "kalshicast.collection.collectors.collect_ome_model",
        "func": "fetch_ome_model_forecast",
        "params": {"models": "icon_seamless"},
        "provider_group": "OME",
        "update_cycle_hours": 6,
    },
    "OME_GEM": {
        "name": "Open-Meteo GEM",
        "enabled": True,
        "module": "kalshicast.collection.collectors.collect_ome_model",
        "func": "fetch_ome_model_forecast",
        "params": {"models": "gem_seamless"},
        "provider_group": "OME",
        "update_cycle_hours": 12,
    },
    "WAPI": {
        "name": "WeatherAPI",
        "enabled": True,
        "module": "kalshicast.collection.collectors.collect_wapi",
        "func": "fetch_wapi_forecast",
        "provider_group": "WAPI",
        "update_cycle_hours": 6,
    },
    "VCR": {
        "name": "Visual Crossing",
        "enabled": True,
        "module": "kalshicast.collection.collectors.collect_vcr",
        "func": "fetch_vcr_forecast",
        "params": {"unitGroup": "us"},
        "provider_group": "VCR",
        "update_cycle_hours": 12,
    },
    "TOM": {
        "name": "Tomorrow.io",
        "enabled": True,
        "module": "kalshicast.collection.collectors.collect_tom",
        "func": "fetch_tom_forecast",
        "params": {"units": "imperial"},
        "provider_group": "TOM",
        "update_cycle_hours": 6,
    },
}


def get_enabled_sources() -> dict[str, dict[str, Any]]:
    return {k: v for k, v in SOURCES.items() if v.get("enabled")}
