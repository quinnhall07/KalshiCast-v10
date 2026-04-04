# kalshicast/ml_v1/dataset.py
from __future__ import annotations

import io
import os
import pandas as pd
import requests
import logging
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

from kalshicast.ml_v1.config import DATA_DIR
from kalshicast.ml_v1.stations import get_stations, get_station

log = logging.getLogger(__name__)
os.makedirs(DATA_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────
# Ground truth: IEM ASOS (preferred) vs Open-Meteo ERA5 (fallback)
# ─────────────────────────────────────────────────────────────────────

def _fetch_iem_actuals(station_id: str, start_date, end_date) -> pd.DataFrame | None:
    """Fetch daily high/low from Iowa Environmental Mesonet ASOS archive.

    Uses the same ASOS instruments that produce NWS CLI reports,
    eliminating the ERA5 vs NWS CLI train/serve mismatch.
    """
    try:
        station = get_station(station_id)
        state = station["state"]
    except KeyError:
        return None

    iem_id = station_id[1:]  # KNYC -> NYC, KMDW -> MDW
    network = f"{state}_ASOS"

    url = (
        f"https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py?"
        f"network={network}&stations={iem_id}"
        f"&year1={start_date.year}&month1={start_date.month}&day1={start_date.day}"
        f"&year2={end_date.year}&month2={end_date.month}&day2={end_date.day}"
        f"&var=max_temp_f&var=min_temp_f&format=csv"
    )

    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        if df.empty or "max_temp_f" not in df.columns:
            return None
        df = df.rename(columns={
            "day": "time",
            "max_temp_f": "actual_high",
            "min_temp_f": "actual_low",
        })
        df["actual_high"] = pd.to_numeric(df["actual_high"], errors="coerce")
        df["actual_low"] = pd.to_numeric(df["actual_low"], errors="coerce")
        df = df[["time", "actual_high", "actual_low"]].dropna()
        if len(df) < 30:
            log.warning(f"  IEM returned only {len(df)} rows for {station_id}, falling back")
            return None
        log.info(f"  IEM ASOS: {len(df)} days of station observations for {station_id}")
        return df
    except Exception as e:
        log.warning(f"  IEM fetch failed for {station_id}: {e}")
        return None


def _fetch_openmeteo_actuals(lat: float, lon: float, start_str: str, end_str: str) -> pd.DataFrame | None:
    """Fallback: fetch from Open-Meteo archive (ERA5 reanalysis)."""
    a_url = (
        f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&"
        f"start_date={start_str}&end_date={end_str}&daily=temperature_2m_max,temperature_2m_min&"
        f"temperature_unit=fahrenheit&timezone=UTC"
    )
    try:
        resp = requests.get(a_url, timeout=120)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json()["daily"]).rename(columns={
            "temperature_2m_max": "actual_high",
            "temperature_2m_min": "actual_low",
        })
        return df
    except Exception as e:
        log.warning(f"  Open-Meteo archive fallback failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────
# Bootstrap data pipeline
# ─────────────────────────────────────────────────────────────────────

def fetch_bootstrap_data(station_id: str, lat: float, lon: float, years_back: int = 3, force_refresh: bool = False) -> pd.DataFrame:
    csv_path = os.path.join(DATA_DIR, f"{station_id}_bootstrap.csv")
    if os.path.exists(csv_path) and not force_refresh:
        return pd.read_csv(csv_path, parse_dates=['time'])

    log.info(f"Downloading bootstrap data for {station_id}...")

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=365 * years_back)
    start_str, end_str = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

    # --- Forecast data: archived GFS model predictions ---
    f_url = (f"https://historical-forecast-api.open-meteo.com/v1/forecast?"
             f"latitude={lat}&longitude={lon}&start_date={start_str}&end_date={end_str}"
             f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,"
             f"shortwave_radiation_sum,et0_fao_evapotranspiration&models=gfs_seamless&"
             f"temperature_unit=fahrenheit&precipitation_unit=inch&wind_speed_unit=mph&timezone=UTC")

    try:
        f_resp = requests.get(f_url, timeout=120)
        f_resp.raise_for_status()
        f_df = pd.DataFrame(f_resp.json()["daily"]).rename(columns={
            "temperature_2m_max": "forecast_high", "temperature_2m_min": "forecast_low",
            "precipitation_sum": "forecast_precip", "wind_speed_10m_max": "forecast_wind",
            "shortwave_radiation_sum": "forecast_radiation", "et0_fao_evapotranspiration": "forecast_evap"
        })
    except Exception as e:
        log.error(f"Failed to download forecast data for {station_id}: {e}")
        return pd.DataFrame()

    # --- Actuals: IEM ASOS station obs (preferred) -> ERA5 (fallback) ---
    a_df = _fetch_iem_actuals(station_id, start_date, end_date)
    if a_df is None:
        log.info(f"  IEM unavailable for {station_id}, falling back to ERA5 archive")
        a_df = _fetch_openmeteo_actuals(lat, lon, start_str, end_str)
    if a_df is None:
        log.error(f"Failed to download actuals for {station_id}")
        return pd.DataFrame()

    df = pd.merge(f_df, a_df, on="time").dropna()
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values('time')
    df['day_of_year'] = df['time'].dt.dayofyear
    
    # --- REGIME MEMORY ENGINEERING ---
    for t in ['high', 'low']:
        err = df[f'actual_{t}'] - df[f'forecast_{t}']
        df[f'target_error_{t}'] = err
        
        df[f'lag_{t}_1d'] = err.shift(1)
        df[f'lag_{t}_2d'] = err.shift(2)
        df[f'roll_{t}_7d_mean'] = err.shift(1).rolling(7).mean()
        df[f'roll_{t}_14d_mean'] = err.shift(1).rolling(14).mean()
        df[f'roll_{t}_7d_std'] = err.shift(1).rolling(7).std()

    df = df.dropna()
    df.to_csv(csv_path, index=False)
    log.info(f"✅ Saved {len(df)} days of regime data for {station_id}")
    return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stations = get_stations(active_only=True)
    
    log.info(f"🚀 Bootstrapping data for {len(stations)} stations...")
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        for s in stations:
            executor.submit(fetch_bootstrap_data, s["station_id"], s["lat"], s["lon"])
            
    log.info("✅ All station data downloaded and engineered.")