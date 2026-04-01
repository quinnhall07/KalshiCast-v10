# kalshicast/ml_v1/dataset.py
from __future__ import annotations

import os
import pandas as pd
import requests
import logging
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

from kalshicast.ml_v1.config import DATA_DIR
from kalshicast.ml_v1.stations import get_stations

log = logging.getLogger(__name__)
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_bootstrap_data(station_id: str, lat: float, lon: float, years_back: int = 3, force_refresh: bool = False) -> pd.DataFrame:
    csv_path = os.path.join(DATA_DIR, f"{station_id}_bootstrap.csv")
    if os.path.exists(csv_path) and not force_refresh:
        return pd.read_csv(csv_path, parse_dates=['time'])

    log.info(f"📥 Downloading Institutional Data for {station_id}...")
    
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=365 * years_back)
    start_str, end_str = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

    f_url = (f"https://historical-forecast-api.open-meteo.com/v1/forecast?"
             f"latitude={lat}&longitude={lon}&start_date={start_str}&end_date={end_str}"
             f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,"
             f"shortwave_radiation_sum,et0_fao_evapotranspiration&models=gfs_seamless&"
             f"temperature_unit=fahrenheit&precipitation_unit=inch&wind_speed_unit=mph&timezone=UTC")
    
    a_url = (f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&"
             f"start_date={start_str}&end_date={end_str}&daily=temperature_2m_max,temperature_2m_min&"
             f"temperature_unit=fahrenheit&timezone=UTC")

    try:
        f_resp = requests.get(f_url)
        f_resp.raise_for_status()
        f_df = pd.DataFrame(f_resp.json()["daily"]).rename(columns={
            "temperature_2m_max": "forecast_high", "temperature_2m_min": "forecast_low",
            "precipitation_sum": "forecast_precip", "wind_speed_10m_max": "forecast_wind",
            "shortwave_radiation_sum": "forecast_radiation", "et0_fao_evapotranspiration": "forecast_evap"
        })
        
        a_resp = requests.get(a_url)
        a_resp.raise_for_status()
        a_df = pd.DataFrame(a_resp.json()["daily"]).rename(columns={
            "temperature_2m_max": "actual_high", "temperature_2m_min": "actual_low"
        })
    except Exception as e:
        log.error(f"❌ Failed to download data for {station_id}: {e}")
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