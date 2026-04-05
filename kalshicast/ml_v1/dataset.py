# kalshicast/ml_v1/dataset.py
from __future__ import annotations

import io
import os
import time
import pandas as pd
import requests
import logging
from datetime import datetime, timedelta

from kalshicast.ml_v1.config import DATA_DIR
from kalshicast.ml_v1.stations import get_stations, get_station

log = logging.getLogger(__name__)
os.makedirs(DATA_DIR, exist_ok=True)


def _fetch_iem_actuals(cli_site: str, state: str,
                       start_date, end_date) -> pd.DataFrame:
    """Fetch historical daily high/low from IEM ASOS.

    Kalshi settles on NWS ASOS thermometers — IEM archives these
    observations and is the authoritative historical source.

    Uses 3-letter FAA codes (strip K from ICAO) per IEM convention.
    Sequential calls only; exponential backoff on 429/502/503.
    """
    network = f"{state}_ASOS"
    url = (
        f"https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py?"
        f"network={network}&stations={cli_site}"
        f"&year1={start_date.year}&month1={start_date.month}&day1={start_date.day}"
        f"&year2={end_date.year}&month2={end_date.month}&day2={end_date.day}"
        f"&var=max_tmpf&var=min_tmpf&format=csv&na=blank"
    )
    headers = {"User-Agent": "Kalshicast/10.0 (weather research)"}

    for attempt in range(5):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code in (429, 502, 503):
                wait = 3 ** attempt
                log.warning("  ⏳ IEM %d, retrying in %ds...",
                            resp.status_code, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        except requests.exceptions.Timeout:
            wait = 3 ** attempt
            log.warning("  ⏳ IEM timeout, retrying in %ds...", wait)
            time.sleep(wait)
    else:
        log.error("❌ IEM API failed after 5 attempts for %s", cli_site)
        return pd.DataFrame()

    try:
        df = pd.read_csv(io.StringIO(resp.text))
    except Exception:
        log.warning("⚠️ IEM returned unparseable response for %s", cli_site)
        return pd.DataFrame()

    if df.empty or "max_tmpf" not in df.columns:
        log.warning("⚠️ IEM returned no usable data for %s", cli_site)
        return pd.DataFrame()

    df = df.rename(columns={
        "day": "time",
        "max_tmpf": "actual_high",
        "min_tmpf": "actual_low",
    })
    df["time"] = pd.to_datetime(df["time"])
    df = df[["time", "actual_high", "actual_low"]].dropna()
    return df


def fetch_bootstrap_data(station_id: str, lat: float, lon: float,
                         years_back: int = 3,
                         force_refresh: bool = False) -> pd.DataFrame:
    csv_path = os.path.join(DATA_DIR, f"{station_id}_bootstrap.csv")
    if os.path.exists(csv_path) and not force_refresh:
        return pd.read_csv(csv_path, parse_dates=['time'])

    station = get_station(station_id)
    cli_site = station["cli_site"]
    state = station["state"]
    tz = station["timezone"]

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=365 * years_back)
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    # --- GFS forecasts from Open-Meteo (station timezone for daily alignment) ---
    log.info(f"📥 Downloading GFS forecasts for {station_id}...")
    f_url = (
        f"https://historical-forecast-api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&start_date={start_str}&end_date={end_str}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,"
        f"wind_speed_10m_max,shortwave_radiation_sum,et0_fao_evapotranspiration"
        f"&models=gfs_seamless"
        f"&temperature_unit=fahrenheit&precipitation_unit=inch&wind_speed_unit=mph"
        f"&timezone={tz}"
    )

    try:
        f_resp = requests.get(f_url)
        f_resp.raise_for_status()
        f_df = pd.DataFrame(f_resp.json()["daily"]).rename(columns={
            "temperature_2m_max": "forecast_high",
            "temperature_2m_min": "forecast_low",
            "precipitation_sum": "forecast_precip",
            "wind_speed_10m_max": "forecast_wind",
            "shortwave_radiation_sum": "forecast_radiation",
            "et0_fao_evapotranspiration": "forecast_evap",
        })
    except Exception as e:
        log.error(f"❌ Failed to download forecasts for {station_id}: {e}")
        return pd.DataFrame()

    # --- Actuals from IEM ASOS (Kalshi settlement source) ---
    log.info(f"📥 Downloading IEM ASOS actuals for {station_id} ({cli_site})...")
    a_df = _fetch_iem_actuals(cli_site, state, start_date, end_date)
    if a_df.empty:
        return pd.DataFrame()

    # --- Merge & feature engineering ---
    f_df['time'] = pd.to_datetime(f_df['time'])
    df = pd.merge(f_df, a_df, on="time").dropna()
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
    log.info(f"✅ Saved {len(df)} days of IEM-sourced data for {station_id}")
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stations = get_stations(active_only=True)

    log.info(f"🚀 Bootstrapping data for {len(stations)} stations...")
    log.info("   (Sequential fetch — IEM academic API rate limits)")

    for s in stations:
        fetch_bootstrap_data(s["station_id"], s["lat"], s["lon"])

    log.info("✅ All station data downloaded and engineered.")
