# kalshicast/ml_v1/dataset.py
from __future__ import annotations

import io
import os
import time
import numpy as np
import pandas as pd
import requests
import logging
from datetime import datetime, timedelta, timezone

from kalshicast.ml_v1.config import DATA_DIR
from kalshicast.config.stations import get_stations, get_station

log = logging.getLogger(__name__)
os.makedirs(DATA_DIR, exist_ok=True)


def _fetch_iem_actuals(cli_site: str, 
                       start_date, end_date,
                       tz: str = "Etc/UTC") -> pd.DataFrame:
    """Fetch historical daily high/low from IEM ASOS hourly observations.

    Kalshi settles on NWS ASOS thermometers — IEM archives these
    observations and is the authoritative historical source.

    Uses 3-letter FAA codes (strip K from ICAO) per IEM convention.
    Fetches hourly METAR observations via the asos.py endpoint (the
    daily.py summary endpoint no longer returns temperature columns)
    and aggregates to daily max/min using the station's local timezone
    so calendar-day boundaries match Kalshi settlement.

    Sequential calls only; exponential backoff on 429/502/503.
    """
    url = (
        "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
        f"station={cli_site}&data=tmpf"
        f"&year1={start_date.year}&month1={start_date.month}&day1={start_date.day}"
        f"&year2={end_date.year}&month2={end_date.month}&day2={end_date.day}"
        f"&tz={tz}&format=onlycomma&latlon=no&elev=no"
        f"&missing=M&trace=T&direct=no&report_type=3"
    )
    headers = {"User-Agent": "Kalshicast/10.0 (weather research)"}

    for attempt in range(5):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code in (429, 502, 503):
                wait = 3 ** attempt
                log.warning("  ⏳ IEM %d, retrying in %ds...",
                            resp.status_code, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        except requests.exceptions.HTTPError:
            raise
        except requests.exceptions.RequestException as exc:
            wait = 3 ** attempt
            log.warning("  ⏳ IEM request error (%s), retrying in %ds...",
                        exc, wait)
            time.sleep(wait)
    else:
        log.error("❌ IEM API failed after 5 attempts for %s", cli_site)
        return pd.DataFrame()

    try:
        df = pd.read_csv(io.StringIO(resp.text))
    except Exception:
        log.warning("⚠️ IEM returned unparseable response for %s", cli_site)
        return pd.DataFrame()

    if df.empty or "tmpf" not in df.columns:
        log.warning("⚠️ IEM returned no usable data for %s", cli_site)
        return pd.DataFrame()

    # Convert to numeric (M = missing → NaN) and aggregate to daily max/min
    df["tmpf"] = pd.to_numeric(df["tmpf"], errors="coerce")
    df["time"] = pd.to_datetime(df["valid"]).dt.date

    # CRITICAL: Require at least 20 valid observations per day.
    # Without this, a day with only 3 early-morning readings during a sensor
    # reboot produces a "daily max" far below the actual afternoon high,
    # poisoning target_error with phantom 10-15°F correction signals.
    obs_per_day = df.groupby("time")["tmpf"].apply(lambda x: x.notna().sum())
    valid_dates = obs_per_day[obs_per_day >= 20].index
    df = df[df["time"].isin(valid_dates)]

    daily = df.groupby("time")["tmpf"].agg(
        actual_high="max", actual_low="min"
    ).reset_index()
    daily["time"] = pd.to_datetime(daily["time"])
    daily = daily.dropna(subset=["actual_high", "actual_low"])
    log.info("  ✅ Got %d days of IEM ASOS actuals for %s", len(daily), cli_site)
    return daily


def _fetch_with_retry(url: str, max_retries: int = 5, timeout=(5, 120)) -> requests.Response:
    """Fetch URL with exponential backoff on 429/502/503."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code in (429, 502, 503):
                wait = min(3 ** attempt, 60)
                log.warning(f"  ⏳ API {resp.status_code}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as exc:
            wait = min(3 ** attempt, 60)
            log.warning(f"  ⏳ Request error ({exc}), retrying in {wait}s...")
            time.sleep(wait)
    raise requests.exceptions.ConnectionError(f"Failed after {max_retries} attempts: {url[:100]}")

def fetch_bootstrap_data(station_id: str, lat: float, lon: float,
                         years_back: int = 3,
                         force_refresh: bool = False) -> pd.DataFrame:
    csv_path = os.path.join(DATA_DIR, f"{station_id}_bootstrap.csv")
    if os.path.exists(csv_path) and not force_refresh:
        return pd.read_csv(csv_path, parse_dates=['time'])

    station = get_station(station_id)
    cli_site = station["cli_site"]
    tz = station["timezone"]

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=365 * years_back)
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    # --- GFS forecasts from Open-Meteo ---
    # CRITICAL: We use the Previous Runs API (previous_day1) to get forecasts
    # with a ~24-hour lead time, matching production conditions where we bet on
    # next-day temperatures. The Historical Forecast API concatenates 0-hour
    # analysis data, which has artificially small errors vs. production.
    #
    # Previous Runs API: data available from Feb 2024+ (GFS temp from Apr 2021).
    # For dates before that, we fall back to the Historical Forecast API and
    # flag the rows so we can track this in diagnostics.
    
    PREV_RUNS_CUTOFF = datetime(2024, 2, 1).date()
    
    # Variables we need for daily aggregation
    # Historical Forecast API supports daily variables directly.
    # Previous Runs API only supports hourly variables with _previous_day1
    # suffix, so we fetch hourly and aggregate to daily ourselves.
    daily_vars_base = [
        "temperature_2m_max", "temperature_2m_min", "precipitation_sum",
        "wind_speed_10m_max", "shortwave_radiation_sum", "et0_fao_evapotranspiration"
    ]
    hourly_vars_prev = [
        "temperature_2m_previous_day1", "precipitation_previous_day1",
        "wind_speed_10m_previous_day1", "shortwave_radiation_previous_day1",
        "et0_fao_evapotranspiration_previous_day1"
    ]
    
    rename_map = {
        "temperature_2m_max": "forecast_high",
        "temperature_2m_min": "forecast_low",
        "precipitation_sum": "forecast_precip",
        "wind_speed_10m_max": "forecast_wind",
        "shortwave_radiation_sum": "forecast_radiation",
        "et0_fao_evapotranspiration": "forecast_evap",
    }
    
    frames = []
    
    # Phase 1: Previous Runs API for dates >= Jan 2024 (realistic lead time)
    # Uses HOURLY variables and aggregates to daily, because the Previous
    # Runs API does not support daily aggregate variables.
    prev_start = max(start_date, PREV_RUNS_CUTOFF)
    if prev_start < end_date:
        prev_start_str = prev_start.strftime('%Y-%m-%d')
        log.info(f"📥 Downloading GFS previous_day1 forecasts for {station_id} ({prev_start_str} → {end_str})...")
        prev_url = (
            f"https://previous-runs-api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}&start_date={prev_start_str}&end_date={end_str}"
            f"&hourly={','.join(hourly_vars_prev)}"
            f"&models=gfs_seamless"
            f"&temperature_unit=fahrenheit&precipitation_unit=inch&wind_speed_unit=mph"
            f"&timezone={tz}"
        )
        try:
            prev_resp = _fetch_with_retry(prev_url)
            data = prev_resp.json()
            if "hourly" not in data:
                raise ValueError(f"Open-Meteo missing 'hourly' key. Response: {str(data)[:200]}")
            
            hdf = pd.DataFrame(data["hourly"])
            hdf["time"] = pd.to_datetime(hdf["time"])
            
            # Drop NaNs at edge boundaries before they corrupt daily aggregations
            hdf["temperature_2m_previous_day1"] = pd.to_numeric(hdf["temperature_2m_previous_day1"], errors="coerce")
            hdf = hdf.dropna(subset=["temperature_2m_previous_day1"])
            
            hdf["date"] = hdf["time"].dt.date
            
            # CRITICAL: Only aggregate days that have all 24 hours. Partial days skew max/min.
            valid_days = hdf.groupby("date").filter(lambda x: len(x) >= 24)

            # Aggregate hourly → daily: max/min for temp, sum for precip/radiation/evap, max for wind
            if not valid_days.empty:
                prev_df = pd.DataFrame({
                    "time": valid_days.groupby("date")["time"].first().values,
                    "forecast_high": valid_days.groupby("date")["temperature_2m_previous_day1"].max().values,
                    "forecast_low": valid_days.groupby("date")["temperature_2m_previous_day1"].min().values,
                    "forecast_precip": valid_days.groupby("date")["precipitation_previous_day1"].sum(min_count=1).values,
                    "forecast_wind": valid_days.groupby("date")["wind_speed_10m_previous_day1"].max().values,
                    "forecast_radiation": valid_days.groupby("date")["shortwave_radiation_previous_day1"].sum(min_count=1).values,
                    "forecast_evap": valid_days.groupby("date")["et0_fao_evapotranspiration_previous_day1"].sum(min_count=1).values,
                })
                prev_df["time"] = pd.to_datetime(prev_df["time"]).dt.normalize()
                prev_df['forecast_lead_hours'] = 24  # True day-ahead forecast
                frames.append(prev_df)
                log.info(f"  ✅ Got {len(prev_df)} days from Previous Runs API (hourly→daily)")
            else:
                raise ValueError("No valid full 24-hour days found in Previous Runs API response.")
        except requests.exceptions.ConnectionError as e:
            # All retries exhausted — this is a hard infrastructure failure.
            # Do NOT silently fall back to 0h analysis data, which would
            # destroy the 24h-lead training premise without anyone noticing.
            log.error(f"❌ Previous Runs API exhausted all retries for {station_id}: {e}")
            log.error("   Skipping station entirely (will not fall back to 0h data).")
            return pd.DataFrame()
        except Exception as e:
            log.warning(f"⚠️ Previous Runs API failed for {station_id}: {e}")
            log.warning("   Falling back to Historical Forecast API for this range.")
            # Fall through to historical API below
            prev_start = end_date  # Force full fallback
    
    # Phase 2: Historical Forecast API for dates before Jan 2024 (or as fallback)
    hist_end = prev_start - timedelta(days=1) if frames else end_date
    if start_date <= hist_end:
        hist_end_str = hist_end.strftime('%Y-%m-%d')
        fallback_start_str = start_str
        log.info(f"📥 Downloading GFS historical forecasts for {station_id} ({fallback_start_str} → {hist_end_str})...")
        hist_url = (
            f"https://historical-forecast-api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}&start_date={fallback_start_str}&end_date={hist_end_str}"
            f"&daily={','.join(daily_vars_base)}"
            f"&models=gfs_seamless"
            f"&temperature_unit=fahrenheit&precipitation_unit=inch&wind_speed_unit=mph"
            f"&timezone={tz}"
        )
        try:
            hist_resp = _fetch_with_retry(hist_url)
            data = hist_resp.json()
            if "daily" not in data:
                raise ValueError(f"Open-Meteo missing 'daily' key. Response: {str(data)[:200]}")
            
            hist_df = pd.DataFrame(data["daily"]).rename(columns=rename_map)
            hist_df['forecast_lead_hours'] = 0  # ~0h analysis data (less realistic)
            frames.append(hist_df)
            log.info(f"  ✅ Got {len(hist_df)} days from Historical Forecast API (0h lead)")
        except Exception as e:
            log.error(f"❌ Failed to download historical forecasts for {station_id}: {e}")
    
    if not frames:
        log.error(f"❌ No forecast data obtained for {station_id}")
        return pd.DataFrame()
    
    f_df = pd.concat(frames, ignore_index=True)

    # --- Actuals from IEM ASOS (Kalshi settlement source) ---
    log.info(f"📥 Downloading IEM ASOS actuals for {station_id} ({cli_site})...")
    a_df = _fetch_iem_actuals(cli_site, start_date, end_date, tz=tz)
    if a_df.empty:
        return pd.DataFrame()

    # --- Merge & feature engineering ---
    f_df['time'] = pd.to_datetime(f_df['time'])
    df = pd.merge(f_df, a_df, on="time").dropna(subset=[
        'forecast_high', 'forecast_low', 'actual_high', 'actual_low'
    ])
    df = df.sort_values('time')
    
    # Log lead time composition for transparency
    if 'forecast_lead_hours' in df.columns:
        lead_counts = df['forecast_lead_hours'].value_counts().to_dict()
        log.info(f"  📊 Lead time composition: {lead_counts}")

    # --- FILTER TO REALISTIC LEAD TIME ONLY ---
    # Production always uses 24h-lead forecasts. Training on 0h analysis data
    # teaches the model to correct smaller errors than it will encounter live.
    if 'forecast_lead_hours' in df.columns:
        n_before = len(df)
        realistic = df[df['forecast_lead_hours'] == 24]
        if len(realistic) >= 180:  # Need at least ~6 months of realistic data
            df = realistic.copy()
            log.info(f"  🎯 Filtered to 24h-lead data only: {n_before} → {len(df)} rows")
        else:
            log.warning(f"  ⚠️ Only {len(realistic)} rows with 24h lead — keeping all {n_before} rows (mixed lead times)")

    # --- RESAMPLE TO COMPLETE DAILY CALENDAR ---
    # IEM ASOS frequently drops days. Without this, .shift(1) bridges over
    # multi-day gaps (e.g., if days 10-14 are missing, shift(1) on day 15
    # would incorrectly use day 9's error as "yesterday's error").
    # Resampling to a calendar index makes gaps become NaN naturally.
    pre_resample = len(df)
    df = df.set_index('time').asfreq('D').reset_index()
    gaps = pre_resample - df['actual_high'].notna().sum()
    if gaps > 0:
        # This shouldn't happen (we only lose data, not gain it), but log just in case
        log.info(f"  📅 Resampled to daily calendar: {len(df)} days ({int(gaps)} gap days inserted as NaN)")
    else:
        n_inserted = len(df) - pre_resample
        if n_inserted > 0:
            log.info(f"  📅 Resampled to daily calendar: {n_inserted} gap days inserted as NaN")

    # Cyclical encoding of day-of-year (sin/cos) so Dec 31 and Jan 1 are
    # adjacent in feature space — tree models can't extrapolate across the
    # linear 365→1 cliff that raw day_of_year creates.
    df['day_sin'] = np.sin(2 * np.pi * df['time'].dt.dayofyear / 365.25)
    df['day_cos'] = np.cos(2 * np.pi * df['time'].dt.dayofyear / 365.25)

    # Cross-target feature: diurnal range predicts GFS bias direction.
    # GFS underestimates on clear/calm days (wide range) and overestimates
    # on overcast days (compressed range).
    df['forecast_diurnal_range'] = df['forecast_high'] - df['forecast_low']

    # --- REGIME MEMORY ENGINEERING ---
    # Now shift/rolling operations respect the true calendar:
    # - shift(1) is always exactly yesterday (NaN if that day was missing)
    # - rolling(7) only includes actual consecutive calendar days
    for t in ['high', 'low']:
        err = df[f'actual_{t}'] - df[f'forecast_{t}']
        df[f'target_error_{t}'] = err

        # "Conservative Resilience" Feature Engineering:
        # 1. Limited forward-fill (max 2 days) to bridge small station outages.
        #    This prevents a 1-day sensor gap from poisoning the next 2 weeks of training.
        s1 = err.shift(1).ffill(limit=2)
        s2 = err.shift(2).ffill(limit=2)
        
        df[f'lag_{t}_1d'] = s1
        df[f'lag_{t}_2d'] = s2
        
        # 2. Use min_periods to allow windows with partial data (requires at least 50% coverage).
        #    This recovers days that would otherwise be NaN due to a single missing point in the window.
        df[f'roll_{t}_7d_mean'] = s1.rolling(7, min_periods=4).mean()
        df[f'roll_{t}_14d_mean'] = s1.rolling(14, min_periods=7).mean()
        df[f'roll_{t}_7d_std'] = s1.rolling(7, min_periods=4).std()

    # --- Diagnose NaN dropout bias ---
    # IEM stations often go offline during severe weather (storms, ice, extreme heat).
    # If NaN lag features disproportionately remove extreme-temperature days,
    # the model never learns to correct GFS's largest errors — exactly when
    # the Kalshi edge is most valuable.
    pre_drop = len(df)
    valid_mask = df['actual_high'].notna()
    if valid_mask.sum() > 50:
        actuals_valid = df.loc[valid_mask, 'actual_high']
        q05 = actuals_valid.quantile(0.05)
        q95 = actuals_valid.quantile(0.95)
        extreme_mask = valid_mask & ((df['actual_high'] <= q05) | (df['actual_high'] >= q95))
        lag_nan_mask = df['lag_high_1d'].isna() | df['roll_high_7d_mean'].isna()
        extreme_nan_rate = (extreme_mask & lag_nan_mask).sum() / max(extreme_mask.sum(), 1)
        normal_nan_rate = (~extreme_mask & valid_mask & lag_nan_mask).sum() / max((~extreme_mask & valid_mask).sum(), 1)
        if extreme_nan_rate > normal_nan_rate * 1.5:
            log.warning(f"  ⚠️ Extreme-day NaN dropout bias: {extreme_nan_rate:.1%} of extreme days "
                        f"have NaN lags vs {normal_nan_rate:.1%} of normal days — "
                        f"model may underperform on high-value trading days")

    # Drop rows with NaN (gap days, warmup period for rolling features)
    df = df.dropna(subset=[
        'target_error_high', 'target_error_low',
        'lag_high_1d', 'lag_high_2d', 'lag_low_1d', 'lag_low_2d',
        'roll_high_7d_mean', 'roll_low_7d_mean',
        'roll_high_14d_mean', 'roll_low_14d_mean',
        'roll_high_7d_std', 'roll_low_7d_std',
    ])
    log.info(f"  📉 Dropped {pre_drop - len(df)} rows (gap days + warmup)")

    # Remove internal bookkeeping column before persisting.
    # If this leaks into training features, the model learns a trivial rule:
    # "if forecast_lead_hours == 0, errors are small" — instant data leakage.
    df = df.drop(columns=['forecast_lead_hours'], errors='ignore')

    df.to_csv(csv_path, index=False)
    log.info(f"✅ Saved {len(df)} days of IEM-sourced data for {station_id}")
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stations = get_stations(active_only=True)

    log.info(f"🚀 Bootstrapping data for {len(stations)} stations...")
    log.info("   (Sequential fetch — IEM academic API rate limits)")

    for i, s in enumerate(stations):
        fetch_bootstrap_data(s["station_id"], s["lat"], s["lon"], force_refresh=True)
        if i < len(stations) - 1:
            time.sleep(2)  # Respect Open-Meteo rate limits

    log.info("✅ All station data downloaded and engineered.")
