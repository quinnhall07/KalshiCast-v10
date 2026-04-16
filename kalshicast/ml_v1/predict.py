import os
import json
import logging
import argparse
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np
import requests
import xgboost as xgb
import lightgbm as lgb
import warnings

from kalshicast.ml_v1.config import FEATURES, BASE_MODELS_DIR
from kalshicast.config.stations import get_stations
from kalshicast.ml_v1.dataset import _fetch_iem_actuals, _fetch_with_retry

warnings.filterwarnings('ignore', category=UserWarning)
logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger("Predict")

def compile_stateless_features(station_id: str, lat: float, lon: float, tz: str) -> pd.DataFrame:
    """
    Phase A + Phase B: The Dual Fetch
    Reconstructs the precise 14-day trailing lag errors natively in RAM
    by fetching the last 21 days, appending tomorrow's live forecast,
    and rolling the variables forward.
    """
    now = datetime.now(timezone.utc)
    # Align to local timezone to calculate days properly
    # Using simple date math. This gets the local date for the target station
    local_now = pd.Timestamp(now).tz_convert(tz).replace(tzinfo=None)
    
    today_date = local_now.date()
    yesterday_date = today_date - timedelta(days=1)
    retro_start_date = today_date - timedelta(days=21)
    
    target_tomorrow_date = today_date + timedelta(days=1)
    
    # --- PHASE A: Retrospective (Last 21 Days) ---
    daily_vars_base = [
        "temperature_2m_max", "temperature_2m_min", "precipitation_sum",
        "wind_speed_10m_max", "shortwave_radiation_sum", "et0_fao_evapotranspiration"
    ]
    rename_map = {
        "temperature_2m_max": "forecast_high",
        "temperature_2m_min": "forecast_low",
        "precipitation_sum": "forecast_precip",
        "wind_speed_10m_max": "forecast_wind",
        "shortwave_radiation_sum": "forecast_radiation",
        "et0_fao_evapotranspiration": "forecast_evap",
    }
    
    # 1. Fetch historical GFS
    hist_url = (
        f"https://historical-forecast-api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&start_date={retro_start_date}&end_date={yesterday_date}"
        f"&daily={','.join(daily_vars_base)}"
        f"&models=gfs_seamless"
        f"&temperature_unit=fahrenheit&precipitation_unit=inch&wind_speed_unit=mph"
        f"&timezone={tz}"
    )
    
    hist_resp = _fetch_with_retry(hist_url, max_retries=3)
    hist_df = pd.DataFrame(hist_resp.json()["daily"]).rename(columns=rename_map)
    hist_df['time'] = pd.to_datetime(hist_df['time'])
    
    # 2. Fetch IEM Actuals
    a_df = _fetch_iem_actuals(get_stations(active_only=True)[0]['cli_site'] if False else next(s for s in get_stations(active_only=True) if s['station_id'] == station_id)['cli_site'], retro_start_date, yesterday_date, tz=tz)
    
    # Merge Retrospective
    if not a_df.empty:
        a_df['time'] = pd.to_datetime(a_df['time'])
        df_retro = pd.merge(hist_df, a_df, on='time', how='left')
    else:
        df_retro = hist_df
        df_retro['actual_high'] = np.nan
        df_retro['actual_low'] = np.nan
        
    # --- PHASE B: Live Forecast (For Tomorrow) ---
    # We fetch today + tomorrow + day after (just to be safe), but extract exactly tomorrow
    live_url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&start_date={today_date}&end_date={target_tomorrow_date + timedelta(days=1)}"
        f"&daily={','.join(daily_vars_base)}"
        f"&models=gfs_seamless"
        f"&temperature_unit=fahrenheit&precipitation_unit=inch&wind_speed_unit=mph"
        f"&timezone={tz}"
    )
    live_resp = _fetch_with_retry(live_url, max_retries=3)
    live_df = pd.DataFrame(live_resp.json()["daily"]).rename(columns=rename_map)
    live_df['time'] = pd.to_datetime(live_df['time'])
    live_df['actual_high'] = np.nan
    live_df['actual_low'] = np.nan
    
    # Combine Retro + Live
    df = pd.concat([df_retro, live_df], ignore_index=True)
    df = df.drop_duplicates(subset=['time'], keep='last').sort_values('time')
    
    # --- Structural Engineering (Rule #4 Conservative Resilience + Calendrical) ---
    df['day_sin'] = np.sin(2 * np.pi * df['time'].dt.dayofyear / 365.25)
    df['day_cos'] = np.cos(2 * np.pi * df['time'].dt.dayofyear / 365.25)
    df['forecast_diurnal_range'] = df['forecast_high'] - df['forecast_low']
    
    for t in ['high', 'low']:
        err = df[f'actual_{t}'] - df[f'forecast_{t}']
        
        # We ffill limit=2 to bridge data gaps caused by "Today" missing or IEM dropouts
        s1 = err.shift(1).ffill(limit=2)
        s2 = err.shift(2).ffill(limit=2)
        
        df[f'lag_{t}_1d'] = s1
        df[f'lag_{t}_2d'] = s2
        
        df[f'roll_{t}_7d_mean'] = s1.rolling(7, min_periods=4).mean()
        df[f'roll_{t}_14d_mean'] = s1.rolling(14, min_periods=7).mean()
        df[f'roll_{t}_7d_std'] = s1.rolling(7, min_periods=4).std()
        
    # Extract only the row for TOMORROW
    tomorrow_row = df[df['time'].dt.date == target_tomorrow_date].copy()
    if tomorrow_row.empty:
        raise ValueError(f"Failed to isolate tomorrow's forecast boundary ({target_tomorrow_date}).")
        
    return tomorrow_row.iloc[0], target_tomorrow_date


def execute_inference():
    stations = get_stations(active_only=True)
    
    log.info("🚀 Booting Kalshicast Inference Engine...")
    print("-" * 65)
    print(f"{'Station':<6} | {'Target':<10} | {'Raw GFS':<8} | {'Bias Adjust':<12} | {'Final Kalshi Predicted':<25}")
    print("-" * 65)
    
    for s in stations:
        station_id = s["station_id"]
        lat = s["lat"]
        lon = s["lon"]
        tz = s["timezone"]
        
        # Load Phase Dual Fetch Features
        try:
            row, target_date = compile_stateless_features(station_id, lat, lon, tz)
        except Exception as e:
            log.warning(f"   [Skipping {station_id}] API Fetch Failure: {e}")
            continue
            
        for t_type in ("HIGH", "LOW"):
            model_dir = os.path.join(BASE_MODELS_DIR, station_id, t_type)
            xgb_path = os.path.join(model_dir, "model.json")
            lgb_path = os.path.join(model_dir, "model.txt")
            blend_path = os.path.join(model_dir, "blend_weight.json")
            
            if not os.path.exists(xgb_path) or not os.path.exists(lgb_path) or not os.path.exists(blend_path):
                # No model artifact found, skip inference
                continue
                
            # Load Weights
            with open(blend_path, "r") as f:
                blend_cfg = json.load(f)
            w_xgb = blend_cfg.get("weight_xgb", 0.5)
            w_lgb = blend_cfg.get("weight_lgb", 0.5)
            
            # Reconstruct DMatrix input array
            feats = FEATURES[t_type]
            # Convert row to DataFrame for prediction
            X_live = pd.DataFrame([row])[feats]
            
            # Predict XGB
            model_xgb = xgb.Booster()
            model_xgb.load_model(xgb_path)
            dmatrix_live = xgb.DMatrix(X_live)
            pred_xgb = model_xgb.predict(dmatrix_live)[0]
            
            # Predict LGB
            model_lgb = lgb.Booster(model_file=lgb_path)
            pred_lgb = model_lgb.predict(X_live)[0]
            
            # Ensemble Final Bias
            blended_bias = (w_xgb * pred_xgb) + (w_lgb * pred_lgb)
            
            # Determine Final Temp
            raw_forecast = row['forecast_high'] if t_type == "HIGH" else row['forecast_low']
            final_pred = raw_forecast + blended_bias
            
            bias_str = f"{blended_bias:+.2f}°F"
            raw_str = f"{raw_forecast}°F"
            final_str = f"{final_pred:.2f}°F"
            
            print(f"{station_id:<6} | {t_type:<10} | {raw_str:<8} | {bias_str:<12} | {final_str:<25}")

if __name__ == "__main__":
    execute_inference()
