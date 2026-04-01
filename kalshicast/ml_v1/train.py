# kalshicast/ml_v1/train.py
from __future__ import annotations

import os
import json
import logging
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error
from concurrent.futures import ProcessPoolExecutor

from kalshicast.ml_v1.config import get_model_path, FEATURES, CURRENT_VERSION
from kalshicast.ml_v1.dataset import fetch_bootstrap_data
from kalshicast.ml_v1.stations import get_stations

log = logging.getLogger(__name__)

def train_station_models(station_id, lat, lon):
    log.info(f"🚀 Compiling VERSION: {CURRENT_VERSION} | STATION: {station_id}")
    df = fetch_bootstrap_data(station_id, lat, lon)
    if df.empty: return
    
    df = df.sort_values('time')
    split = int(len(df) * 0.8)
    train, test = df.iloc[:split], df.iloc[split:]

    for t_type in ['HIGH', 'LOW']:
        xgb_path = get_model_path(station_id, t_type, "json")
        lgbm_path = get_model_path(station_id, t_type, "txt")
        param_path = os.path.join(os.path.dirname(xgb_path), "params.json")
        
        if not os.path.exists(param_path):
            log.error(f"❌ No params for {station_id} {t_type}. Skipping.")
            continue
            
        with open(param_path, 'r') as f: params = json.load(f)
        feat = FEATURES[t_type]
        target = f'target_error_{t_type.lower()}'
        
        m_xgb = xgb.XGBRegressor(**params['xgb'])
        m_xgb.fit(train[feat], train[target])
        m_xgb.save_model(xgb_path)
        
        m_lgbm = lgb.LGBMRegressor(**params['lgbm'], verbose=-1)
        m_lgbm.fit(train[feat], train[target])
        m_lgbm.booster_.save_model(lgbm_path)
        
        p_xgb = m_xgb.predict(test[feat])
        p_lgbm = m_lgbm.predict(test[feat])
        p_final = (p_xgb + p_lgbm) / 2
        mae = mean_absolute_error(test[target], p_final)
        log.info(f"  ✅ {station_id} {t_type} Stacked MAE: {mae:.3f}°F")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stations = get_stations(active_only=True)
    
    log.info(f"🚀 Compiling Models for {len(stations)} stations via Multiprocessing...")
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        for s in stations:
            executor.submit(train_station_models, s["station_id"], s["lat"], s["lon"])