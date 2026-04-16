# kalshicast/ml_v1/train.py
from __future__ import annotations

import os
import numpy as np
import json
import logging
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error
from concurrent.futures import ProcessPoolExecutor
from scipy.optimize import minimize

from kalshicast.ml_v1.config import get_model_path, FEATURES, CURRENT_VERSION
from kalshicast.ml_v1.dataset import fetch_bootstrap_data
from kalshicast.config.stations import get_stations

log = logging.getLogger(__name__)

def train_station_models(station_id, lat, lon):
    log.info(f"🚀 Compiling VERSION: {CURRENT_VERSION} | STATION: {station_id}")
    df = fetch_bootstrap_data(station_id, lat, lon)
    if df.empty: return
    
    # --- 3-way split: Train / Validation / Test ---
    # Train (0-60%):  fits the trees
    # Val   (60-80%): early stopping target (finds optimal n_estimators)
    # Test  (80-100%): TRUE holdout — never used for any training decision
    df = df.sort_values('time')
    n = len(df)
    train_end = int(n * 0.6)
    val_end = int(n * 0.8)
    train = df.iloc[:train_end]
    val   = df.iloc[train_end:val_end]
    test  = df.iloc[val_end:]

    for t_type in ['HIGH', 'LOW']:
        xgb_path = get_model_path(station_id, t_type, "json")
        lgbm_path = get_model_path(station_id, t_type, "txt")
        # Backtest models saved separately so backtest.py never evaluates
        # models on data they were trained on
        xgb_bt_path = xgb_path.replace("model.", "model_backtest.")
        lgbm_bt_path = lgbm_path.replace("model.", "model_backtest.")
        param_path = os.path.join(os.path.dirname(xgb_path), "params.json")
        
        if not os.path.exists(param_path):
            log.error(f"❌ No params for {station_id} {t_type}. Skipping.")
            continue
            
        with open(param_path, 'r') as f: params = json.load(f)
        feat = FEATURES[t_type]
        target = f'target_error_{t_type.lower()}'
        
        # --- Phase 1: Train on 0-60%, Evaluate Blend on 60-80% ---
        # We leverage the immutable optimal n_estimators from params.json 
        # (calculated by walk-forward CV in tune.py) rather than running early
        # stopping on a biased seasonal validation holdout.
        
        xgb_params = params['xgb'].copy()
        m_xgb_val = xgb.XGBRegressor(**xgb_params)
        m_xgb_val.fit(train[feat], train[target])
        
        lgbm_params = params['lgbm'].copy()
        m_lgbm_val = lgb.LGBMRegressor(**lgbm_params)
        m_lgbm_val.fit(train[feat], train[target])
        
        # Save Phase 1 models for backtesting (trained on 60%, never saw test)
        m_xgb_val.save_model(xgb_bt_path)
        m_lgbm_val.booster_.save_model(lgbm_bt_path)
        
        # Optimize Blend Weight on Validation Set (60-80% chunk)
        # Rule #6: Calculate blend weight on validation holdout, not test holdout!
        p_xgb_val = m_xgb_val.predict(val[feat])
        p_lgbm_val = m_lgbm_val.predict(val[feat])
        
        def blend_objective(w):
            blended = w[0] * p_xgb_val + (1 - w[0]) * p_lgbm_val
            
            # 1. Bracket Penalty (We want to MINIMIZE the number of misses outside of 1.5F)
            errors = np.abs(val[target].values - blended)
            # Heavy penalty for errors > 1.5F, light penalty for small errors
            bracket_loss = np.sum(np.where(errors > 1.5, errors * 2, errors))
            
            # 2. Ensemble Penalty (regularization to force mixing, peaks at 0.5)
            # We want to encourage diverse ensembles to prevent 100/0 model collapse
            mixing_penalty = 10.0 * (w[0] - 0.5)**2  
            
            return bracket_loss + mixing_penalty
            
        res = minimize(blend_objective, [0.5], bounds=[(0.0, 1.0)])
        opt_w = float(res.x[0])
        
        # Report TRUE holdout MAE on test set (80-100% chunk)
        p_xgb_test = m_xgb_val.predict(test[feat])
        p_lgbm_test = m_lgbm_val.predict(test[feat])
        p_final = opt_w * p_xgb_test + (1 - opt_w) * p_lgbm_test
        mae = mean_absolute_error(test[target], p_final)
        log.info(f"  📊 {station_id} {t_type} TRUE Holdout MAE: {mae:.3f}°F (Blend: {opt_w:.2f} XGB, {1-opt_w:.2f} LGBM)")
        
        # --- Phase 2: Retrain on 100% of data for production deployment ---
        # Scale n_estimators conservatively (+10%) instead of linear scaling (1.67x)
        # to prevent sudden severe overfitting of the tree ensembles.
        
        xgb_prod_params = params['xgb'].copy()
        xgb_prod_params['n_estimators'] = int(xgb_prod_params['n_estimators'] * 1.10)
        
        m_xgb_prod = xgb.XGBRegressor(**xgb_prod_params)
        m_xgb_prod.fit(df[feat], df[target])
        m_xgb_prod.save_model(xgb_path)
        
        lgbm_prod_params = params['lgbm'].copy()
        lgbm_prod_params['n_estimators'] = int(lgbm_prod_params['n_estimators'] * 1.10)
        
        m_lgbm_prod = lgb.LGBMRegressor(**lgbm_prod_params)
        m_lgbm_prod.fit(df[feat], df[target])
        m_lgbm_prod.booster_.save_model(lgbm_path)
        
        # Save the optimal blend weight for the inference pipeline
        blend_path = os.path.join(os.path.dirname(xgb_path), f"blend_{t_type.lower()}.json")
        with open(blend_path, 'w') as f:
            json.dump({'xgb_weight': opt_w, 'lgbm_weight': 1.0 - opt_w}, f, indent=4)
            
        log.info(f"  ✅ {station_id} {t_type} production model saved (100% data, +10% estimators)")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stations = get_stations(active_only=True)
    
    log.info(f"🚀 Compiling Models for {len(stations)} stations via Multiprocessing...")
    
    # Collect futures so crashed stations surface their exceptions
    # instead of being silently swallowed by the executor.
    futures = {}
    with ProcessPoolExecutor(max_workers=4) as executor:
        for s in stations:
            f = executor.submit(train_station_models, s["station_id"], s["lat"], s["lon"])
            futures[f] = s["station_id"]
        for f in futures:
            try:
                f.result()
            except Exception as e:
                log.error(f"\u274c Training failed for {futures[f]}: {e}")