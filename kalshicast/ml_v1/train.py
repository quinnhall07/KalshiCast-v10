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
        
        # --- Phase 1: Train on 60%, early stop on VAL (60-80%) ---
        # Early stopping watches the validation set to find when boosting
        # should stop. The test set is NEVER exposed to the training loop.
        
        xgb_params = params['xgb'].copy()
        xgb_params['n_estimators'] = 5000  # High ceiling; early stopping finds the real optimum
        xgb_params['early_stopping_rounds'] = 50
        xgb_params['eval_metric'] = 'mae'
        
        m_xgb_val = xgb.XGBRegressor(**xgb_params)
        m_xgb_val.fit(train[feat], train[target],
                       eval_set=[(val[feat], val[target])], verbose=False)
        xgb_best_iter = m_xgb_val.best_iteration + 1  # 0-indexed -> count
        
        lgbm_params = params['lgbm'].copy()
        lgbm_params['n_estimators'] = 5000
        
        m_lgbm_val = lgb.LGBMRegressor(**lgbm_params)
        m_lgbm_val.fit(train[feat], train[target],
                        eval_set=[(val[feat], val[target])],
                        callbacks=[lgb.early_stopping(50), lgb.log_evaluation(period=0)])
        lgbm_best_iter = m_lgbm_val.best_iteration_ + 1
        
        # Save Phase 1 models for backtesting (trained on 60%, never saw test)
        m_xgb_val.save_model(xgb_bt_path)
        m_lgbm_val.booster_.save_model(lgbm_bt_path)
        
        # Report TRUE holdout MAE on test set (never used for any training decision)
        p_xgb_test = m_xgb_val.predict(test[feat])
        p_lgbm_test = m_lgbm_val.predict(test[feat])
        p_final = (p_xgb_test + p_lgbm_test) / 2
        mae = mean_absolute_error(test[target], p_final)
        log.info(f"  📊 {station_id} {t_type} TRUE Holdout MAE (80-100%): {mae:.3f}°F")
        log.info(f"     XGB best iters: {xgb_best_iter} | LGB best iters: {lgbm_best_iter}")
        
        # --- Phase 2: Retrain on 100% of data for production deployment ---
        # Scale n_estimators proportionally: more data → more iterations to converge
        scale_factor = len(df) / len(train)  # 100% / 60% ≈ 1.67
        
        xgb_prod_params = params['xgb'].copy()
        xgb_prod_params['n_estimators'] = int(xgb_best_iter * scale_factor)
        # Remove early stopping keys for final training (no holdout to stop against)
        xgb_prod_params.pop('early_stopping_rounds', None)
        xgb_prod_params.pop('eval_metric', None)
        
        m_xgb_prod = xgb.XGBRegressor(**xgb_prod_params)
        m_xgb_prod.fit(df[feat], df[target])
        m_xgb_prod.save_model(xgb_path)
        
        lgbm_prod_params = params['lgbm'].copy()
        lgbm_prod_params['n_estimators'] = int(lgbm_best_iter * scale_factor)
        
        m_lgbm_prod = lgb.LGBMRegressor(**lgbm_prod_params)
        m_lgbm_prod.fit(df[feat], df[target])
        m_lgbm_prod.booster_.save_model(lgbm_path)
        
        log.info(f"  ✅ {station_id} {t_type} production model saved "
                 f"(100% data, XGB iters: {xgb_prod_params['n_estimators']}, "
                 f"LGB iters: {lgbm_prod_params['n_estimators']})")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stations = get_stations(active_only=True)
    
    log.info(f"🚀 Compiling Models for {len(stations)} stations via Multiprocessing...")
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        for s in stations:
            executor.submit(train_station_models, s["station_id"], s["lat"], s["lon"])