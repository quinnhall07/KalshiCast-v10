# kalshicast/ml_v1/tune.py
from __future__ import annotations

import os
import json
import logging
import optuna
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error
from concurrent.futures import ProcessPoolExecutor

from kalshicast.ml_v1.config import get_model_path, FEATURES, CURRENT_VERSION
from kalshicast.ml_v1.dataset import fetch_bootstrap_data
from kalshicast.ml_v1.stations import get_stations

log = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

def tune_xgb(X_train, y_train, X_valid, y_valid):
    def objective(trial):
        params = {
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.1, log=True),
            'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
            'subsample': trial.suggest_float('subsample', 0.6, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
            'gamma': trial.suggest_float('gamma', 0.0, 5.0),
            'random_state': 42,
            'n_jobs': -1
        }
        model = xgb.XGBRegressor(**params)
        model.fit(X_train, y_train, eval_set=[(X_valid, y_valid)], verbose=False)
        return mean_absolute_error(y_valid, model.predict(X_valid))
    
    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=50)
    return study.best_params

def tune_lgbm(X_train, y_train, X_valid, y_valid):
    def objective(trial):
        params = {
            'num_leaves': trial.suggest_int('num_leaves', 20, 150),
            'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.1, log=True),
            'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
            'min_child_samples': trial.suggest_int('min_child_samples', 5, 50),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.6, 1.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 1.0),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
            'random_state': 42,
            'verbosity': -1
        }
        model = lgb.LGBMRegressor(**params)
        model.fit(X_train, y_train, eval_set=[(X_valid, y_valid)], callbacks=[lgb.log_evaluation(period=0)])
        return mean_absolute_error(y_valid, model.predict(X_valid))

    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=50)
    return study.best_params

def run_tuning(station_id, lat, lon):
    log.info(f"🧬 Tuning {CURRENT_VERSION} for {station_id}...")
    df = fetch_bootstrap_data(station_id, lat, lon)
    if df.empty: return
    
    df = df.sort_values('time')
    split = int(len(df) * 0.8)
    train, valid = df.iloc[:split], df.iloc[split:]
    
    for t_type in ['HIGH', 'LOW']:
        feat = FEATURES[t_type]
        target = f'target_error_{t_type.lower()}'
        
        best_xgb = tune_xgb(train[feat], train[target], valid[feat], valid[target])
        best_lgbm = tune_lgbm(train[feat], train[target], valid[feat], valid[target])
        
        param_path = os.path.join(os.path.dirname(get_model_path(station_id, t_type, "json")), "params.json")
        with open(param_path, 'w') as f:
            json.dump({'xgb': best_xgb, 'lgbm': best_lgbm}, f, indent=4)
        log.info(f"  ✅ Saved {t_type} params for {station_id}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stations = get_stations(active_only=True)
    
    log.info(f"🧬 Kicking off Optuna Multiprocessing for {len(stations)} stations...")
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        for s in stations:
            executor.submit(run_tuning, s["station_id"], s["lat"], s["lon"])