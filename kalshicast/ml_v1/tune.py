# kalshicast/ml_v1/tune.py
from __future__ import annotations

import os
import json
import logging
import numpy as np
import optuna
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit
from concurrent.futures import ProcessPoolExecutor

from kalshicast.ml_v1.config import get_model_path, FEATURES, CURRENT_VERSION
from kalshicast.ml_v1.dataset import fetch_bootstrap_data
from kalshicast.ml_v1.stations import get_stations

log = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

def tune_xgb(X, y, n_splits=3):
    """Tune XGBoost with TimeSeriesSplit CV for robust hyperparameters.

    Using 3-fold expanding-window CV instead of a single split prevents
    hyperparameters from overfitting to one seasonal validation window.
    """
    tscv = TimeSeriesSplit(n_splits=n_splits)

    def objective(trial):
        params = {
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.1, log=True),
            'n_estimators': 2000,
            'subsample': trial.suggest_float('subsample', 0.6, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
            'gamma': trial.suggest_float('gamma', 0.0, 5.0),
            'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 10.0, log=True),
            'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 10.0, log=True),
            'early_stopping_rounds': 50,
            'eval_metric': 'mae',
            'random_state': 42,
            'n_jobs': 2
        }
        scores = []
        best_iters = []
        for train_idx, val_idx in tscv.split(X):
            model = xgb.XGBRegressor(**params)
            model.fit(X.iloc[train_idx], y.iloc[train_idx],
                      eval_set=[(X.iloc[val_idx], y.iloc[val_idx])], verbose=False)
            pred = model.predict(X.iloc[val_idx])
            scores.append(mean_absolute_error(y.iloc[val_idx], pred))
            b_iter = getattr(model, 'best_iteration', None)
            best_iters.append((b_iter + 1) if b_iter is not None else params['n_estimators'])
        trial.set_user_attr('n_estimators', int(np.mean(best_iters)))
        return np.mean(scores)

    sampler = optuna.samplers.TPESampler(seed=42)
    study = optuna.create_study(direction='minimize', sampler=sampler)
    study.optimize(objective, n_trials=50)
    best = study.best_params.copy()
    best['n_estimators'] = study.best_trial.user_attrs['n_estimators']
    return best

def tune_lgbm(X, y, n_splits=3):
    """Tune LightGBM with TimeSeriesSplit CV for robust hyperparameters."""
    tscv = TimeSeriesSplit(n_splits=n_splits)

    def objective(trial):
        params = {
            'num_leaves': trial.suggest_int('num_leaves', 20, 150),
            'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.1, log=True),
            'n_estimators': 2000,
            'min_child_samples': trial.suggest_int('min_child_samples', 5, 50),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.6, 1.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 1.0),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
            'lambda_l1': trial.suggest_float('lambda_l1', 1e-8, 10.0, log=True),
            'lambda_l2': trial.suggest_float('lambda_l2', 1e-8, 10.0, log=True),
            'random_state': 42,
            'n_jobs': 2,
            'verbosity': -1
        }
        scores = []
        best_iters = []
        for train_idx, val_idx in tscv.split(X):
            model = lgb.LGBMRegressor(**params)
            model.fit(X.iloc[train_idx], y.iloc[train_idx],
                      eval_set=[(X.iloc[val_idx], y.iloc[val_idx])],
                      callbacks=[lgb.early_stopping(50), lgb.log_evaluation(period=0)])
            pred = model.predict(X.iloc[val_idx])
            scores.append(mean_absolute_error(y.iloc[val_idx], pred))
            best_iters.append(model.best_iteration_ + 1)
        trial.set_user_attr('n_estimators', int(np.mean(best_iters)))
        return np.mean(scores)

    sampler = optuna.samplers.TPESampler(seed=42)
    study = optuna.create_study(direction='minimize', sampler=sampler)
    study.optimize(objective, n_trials=50)
    best = study.best_params.copy()
    best['n_estimators'] = study.best_trial.user_attrs['n_estimators']
    return best

def run_tuning(station_id, lat, lon):
    log.info(f"🧬 Tuning {CURRENT_VERSION} for {station_id}...")
    df = fetch_bootstrap_data(station_id, lat, lon)
    if df.empty: return

    df = df.sort_values('time')
    n = len(df)
    # Hold out final 20% as test — tuning never sees this data.
    # TimeSeriesSplit creates expanding windows within the 80% tuning data.
    tuning_data = df.iloc[:int(n * 0.8)]

    for t_type in ['HIGH', 'LOW']:
        feat = FEATURES[t_type]
        target = f'target_error_{t_type.lower()}'

        best_xgb = tune_xgb(tuning_data[feat], tuning_data[target])
        best_lgbm = tune_lgbm(tuning_data[feat], tuning_data[target])

        # Persist complete params including fixed settings
        best_xgb['random_state'] = 42
        best_xgb['n_jobs'] = 2
        best_lgbm['random_state'] = 42
        best_lgbm['n_jobs'] = 2
        best_lgbm['verbosity'] = -1

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