# kalshicast/ml_v1/tune.py
from __future__ import annotations

import os
import json
import logging
import numpy as np
import pandas as pd
import optuna
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit
from concurrent.futures import ProcessPoolExecutor, as_completed
import psutil
from tqdm import tqdm

from kalshicast.ml_v1.config import get_model_path, FEATURES, CURRENT_VERSION, BASE_MODELS_DIR
from kalshicast.ml_v1.dataset import fetch_bootstrap_data
from kalshicast.config.stations import get_stations

log = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

def tune_xgb(X, y, n_splits=6):
    """Tune XGBoost with TimeSeriesSplit CV for robust hyperparameters.

    Using 6-fold walk-forward CV over 45-day test windows instead of a
    single split prevents hyperparameters from overfitting to one seasonal
    validation window. MedianPruner kills trials early if Fold 1 MAE is already
    worse than the median of completed trials, saving ~30-40% of compute.
    """
    tscv = TimeSeriesSplit(n_splits=n_splits, test_size=45)
    pruner = optuna.pruners.MedianPruner(n_warmup_steps=1)

    def objective(trial):
        params = {
            # Tightened max_depth: 14 features + ~600 rows -> depth 10 creates
            # 1024 leaf nodes, extreme overfitting. Depth 3-7 is optimal for
            # dense tabular weather data.
            'max_depth': trial.suggest_int('max_depth', 3, 7),
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
        for fold_idx, (train_idx, val_idx) in enumerate(tscv.split(X)):
            model = xgb.XGBRegressor(**params)
            model.fit(X.iloc[train_idx], y.iloc[train_idx],
                      eval_set=[(X.iloc[val_idx], y.iloc[val_idx])], verbose=False)
            pred = model.predict(X.iloc[val_idx])
            scores.append(mean_absolute_error(y.iloc[val_idx], pred))
            b_iter = getattr(model, 'best_iteration', None)
            best_iters.append((b_iter + 1) if b_iter is not None else params['n_estimators'])

            # Report intermediate MAE for pruning
            trial.report(np.mean(scores), fold_idx)
            if trial.should_prune():
                raise optuna.TrialPruned()

        # Store per-fold diagnostics for the CSV export
        for i, (s, it) in enumerate(zip(scores, best_iters)):
            trial.set_user_attr(f'fold{i+1}_mae', round(s, 4))
            trial.set_user_attr(f'fold{i+1}_iters', it)
        trial.set_user_attr('iter_spread', max(best_iters) - min(best_iters))

        weights = np.arange(1, len(best_iters) + 1, dtype=float)
        trial.set_user_attr('n_estimators', int(np.average(best_iters, weights=weights)))
        return np.mean(scores)

    sampler = optuna.samplers.TPESampler(seed=42)
    study = optuna.create_study(direction='minimize', sampler=sampler, pruner=pruner)
    study.optimize(objective, n_trials=50)
    best = study.best_params.copy()
    best['n_estimators'] = study.best_trial.user_attrs['n_estimators']
    return best, study

def tune_lgbm(X, y, n_splits=6):
    """Tune LightGBM with TimeSeriesSplit CV for robust hyperparameters."""
    tscv = TimeSeriesSplit(n_splits=n_splits, test_size=45)
    pruner = optuna.pruners.MedianPruner(n_warmup_steps=1)

    def objective(trial):
        params = {
            # Tightened num_leaves: 150 leaves with ~600 rows causes immediate
            # overfitting (visible in tuning output as models stopping at 7-44
            # iterations). Cap at 63 (~2^6) for stable convergence.
            'num_leaves': trial.suggest_int('num_leaves', 15, 63),
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
        for fold_idx, (train_idx, val_idx) in enumerate(tscv.split(X)):
            model = lgb.LGBMRegressor(**params)
            model.fit(X.iloc[train_idx], y.iloc[train_idx],
                      eval_set=[(X.iloc[val_idx], y.iloc[val_idx])],
                      eval_metric='mae',
                      callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(period=0)])
            pred = model.predict(X.iloc[val_idx])
            scores.append(mean_absolute_error(y.iloc[val_idx], pred))
            best_iters.append(model.best_iteration_ + 1)

            # Report intermediate MAE for pruning
            trial.report(np.mean(scores), fold_idx)
            if trial.should_prune():
                raise optuna.TrialPruned()

        # Store per-fold diagnostics for the CSV export
        for i, (s, it) in enumerate(zip(scores, best_iters)):
            trial.set_user_attr(f'fold{i+1}_mae', round(s, 4))
            trial.set_user_attr(f'fold{i+1}_iters', it)
        trial.set_user_attr('iter_spread', max(best_iters) - min(best_iters))

        weights = np.arange(1, len(best_iters) + 1, dtype=float)
        trial.set_user_attr('n_estimators', int(np.average(best_iters, weights=weights)))
        return np.mean(scores)

    sampler = optuna.samplers.TPESampler(seed=42)
    study = optuna.create_study(direction='minimize', sampler=sampler, pruner=pruner)
    study.optimize(objective, n_trials=50)
    best = study.best_params.copy()
    best['n_estimators'] = study.best_trial.user_attrs['n_estimators']
    return best, study


def _export_study_diagnostics(study, station_id, t_type, model_name, model_dir):
    """Export per-trial Optuna diagnostics to CSV for post-hoc analysis.

    Each row = one trial. Columns include hyperparams, per-fold MAE,
    per-fold iteration counts, iteration spread (overfitting signal),
    pruning status, and overall MAE. Feed this CSV to an AI agent to
    instantly diagnose search space issues, seasonal fold bias, and
    overfitting patterns.
    """
    rows = []
    for trial in study.trials:
        row = {
            'station': station_id,
            'target': t_type,
            'model': model_name,
            'trial': trial.number,
            'state': trial.state.name,  # COMPLETE, PRUNED, FAIL
            'mean_mae': round(trial.value, 4) if trial.value is not None else None,
        }
        # Hyperparameters
        row.update(trial.params)
        # Per-fold diagnostics (only present on completed trials)
        row.update({k: v for k, v in trial.user_attrs.items()})
        rows.append(row)

    return pd.DataFrame(rows)


def run_tuning(station_id, lat, lon):
    log.info(f"\U0001f9ec Tuning {CURRENT_VERSION} for {station_id}...")
    df = fetch_bootstrap_data(station_id, lat, lon)
    if df.empty: return

    df = df.sort_values('time')
    n = len(df)
    # Hold out final 20% as test — tuning never sees this data.
    # TimeSeriesSplit creates expanding windows within the 80% tuning data.
    tuning_data = df.iloc[:int(n * 0.8)]

    diag_frames = []

    for t_type in ['HIGH', 'LOW']:
        feat = FEATURES[t_type]
        target = f'target_error_{t_type.lower()}'

        best_xgb, study_xgb = tune_xgb(tuning_data[feat], tuning_data[target])
        best_lgbm, study_lgbm = tune_lgbm(tuning_data[feat], tuning_data[target])

        # Persist complete params including fixed settings
        best_xgb['random_state'] = 42
        best_xgb['n_jobs'] = 2
        best_lgbm['random_state'] = 42
        best_lgbm['n_jobs'] = 2
        best_lgbm['verbosity'] = -1

        model_dir = os.path.dirname(get_model_path(station_id, t_type, "json"))
        param_path = os.path.join(model_dir, "params.json")
        with open(param_path, 'w') as f:
            json.dump({'xgb': best_xgb, 'lgbm': best_lgbm}, f, indent=4)
        log.info(f"  \u2705 Saved {t_type} params for {station_id}")

        # Collect per-trial diagnostics
        diag_frames.append(_export_study_diagnostics(study_xgb, station_id, t_type, 'xgb', model_dir))
        diag_frames.append(_export_study_diagnostics(study_lgbm, station_id, t_type, 'lgbm', model_dir))

    # Save per-station diagnostics CSV
    if diag_frames:
        diag_df = pd.concat(diag_frames, ignore_index=True)
        diag_path = os.path.join(os.path.dirname(get_model_path(station_id, 'HIGH', 'json')), '..', 'tuning_diagnostics.csv')
        diag_path = os.path.normpath(diag_path)
        # Append if file exists (other stations running in parallel)
        if os.path.exists(diag_path):
            existing = pd.read_csv(diag_path)
            # Remove old rows for this station before appending fresh ones
            existing = existing[existing['station'] != station_id]
            diag_df = pd.concat([existing, diag_df], ignore_index=True)
        diag_df.to_csv(diag_path, index=False)
        log.info(f"  \U0001f4ca Saved tuning diagnostics to {diag_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stations = get_stations(active_only=True)

    log.info(f"\U0001f9ec Kicking off Optuna Multiprocessing for {len(stations)} stations...")

    # Clear old diagnostics file so we start fresh
    diag_path = os.path.join(BASE_MODELS_DIR, "tuning_diagnostics.csv")
    if os.path.exists(diag_path):
        os.remove(diag_path)

    # Collect futures so crashed stations surface their exceptions
    # instead of being silently swallowed by the executor.
    futures = {}
    with ProcessPoolExecutor(max_workers=4) as executor:
        for s in stations:
            f = executor.submit(run_tuning, s["station_id"], s["lat"], s["lon"])
            futures[f] = s["station_id"]
            
        with tqdm(total=len(futures), desc="Tuning Stations", unit="station") as pbar:
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    log.error(f"❌ Tuning failed for {futures[f]}: {e}")
                
                cpu_usage = psutil.cpu_percent(interval=None)
                pbar.set_postfix({'CPU': f'{cpu_usage:.1f}%'})
                pbar.update(1)

    log.info(f"\u2705 Tuning complete. Feed '{diag_path}' to the AI for analysis.")