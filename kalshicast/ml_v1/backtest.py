# kalshicast/ml_v1/backtest.py
from __future__ import annotations

import os
import json
import pandas as pd
import numpy as np
import xgboost as xgb
import lightgbm as lgb
import logging
from sklearn.metrics import mean_absolute_error

from kalshicast.ml_v1.config import get_model_path, FEATURES, CURRENT_VERSION, DATA_DIR
from kalshicast.ml_v1.stations import get_stations

log = logging.getLogger(__name__)

def run_stacked_backtest(station_id: str):
    csv_path = os.path.join(DATA_DIR, f"{station_id}_bootstrap.csv")
    if not os.path.exists(csv_path):
        log.error(f"❌ No bootstrap data found for {station_id}.")
        return

    df = pd.read_csv(csv_path, parse_dates=['time']).sort_values('time')
    split_idx = int(len(df) * 0.8)
    test_df = df.iloc[split_idx:].copy()

    log.info(f"\n{'='*75}")
    log.info(f" 📈 STACKED TOURNAMENT [{CURRENT_VERSION}]: {station_id}")
    log.info(f"{'='*75}")

    for t_type in ['HIGH', 'LOW']:
        feats = FEATURES[t_type]
        target_col = f'target_error_{t_type.lower()}'
        actual_col = f'actual_{t_type.lower()}'
        raw_f_col  = f'forecast_{t_type.lower()}'

        xgb_path = get_model_path(station_id, t_type, "json")
        lgbm_path = get_model_path(station_id, t_type, "txt")

        if not os.path.exists(xgb_path) or not os.path.exists(lgbm_path):
            log.warning(f"⚠️ Models missing for {station_id} {t_type}. Skipping.")
            continue

        m_xgb = xgb.XGBRegressor()
        m_xgb.load_model(xgb_path)
        m_lgbm = lgb.Booster(model_file=lgbm_path)

        p_xgb = m_xgb.predict(test_df[feats])
        p_lgbm = m_lgbm.predict(test_df[feats])
        p_simple = (p_xgb + p_lgbm) / 2
        
        best_w, min_mae = 0.5, 99.9
        for w in np.linspace(0, 1, 11):
            p_w = (w * p_xgb) + ((1 - w) * p_lgbm)
            mae_w = mean_absolute_error(test_df[actual_col], test_df[raw_f_col] + p_w)
            if mae_w < min_mae:
                min_mae, best_w = mae_w, w

        mae_raw = mean_absolute_error(test_df[actual_col], test_df[raw_f_col])
        mae_xgb = mean_absolute_error(test_df[actual_col], test_df[raw_f_col] + p_xgb)
        mae_lgb = mean_absolute_error(test_df[actual_col], test_df[raw_f_col] + p_lgbm)
        mae_simple = mean_absolute_error(test_df[actual_col], test_df[raw_f_col] + p_simple)
        
        log.info(f"\n[ {t_type} Results ]")
        log.info(f"Raw NOAA GFS Error:      {mae_raw:.3f}°F")
        log.info(f"XGBoost Only:           {mae_xgb:.3f}°F ({(mae_raw-mae_xgb)/mae_raw:+.1%})")
        log.info(f"LightGBM Only:          {mae_lgb:.3f}°F ({(mae_raw-mae_lgb)/mae_raw:+.1%})")
        log.info(f"Simple Stack (50/50):   {mae_simple:.3f}°F ({(mae_raw-mae_simple)/mae_raw:+.1%})")
        log.info(f"Optimum Weighted Blend: {min_mae:.3f}°F ({(mae_raw-min_mae)/mae_raw:+.1%}) -> (Weight: {best_w:.1f} XGB)")

        # Persist optimal blend weight for production use
        weight_path = os.path.join(os.path.dirname(xgb_path), "blend_weight.json")
        weights_by_target = {}
        if os.path.exists(weight_path):
            with open(weight_path, 'r') as f:
                try:
                    existing = json.load(f)
                    if isinstance(existing, dict):
                        weights_by_target = existing
                except json.JSONDecodeError:
                    pass
        weights_by_target[t_type] = {
            'xgb_weight': round(best_w, 2),
            'lgbm_weight': round(1 - best_w, 2),
            'holdout_mae': round(min_mae, 4),
            'raw_gfs_mae': round(mae_raw, 4),
            'improvement_pct': round((mae_raw - min_mae) / mae_raw * 100, 2),
        }
        with open(weight_path, 'w') as f:
            json.dump(weights_by_target, f, indent=4)
        log.info(f"  💾 Saved blend weight for {t_type} to {weight_path}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stations = get_stations(active_only=True)
    
    # We run backtests sequentially so the logs are highly readable
    for s in stations:
        run_stacked_backtest(s["station_id"])