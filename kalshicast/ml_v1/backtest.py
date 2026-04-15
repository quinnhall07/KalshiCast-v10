# kalshicast/ml_v1/backtest.py
from __future__ import annotations

import os
import json
import pandas as pd
import numpy as np
import xgboost as xgb
import lightgbm as lgb
import logging
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from scipy.optimize import minimize_scalar

from kalshicast.ml_v1.config import get_model_path, FEATURES, CURRENT_VERSION, DATA_DIR
from kalshicast.ml_v1.stations import get_stations

log = logging.getLogger(__name__)

def run_stacked_backtest(station_id: str):
    csv_path = os.path.join(DATA_DIR, f"{station_id}_bootstrap.csv")
    if not os.path.exists(csv_path):
        log.error(f"❌ No bootstrap data found for {station_id}.")
        return

    df = pd.read_csv(csv_path, parse_dates=['time']).sort_values('time')
    n = len(df)
    val_idx = int(n * 0.6)
    test_idx = int(n * 0.8)
    val_df = df.iloc[val_idx:test_idx].copy()   # 60-80%: optimize blend weight here
    test_df = df.iloc[test_idx:].copy()          # 80-100%: untouched final evaluation

    log.info(f"\n{'='*85}")
    log.info(f" 📈 STACKED TOURNAMENT & EXTENSIVE BACKTEST [{CURRENT_VERSION}]: {station_id}")
    log.info(f"  Split: val={len(val_df)} rows (60-80%) | test={len(test_df)} rows (80-100%)")
    log.info(f"{'='*85}")

    for t_type in ['HIGH', 'LOW']:
        feats = FEATURES[t_type]
        target_col = f'target_error_{t_type.lower()}'
        actual_col = f'actual_{t_type.lower()}'
        raw_f_col  = f'forecast_{t_type.lower()}'

        # Load BACKTEST models (trained on 60% data, never saw val/test)
        # NOT production models (trained on 100%), which would make this in-sample
        xgb_path = get_model_path(station_id, t_type, "json")
        lgbm_path = get_model_path(station_id, t_type, "txt")
        xgb_bt_path = xgb_path.replace("model.", "model_backtest.")
        lgbm_bt_path = lgbm_path.replace("model.", "model_backtest.")

        # Prefer backtest models; fall back to production if not yet generated
        xgb_load = xgb_bt_path if os.path.exists(xgb_bt_path) else xgb_path
        lgbm_load = lgbm_bt_path if os.path.exists(lgbm_bt_path) else lgbm_path

        if not os.path.exists(xgb_load) or not os.path.exists(lgbm_load):
            log.warning(f"⚠️ Models missing for {station_id} {t_type}. Skipping.")
            continue

        if xgb_load == xgb_path:
            log.warning(f"⚠️ No backtest models found for {station_id} {t_type} — "
                        f"using production models (re-run train.py to generate backtest models)")

        model_dir = os.path.dirname(xgb_path)

        m_xgb = xgb.XGBRegressor()
        m_xgb.load_model(xgb_load)
        m_lgbm = lgb.Booster(model_file=lgbm_load)

        # --- Phase 1: Find optimal blend weight on VALIDATION set (60-80%) ---
        val_p_xgb = m_xgb.predict(val_df[feats])
        val_p_lgbm = m_lgbm.predict(val_df[feats])

        best_w, min_val_mae = 0.5, 99.9
        def _blend_mae(w):
            p_w = (w * val_p_xgb) + ((1 - w) * val_p_lgbm)
            return mean_absolute_error(val_df[actual_col], val_df[raw_f_col] + p_w)
        result = minimize_scalar(_blend_mae, bounds=(0, 1), method='bounded')
        best_w = result.x
        min_val_mae = result.fun

        log.info(f"\n  🔧 {t_type} Blend weight selected on validation set: {best_w:.4f} XGB | {1-best_w:.4f} LGB (val MAE: {min_val_mae:.3f}°F)")

        # --- Phase 2: Evaluate on UNTOUCHED TEST set (80-100%) ---
        p_xgb = m_xgb.predict(test_df[feats])
        p_lgbm = m_lgbm.predict(test_df[feats])
        p_simple = (p_xgb + p_lgbm) / 2

        # Apply the weight found on the validation set to the test set
        p_opt_err = (best_w * p_xgb) + ((1 - best_w) * p_lgbm)
        p_opt_forecast = test_df[raw_f_col] + p_opt_err
        
        raw_forecast = test_df[raw_f_col]
        actuals = test_df[actual_col]
        
        # Calculate broader array of metrics to analyze performance & hidden biases
        abs_err_raw = np.abs(raw_forecast - actuals)
        abs_err_opt = np.abs(p_opt_forecast - actuals)
        
        err_raw = raw_forecast - actuals # Prediction - Actual
        err_opt = p_opt_forecast - actuals
        
        mae_raw = np.mean(abs_err_raw)
        rmse_raw = np.sqrt(mean_squared_error(actuals, raw_forecast))
        bias_raw = np.mean(err_raw)
        
        mae_opt = mean_absolute_error(actuals, p_opt_forecast)
        rmse_opt = np.sqrt(mean_squared_error(actuals, p_opt_forecast))
        bias_opt = np.mean(err_opt)
        
        # Threshold Accuracy
        within_1_raw = np.mean(abs_err_raw <= 1.0) * 100
        within_2_raw = np.mean(abs_err_raw <= 2.0) * 100
        within_3_raw = np.mean(abs_err_raw <= 3.0) * 100
        within_1_opt = np.mean(abs_err_opt <= 1.0) * 100
        within_2_opt = np.mean(abs_err_opt <= 2.0) * 100
        within_3_opt = np.mean(abs_err_opt <= 3.0) * 100
        
        # Additional Models MAE
        mae_xgb = mean_absolute_error(actuals, raw_forecast + p_xgb)
        mae_lgb = mean_absolute_error(actuals, raw_forecast + p_lgbm)
        mae_simple = mean_absolute_error(actuals, raw_forecast + p_simple)
        
        log.info(f"\n[ {t_type} Results ]")
        log.info(f"--- Raw NOAA GFS Baseline ---")
        log.info(f"MAE:       {mae_raw:.3f}°F   RMSE: {rmse_raw:.3f}°F   Bias: {bias_raw:+.3f}°F")
        log.info(f"Accuracy:  ≤1°F: {within_1_raw:.1f}% | ≤2°F: {within_2_raw:.1f}% | ≤3°F: {within_3_raw:.1f}%")
        
        log.info(f"\n--- Model Components ---")
        log.info(f"XGBoost Only MAE:       {mae_xgb:.3f}°F ({(mae_raw-mae_xgb)/mae_raw:+.1%})")
        log.info(f"LightGBM Only MAE:      {mae_lgb:.3f}°F ({(mae_raw-mae_lgb)/mae_raw:+.1%})")
        log.info(f"Simple Stack (50/50):   {mae_simple:.3f}°F ({(mae_raw-mae_simple)/mae_raw:+.1%})")
        
        log.info(f"\n--- Optimized Fusion (Weight: {best_w:.2f} XGB | {1-best_w:.2f} LGB) ---")
        log.info(f"MAE:       {mae_opt:.3f}°F ({(mae_raw-mae_opt)/mae_raw:+.1%})   RMSE: {rmse_opt:.3f}°F   Bias: {bias_opt:+.3f}°F")
        log.info(f"Accuracy:  ≤1°F: {within_1_opt:.1f}% | ≤2°F: {within_2_opt:.1f}% | ≤3°F: {within_3_opt:.1f}%")
        
        improvement_pct = (mae_raw - mae_opt) / mae_raw * 100
        if improvement_pct < 0:
            log.warning("⚠️ Warning: Stacked model performed worse than Raw GFS Baseline!")

        # Generate an extensive dataframe with all error data for diagnostics and bias-hunting
        results_df = test_df[['time', actual_col, raw_f_col]].copy()
        results_df.rename(columns={actual_col: 'actual', raw_f_col: 'forecast_raw'}, inplace=True)
        results_df['xgb_pred_err'] = p_xgb
        results_df['lgb_pred_err'] = p_lgbm
        results_df['opt_pred_err'] = p_opt_err
        results_df['forecast_xgb'] = results_df['forecast_raw'] + p_xgb
        results_df['forecast_lgb'] = results_df['forecast_raw'] + p_lgbm
        results_df['forecast_opt'] = p_opt_forecast
        
        results_df['error_raw'] = err_raw
        results_df['error_opt'] = err_opt
        results_df['abs_error_raw'] = abs_err_raw
        results_df['abs_error_opt'] = abs_err_opt
        
        # Include calendar features to help identify seasonal or monthly biases
        results_df['month'] = results_df['time'].dt.month
        
        # Look for magnitude biases (does it fail on extremes?)
        actual_q33 = results_df['actual'].quantile(0.33)
        actual_q66 = results_df['actual'].quantile(0.66)
        
        def magnitude_bucket(val):
            if val <= actual_q33: return "Cold"
            elif val <= actual_q66: return "Normal"
            else: return "Hot"
            
        results_df['temp_bracket'] = results_df['actual'].apply(magnitude_bucket)
        
        # Save extensive CSV data for visual inspection
        extensive_csv_path = os.path.join(model_dir, "extensive_backtest_results.csv")
        results_df.to_csv(extensive_csv_path, index=False)
        log.info(f"\n  📊 Saved extensive detailed predictions to: {extensive_csv_path}")

        # Compute and display aggregations for quick pattern detection
        log.info(f"  🔍 Diagnostic: Monthly Bias (Error = Pred - Actual)")
        monthly_stats = results_df.groupby('month').agg(
            raw_mae=('abs_error_raw', 'mean'),
            opt_mae=('abs_error_opt', 'mean'),
            raw_bias=('error_raw', 'mean'),
            opt_bias=('error_opt', 'mean'),
            n_samples=('time', 'count')
        ).round(3)
        log.info("\n" + monthly_stats.to_string())
        
        log.info(f"\n  🔍 Diagnostic: Bias based on Actual Temperature Bracket")
        bracket_stats = results_df.groupby('temp_bracket').agg(
            raw_mae=('abs_error_raw', 'mean'),
            opt_mae=('abs_error_opt', 'mean'),
            raw_bias=('error_raw', 'mean'),
            opt_bias=('error_opt', 'mean'),
            n_samples=('time', 'count')
        ).round(3)
        log.info("\n" + bracket_stats.to_string())

        # Persist optimal blend weight for production use
        weight_path = os.path.join(model_dir, "blend_weight.json")
        weights_by_target = {}
        if os.path.exists(weight_path):
            with open(weight_path, 'r') as f:
                try:
                    existing = json.load(f)
                    if isinstance(existing, dict):
                        weights_by_target = existing
                except json.JSONDecodeError:
                    pass
        
        # Expanding weight payload with thorough validation metrics
        weights_by_target[t_type] = {
            'xgb_weight': round(best_w, 3),
            'lgbm_weight': round(1 - best_w, 3),
            'metrics_base': {
                'raw_gfs_mae': round(mae_raw, 4),
                'raw_gfs_rmse': round(rmse_raw, 4),
                'raw_gfs_bias': round(bias_raw, 4),
            },
            'metrics_opt': {
                'holdout_mae': round(mae_opt, 4),
                'holdout_rmse': round(rmse_opt, 4),
                'holdout_bias': round(bias_opt, 4),
            },
            'accuracy_thresholds': {
                'within_1F_raw': round(within_1_raw, 2),
                'within_1F_opt': round(within_1_opt, 2),
                'within_2F_raw': round(within_2_raw, 2),
                'within_2F_opt': round(within_2_opt, 2),
                'within_3F_raw': round(within_3_raw, 2),
                'within_3F_opt': round(within_3_opt, 2),
            },
            'improvement_pct': round(improvement_pct, 2),
        }
        with open(weight_path, 'w') as f:
            json.dump(weights_by_target, f, indent=4)
        log.info(f"  💾 Saved enhanced blend weight and metrics to {weight_path}\n")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stations = get_stations(active_only=True)
    
    # We run backtests sequentially so the logs are highly readable
    for s in stations:
        run_stacked_backtest(s["station_id"])