# kalshicast/ml_v1/config.py
import os

# --- VERSION CONTROL ---
# Change this when you start a new experiment (e.g., "v1.4_rolling")
CURRENT_VERSION = "v1.4_rolling"

# --- PATHING ---
ML_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ML_DIR, "data")
BASE_MODELS_DIR = os.path.join(ML_DIR, "models", CURRENT_VERSION)

def get_model_path(station_id, target_type, model_ext):
    """
    Returns a clean path: ml_v1/models/v1.3_stacked/KORD/HIGH/model.json
    Also ensures the folders exist.
    """
    path = os.path.join(BASE_MODELS_DIR, station_id, target_type)
    os.makedirs(path, exist_ok=True)
    return os.path.join(path, f"model.{model_ext}")

# --- FEATURE SETS ---
# We keep these here so tune.py and train.py are always in sync
FEATURES = {
    'HIGH': ['forecast_high', 'forecast_low', 'forecast_diurnal_range',
             'day_sin', 'day_cos', 'forecast_precip', 'forecast_wind',
             'forecast_radiation', 'forecast_evap', 'lag_high_1d', 'lag_high_2d',
             'roll_high_7d_mean', 'roll_high_14d_mean', 'roll_high_7d_std'],
    'LOW':  ['forecast_low', 'forecast_high', 'forecast_diurnal_range',
             'day_sin', 'day_cos', 'forecast_precip', 'forecast_wind',
             'forecast_radiation', 'forecast_evap', 'lag_low_1d', 'lag_low_2d',
             'roll_low_7d_mean', 'roll_low_14d_mean', 'roll_low_7d_std']
}