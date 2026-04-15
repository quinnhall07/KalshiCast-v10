# ML v1 Pipeline Documentation (Current State — Audit Hardened)

This document details the architecture, data logic, and safeguards of the `ml_v1` weather forecasting pipeline, following the comprehensive 2026 Audit.

## 1. Data Processing (`dataset.py`)

The pipeline prioritizes production realism and statistical integrity over raw data volume.

### Data Logic
- **Actuals:** Fetched from **IEM ASOS (Iowa Environmental Mesonet)**. Authority source for settlement.
- **Realistic Lead Time (C4):** The pipeline now filters for `forecast_lead_hours == 24` (Open-Meteo Previous Runs API). This ensures the model learns to correct the exact error regime it faces in live trading. Mixed data is only kept if realistic volume is < 180 days.
- **Cyclical Encoding (C1):** Replaced linear `day_of_year` with `day_sin`/`day_cos` to eliminate the Dec 31 → Jan 1 discontinuity, allowing trees to learn seasonal continuity.
- **Cross-Target Signal (M5):** Each model sees the opposite high/low forecast and the `forecast_diurnal_range`. This provides physical signal about sky conditions (clear vs overcast) which correlates with GFS bias.

### Key Safeguards
- **Autoregressive Gap Fix:** Resamples to daily calendar (`.asfreq('D')`) before computing features. `.shift(1)` is guaranteed to be "exactly yesterday."
- **Strict Rolling Windows (M2):** No `min_periods`. A 7-day mean always represents exactly 7 consecutive days, ensuring feature semantic consistency.
- **Extreme-Day Diagnostics (N3):** Logs "NaN dropout bias" warnings if IEM outages disproportionately remove extreme weather days (top/bottom 5%) from training data.

---

## 2. Model Tuning (`tune.py`)

- **Optimizer:** Optuna (TPESampler with fixed seed 42 for reproducibility).
- **Strategy (M1):** Replaced single-split tuning with **3-fold TimeSeriesSplit**. Hyperparameters are validated across multiple expanding seasonal windows.
- **Search Space (M3):** Includes regularization parameters (`reg_alpha`, `reg_lambda`, `lambda_l1`, `lambda_l2`) to prevent overfitting on small station datasets.
- **Outcome:** Generates `params.json` with robust, cross-validated settings and optimal `n_estimators`.

---

## 3. Training & Production Compilation (`train.py`)

A two-phase approach that strictly isolates test data.

### Phase 1: Holdout Validation (60/20/20)
- **Train (0-60%):** Fits the trees.
- **Validation (60-80%):** Target for **Early Stopping** to discover iteration counts.
- **Test (80-100%):** **True Holdout**. Never seen by training or stopping logic.
- **Output:** Phase 1 models are saved as `model_backtest.*` for honest evaluation.

### Phase 2: Production Scaling (100%)
- Retrains on **100% of data** using scaled iterations.
- **Scaling Factor:** `n_estimators` from Phase 1 is scaled by **1.67x** (100/60) to account for the larger data base.
- **Output:** Saves final production artifacts (`model.json` / `model.txt`).

---

## 4. Backtesting & Ensembling (`backtest.py`)

### Out-of-Sample Evaluation (C3)
- Backtest now loads `model_backtest.*` files (trained on 60%) to evaluate on the final 20% test set.
- **Zero Leakage:** Prevents the "evaluating on training data" bug.

### Precise Ensembling (N1)
- **Blend Optimization:** Uses `scipy.optimize.minimize_scalar` (bounded) to find the exact optimal blend weight (XGB vs LGBM) on the validation set.
- **Diagnostics:** Comprehensive bias analysis by Month, Magnitude (Cold/Normal/Hot), and MAE thresholds.

---

## 5. Metadata & Versioning

- **Phoenix Timezone (M4):** Fixed to `America/Phoenix` (no DST) to align forecast days with actual observation periods in Arizona.
- **Versioning:** governs model subdirectories (e.g., `v1.4_rolling`).
- **Standardized Features:** Managed via `config.py` to ensure `tune.py` and `train.py` are always in sync.
