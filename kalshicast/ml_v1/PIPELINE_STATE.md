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
- **Strategy (M1 / Hardened):** Upgraded from generic expanding windows to a **6-fold Walk-Forward TimeSeriesSplit** operating over strict 45-day blocks. Hyperparameters are validated uniformly across rotating macro-seasons, completely eradicating specific-season early-stopping bias.
- **Search Space (M3):** Includes regularization parameters (`reg_alpha`, `reg_lambda`, `lambda_l1`, `lambda_l2`) to prevent overfitting on small station datasets.
- **Outcome:** Generates `params.json` with highly robust, cross-validated settings and global optimal `n_estimators`.

---

## 3. Training & Production Compilation (`train.py`)

A two-phase approach that tightly synchronizes with the output of `tune.py` to prevent redundant seasonal overfitting.

### Phase 1: Backtest and Blend Configuration (60/20/20)
- **Train (0-60%):** Fits the trees using the optimal `n_estimators` dynamically loaded from `params.json`. *No secondary early stopping is used, trusting the robust walk-forward calculation.*
- **Blend Optimization (60-80%):** Validates and blends XGBoost + LightGBM to minimize Custom Bracket Loss.
- **Test (80-100%):** **True Holdout**. Never seen by training or blend weight logic.
- **Output:** Phase 1 models are saved as `model_backtest.*` alongside the globally optimized `blend_weight.json`.

### Phase 2: Production Scaling (100%)
- Retrains on **100% of data** using carefully constrained iterations.
- **Scaling Factor:** Base `n_estimators` from tuning is scaled by **1.10x** (a conservative +10% boost) to account for the extra data volume, replacing the structurally dangerous 1.67x linear multiplier.
- **Output:** Saves final production artifacts (`model.json` / `model.txt`).

---

## 4. Backtesting & Ensembling (`backtest.py`)

### Out-of-Sample Evaluation (C3)
- Backtest strictly loads `model_backtest.*` files (trained on 0-60%) to execute evaluation sweeps on the untouched 80-100% test set.
- **Zero Leakage:** Complete logical separation preventing recursive data evaluation.

### Kalshicast-Specific Objective Ensembling (N1)
- **Bracket Prioritization (Rule #8):** Eliminated vanilla MAE from blending calculations. Weight optimization explicitly utilizes a Custom Bracket Loss function aggressively taxing error radii extending beyond 1.5°F natively to protect perfect Kalshi hit payouts.
- **Anti-Collapse (Rule #6):** Features a dedicated L2 Mixing Penalty matrix to terminate ensemble monopole decay, mathematically ensuring diverse deployment matrices and preventing 1.0/0.0 single-model collapses.
- **Diagnostics:** Comprehensive bias analysis by Month, Magnitude (Cold/Normal/Hot), and MAE thresholds natively exported.

---

## 5. Metadata & Versioning

- **Phoenix Timezone (M4):** Fixed to `America/Phoenix` (no DST) to align forecast days with actual observation periods in Arizona.
- **Versioning:** governs model subdirectories (e.g., `v1.4_rolling`).
- **Standardized Features:** Managed via `config.py` to ensure `tune.py` and `train.py` are always in sync.
