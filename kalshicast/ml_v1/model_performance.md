# 📈 Kalshicast ML_V1 Performance Log

## 🛠️ Model Evolution & Versioning
| Version | Strategy | Features Added | Status |
| :--- | :--- | :--- | :--- |
| **v1.0** | Baseline XGB | Temp + DayOfYear | 🟢 Deprecated |
| **v1.1** | Physics Boost | Wind + Precip + Radiation | 🟢 Deprecated |
| **v1.2** | Autoregressive | 1d & 2d Error Lags | 🟢 Deprecated |
| **v1.3** | Stacked Ens | XGBoost + LightGBM (50/50) | 🟢 Deprecated |
| **v1.4** | **Regime Memory** | **7d & 14d Rolling Windows** | 🏆 **Current Best** |

---

## 🏆 V1.4 Leaderboard (Out-of-Sample)
*Backtest Run: 2026-03-31 | Period: 2025-08-27 to 2026-03-31*

### KORD (Chicago O'Hare)
| Target | Raw GFS MAE | ML Stacked (v1.4) | Edge Gained |
| :--- | :--- | :--- | :--- |
| **High** | 2.055°F | **1.377°F** | **+33.0%** |
| **Low** | 1.639°F | **1.400°F** | **+14.5%** |

### KNYC (Central Park)
| Target | Raw GFS MAE | ML Stacked (v1.4) | Edge Gained |
| :--- | :--- | :--- | :--- |
| **High** | 2.082°F | **1.469°F** | **+29.5%** |
| **Low** | 1.926°F | **1.606°F** | **+16.6%** |

---

## 🏗️ Infrastructure & Scale Upgrade
* **Station Expansion**: Successfully scaled from 2 stations to the **Kalshi Core 20** NWS ASOS stations.
* **Compute Strategy**: Implemented `ProcessPoolExecutor` (Multiprocessing) to handle parallel tuning and training across 20 cities.
* **Organization**: Moved to **Standardized Pathing Logic**: `models/[Version]/[Station]/[Target]/`.
* **Hardware Alignment**: Optimized for high-wattage CPU P-Core performance (Intel i7-13620H) to handle 80+ simultaneous Optuna studies.

---

## 📓 Research Notes
* **The "LGBM" Dominance**: In KORD and KNYC Highs, LightGBM accounted for **90-100%** of the optimal weight. Its leaf-wise growth is significantly more efficient at utilizing "Regime Memory" (Rolling Mean/Std) than XGBoost.
* **Regime Memory Impact**: Adding the 7-day rolling mean successfully mitigated systemic bias in the GFS, pushing Chicago High-temp accuracy past the **33% improvement** ceiling.
* **Regional Variance**: XGBoost remains a vital "safety" component in NYC Lows (Weight: 0.6), likely due to its more conservative depth-wise splitting in high-noise coastal environments.

---

## 🧪 Next Experiments
* **Spatial Lags**: Incorporating "Upstream" weather data (e.g., Des Moines for Chicago) to catch fronts before they arrive.
* **Meta-Learner**: Replacing static 50/50 stacking with a Ridge Regressor to blend XGB/LGBM weights dynamically based on live volatility (`roll_std`).
* **Hourly Granularity**: Transitioning to 26k+ rows per city to utilize RTX 3050 GPU acceleration.