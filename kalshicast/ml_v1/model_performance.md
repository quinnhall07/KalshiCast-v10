# 📈 Kalshicast ML_V1 Performance Log

## Model Evolution Strategy
* **V1.0**: Raw GFS + Day of Year (XGBoost)
* **V1.1**: Added Physics (Radiation, Wind, Precip)
* **V1.2**: Added Autoregressive Lags (1d, 2d Error)
* **V1.3**: Added Stacking (XGBoost + LightGBM)
* **V1.4 (Current)**: Added Regime Memory (7d/14d Rolling Windows)

---

## 🏆 Current Leaderboard (Out-of-Sample)
*Backtest Period: 2025-08-25 to 2026-03-31*

### KORD (Chicago O'Hare)
| Metric | Raw GFS | ML Stacked (V1.4) | % Improvement |
| :--- | :--- | :--- | :--- |
| **High Temp MAE** | 2.057°F | **1.412°F** | **+31.33%** |
| **Low Temp MAE** | 1.639°F | **1.463°F** | **+10.76%** |

### KNYC (Central Park)
| Metric | Raw GFS | ML Stacked (V1.4) | % Improvement |
| :--- | :--- | :--- | :--- |
| **High Temp MAE** | 2.073°F | **1.499°F** | **+27.71%** |
| **Low Temp MAE** | 1.923°F | **1.640°F** | **+14.72%** |

---

## 📓 Research Notes
* **Regime Memory**: Rolling 7-day windows significantly stabilized the Low temperature predictions in NYC.
* **XGB vs LGBM**: LightGBM shows a slight edge in high-volatility spring days (standard deviation > 3.0).
* **Next Steps**: Test Meta-Learner (Ridge Regression) to blend weights dynamically based on rolling std.