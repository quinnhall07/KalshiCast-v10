import pandas as pd

df = pd.read_csv(r'C:\Users\geeti\Documents\Kalshicast-v10\kalshicast\ml_v1\models\v1.4_rolling\backtest_summary.csv')

print('=== OVERALL IMPROVEMENT ===')
print(f'Mean MAE Improvement: {df["improvement_pct"].mean():.2f}%')
worse = df[df["improvement_pct"] <= 0]
print(f'Worse than GFS count: {len(worse)} out of {len(df)}')
if not worse.empty:
    print(worse[['station', 'target', 'xgb_weight', 'lgbm_weight', 'raw_gfs_mae', 'opt_mae', 'improvement_pct']].to_string(index=False))

print('\n=== BRACKET RULE (±1°F Accuracy) ===')
df['acc_1F_diff'] = df['within_1F_opt'] - df['within_1F_raw']
print(f'Mean 1°F Threshold Improvment: {df["acc_1F_diff"].mean():.2f}%')
print(f'Stations with 1°F regression despite positive MAE improvement:')
bracket_fail = df[(df['improvement_pct'] > 0) & (df['acc_1F_diff'] < 0)]
if not bracket_fail.empty:
    print(bracket_fail[['station', 'target', 'improvement_pct', 'acc_1F_diff', 'within_1F_raw', 'within_1F_opt']].to_string(index=False))

print('\n=== BLEND WEIGHT EXTREMES ===')
extreme_blend = df[(df['xgb_weight'] < 0.05) | (df['xgb_weight'] > 0.95)]
print(f'Models using nearly 100% of a single algorithm: {len(extreme_blend)}')
if not extreme_blend.empty:
    print(extreme_blend[['station', 'target', 'xgb_weight', 'lgbm_weight']].head(10).to_string(index=False))

print('\n=== BIAS FAILURE ===')
df['bias_magnitude_diff'] = df['opt_bias'].abs() - df['raw_gfs_bias'].abs()
worse_bias = df[df['bias_magnitude_diff'] > 0]
print(f'Models that INCREASED overall bias magnitude vs raw GFS: {len(worse_bias)}')
if not worse_bias.empty:
    print(worse_bias[['station', 'target', 'raw_gfs_bias', 'opt_bias', 'bias_magnitude_diff']].head(5).to_string(index=False))
