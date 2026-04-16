import os
import glob
import pandas as pd
import numpy as np

# Adjust base directory if running from different locations
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, 'models', 'v1.4_rolling')

def analyze_diagnostics():
    print(f"Gathering Optuna Diagnostics from: {MODELS_DIR}\n")
    
    csv_files = glob.glob(os.path.join(MODELS_DIR, '*', 'tuning_diagnostics.csv'))
    if not csv_files:
        print("Error: No tuning_diagnostics.csv files found. Ensure tuning has run.")
        return

    all_dfs = []
    print(f"Found {len(csv_files)} diagnostic files. Aggregating data...")
    for f in sorted(csv_files):
        try:
            df = pd.read_csv(f)
            # station code is typically the parent directory name
            station = os.path.basename(os.path.dirname(f))
            df['station'] = station
            all_dfs.append(df)
        except pd.errors.EmptyDataError:
            print(f"  Warning: Empty CSV file at {f}")
            continue

    if not all_dfs:
        print("No valid data found to analyze.")
        return

    big_df = pd.concat(all_dfs, ignore_index=True)
    complete = big_df[big_df['state'] == 'COMPLETE']
    
    print("-" * 60)
    print(f"Total Optuna Trials Computed: {len(big_df)}")
    print(f"Successfully Completed Trials: {len(complete)}")
    print(f"Pruned (early exit) Trials: {len(big_df[big_df['state'] == 'PRUNED'])}")
    print("-" * 60 + "\n")

    if len(complete) == 0:
        print("No complete trials to analyze.")
        return

    # Extract the BEST trial for each (station, target, model)
    best = complete.loc[complete.groupby(['station', 'target', 'model'])['mean_mae'].idxmin()]
    best = best.sort_values(['station', 'target', 'model'])

    print("SYSTEM MAE HEALTH CHECK")
    high_mae = best[best['mean_mae'] > 3.0]
    if len(high_mae) > 0:
        print("WARNING: The following models have dangerously high Validation MAE (>3.0°):")
        for _, row in high_mae.iterrows():
            print(f"  - {row['station']} [{row['target']}] ({row['model']}): {row['mean_mae']:.4f}° MAE")
    else:
        print("All winning models achieved sub-3.0° MAE on cross-validation.")
        print(f"   (Average MAE across all winning models: {best['mean_mae'].mean():.4f}°)")
    print()

    # Determine CV Fold Column Names dynamically
    cv_mae_cols = [c for c in complete.columns if c.endswith('_mae') and c.startswith('fold')]
    cv_iter_cols = [c for c in complete.columns if c.endswith('_iters') and c.startswith('fold')]

    print("CV FOLD STABILITY ANALYSIS")
    unstable_iters = []
    unstable_maes = []
    
    for _, row in best.iterrows():
        # Iteration Analysis
        iters = [row[c] for c in cv_iter_cols if c in row and pd.notna(row[c])]
        if len(iters) > 1:
            mean_it = np.mean(iters)
            spread_it = max(iters) - min(iters)
            if mean_it > 0 and spread_it > mean_it: # Spread > 100% of the mean
                unstable_iters.append({
                    'id': f"{row['station']} {row['target']} {row['model']}",
                    'iters': [int(i) for i in iters],
                    'spread_pct': (spread_it / mean_it) * 100
                })
        
        # MAE Analysis
        maes = [row[c] for c in cv_mae_cols if c in row and pd.notna(row[c])]
        if len(maes) > 1:
            mean_mae = np.mean(maes)
            spread_mae = max(maes) - min(maes)
            if mean_mae > 0 and spread_mae > (0.4 * mean_mae): # Spread > 40% of the mean MAE
                unstable_maes.append({
                    'id': f"{row['station']} {row['target']} {row['model']}",
                    'maes': [round(m, 3) for m in maes],
                    'spread_pct': (spread_mae / mean_mae) * 100
                })

    if unstable_iters:
        print(f"WARNING: Found {len(unstable_iters)} models with severe Early-Stopping Instability (Iteration spread >100%):")
        for u in unstable_iters[:5]:
            print(f"  - {u['id']}: Iters {u['iters']} (Variance: {u['spread_pct']:.0f}%)")
        if len(unstable_iters) > 5:
            print(f"  ... and {len(unstable_iters) - 5} more.")
    else:
        print("Optimal Iteration counts are relatively stable across folds.")
        
    print()
    if unstable_maes:
        print(f"WARNING: Found {len(unstable_maes)} models with severe Performance Shift across folds (MAE spread >40%):")
        for u in unstable_maes[:5]:
            print(f"  - {u['id']}: MAEs {u['maes']} (Variance: {u['spread_pct']:.0f}%)")
        if len(unstable_maes) > 5:
            print(f"  ... and {len(unstable_maes) - 5} more.")
    else:
        print("Validation Performance is highly stable across folds.")

    print("\nDIAGNOSTIC SUMMARY:")
    if unstable_iters or unstable_maes:
        print("  -> The highly unstable iteration counts indicate that default TimeSeriesSplit chunks")
        print("     are capturing isolated specific seasons (e.g., Fold1=Winter, Fold2=Summer).")
        print("  -> Recommendation: Modify `dataset.py` or `tune.py` CV strategy to use smaller, more")
        print("     frequent walk-forward validations (e.g., n_splits=6, test_size=45) to ensure each")
        print("     hyperparameter combination is evaluated against diverse seasonal regimes.")
    else:
        print("  -> The models look healthy and consistent. Ready for production ensembling.")

if __name__ == "__main__":
    analyze_diagnostics()
