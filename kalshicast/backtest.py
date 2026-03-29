"""Walk-forward backtesting harness.

Simulates full pipeline on historical data with train/test splits.
Evaluates acceptance criteria: BSS, Brier, PnL.
"""

from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from typing import Any

from kalshicast.config.params_bootstrap import get_param_int, get_param_float

log = logging.getLogger(__name__)


def _date_range(start: date, end: date) -> list[date]:
    """Generate list of dates from start to end inclusive."""
    days = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(days)]


def get_historical_window(
    conn: Any,
    station_id: str,
    target_type: str,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """Fetch historical shadow book + observation pairs for backtesting."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sb.TARGET_DATE, sb.BIN_LOWER, sb.BIN_UPPER, sb.P_WIN,
                   o.OBSERVED_HIGH_F, o.OBSERVED_LOW_F
            FROM SHADOW_BOOK sb
            JOIN OBSERVATIONS o
              ON o.STATION_ID = sb.STATION_ID AND o.TARGET_DATE = sb.TARGET_DATE
            WHERE sb.STATION_ID = :sid
              AND sb.TARGET_TYPE = :tt
              AND sb.TARGET_DATE BETWEEN TO_DATE(:sd, 'YYYY-MM-DD')
                                      AND TO_DATE(:ed, 'YYYY-MM-DD')
            ORDER BY sb.TARGET_DATE, sb.BIN_LOWER
        """, {
            "sid": station_id, "tt": target_type,
            "sd": start_date.isoformat(), "ed": end_date.isoformat(),
        })
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, row)) for row in cur]


def compute_brier_for_day(bins: list[dict], observed: float) -> float:
    """Compute Brier score for a single day's shadow book vs observation."""
    brier_sum = 0.0
    for b in bins:
        p = b["p_win"]
        outcome = 1.0 if b["bin_lower"] <= observed < b["bin_upper"] else 0.0
        brier_sum += (p - outcome) ** 2
    return brier_sum / max(len(bins), 1)


def compute_bss(brier_scores: list[float], climatology: float = 0.25) -> float:
    """BSS = 1 - mean(Brier) / climatology."""
    if not brier_scores or climatology <= 0:
        return 0.0
    mean_brier = sum(brier_scores) / len(brier_scores)
    return 1.0 - mean_brier / climatology


def simulate_pnl(
    bins: list[dict],
    observed: float,
    bankroll: float,
    fee_rate: float = 0.07,
) -> dict:
    """Simulate PnL for a single day assuming Kelly-optimal bet on best bin."""
    best = max(bins, key=lambda b: b["p_win"])
    p_win = best["p_win"]
    c_market = p_win * 0.90  # assume 10% edge for backtest

    if p_win <= c_market:
        return {"pnl": 0.0, "bet": False}

    # Simple Kelly fraction
    edge = p_win - c_market
    f_star = edge / (1.0 - c_market) if c_market < 1.0 else 0.0
    f_star = min(f_star, 0.10)  # cap at 10%

    bet_size = f_star * bankroll
    won = best["bin_lower"] <= observed < best["bin_upper"]

    if won:
        gross = (1.0 - c_market) * bet_size / c_market
        pnl = gross * (1.0 - fee_rate)
    else:
        pnl = -bet_size

    return {"pnl": round(pnl, 2), "bet": True, "won": won, "f_star": f_star}


def walk_forward_backtest(
    conn: Any,
    station_id: str = "KJFK",
    target_type: str = "HIGH",
    train_days: int = 60,
    test_days: int = 30,
    n_folds: int = 3,
) -> dict:
    """Run walk-forward backtest with multiple folds.

    Each fold:
    - Train window: used for parameter calibration context
    - Test window: evaluated for Brier, BSS, PnL

    Returns aggregate metrics across all folds.
    """
    # Determine date range from DB
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MIN(TARGET_DATE), MAX(TARGET_DATE)
            FROM SHADOW_BOOK
            WHERE STATION_ID = :sid AND TARGET_TYPE = :tt
        """, {"sid": station_id, "tt": target_type})
        row = cur.fetchone()

    if not row or not row[0]:
        log.warning("[backtest] no data for %s/%s", station_id, target_type)
        return {"status": "NO_DATA", "folds": []}

    data_start = row[0].date() if hasattr(row[0], 'date') else row[0]
    data_end = row[1].date() if hasattr(row[1], 'date') else row[1]

    total_needed = n_folds * (train_days + test_days)
    available = (data_end - data_start).days
    if available < total_needed:
        actual_folds = max(1, available // (train_days + test_days))
        log.warning("[backtest] only %d days available, reducing to %d folds",
                    available, actual_folds)
        n_folds = actual_folds

    folds = []
    fold_start = data_start

    for fold_idx in range(n_folds):
        train_start = fold_start
        train_end = train_start + timedelta(days=train_days - 1)
        test_start = train_end + timedelta(days=1)
        test_end = test_start + timedelta(days=test_days - 1)

        if test_end > data_end:
            break

        # Fetch test window data
        rows = get_historical_window(conn, station_id, target_type, test_start, test_end)

        if not rows:
            fold_start = test_end + timedelta(days=1)
            continue

        # Group by target_date
        days: dict[str, list[dict]] = {}
        obs_by_day: dict[str, float] = {}
        for r in rows:
            td = str(r["target_date"])[:10]
            if td not in days:
                days[td] = []
            days[td].append(r)
            obs_col = "observed_high_f" if target_type == "HIGH" else "observed_low_f"
            if r.get(obs_col) is not None:
                obs_by_day[td] = float(r[obs_col])

        brier_scores = []
        pnl_total = 0.0
        bankroll = 1000.0
        wins = 0
        bets = 0

        for td, bins in days.items():
            obs = obs_by_day.get(td)
            if obs is None:
                continue

            bs = compute_brier_for_day(bins, obs)
            brier_scores.append(bs)

            sim = simulate_pnl(bins, obs, bankroll)
            if sim["bet"]:
                pnl_total += sim["pnl"]
                bankroll += sim["pnl"]
                bets += 1
                if sim.get("won"):
                    wins += 1

        bss = compute_bss(brier_scores)
        mean_brier = sum(brier_scores) / max(len(brier_scores), 1)

        fold_result = {
            "fold": fold_idx + 1,
            "train_start": train_start.isoformat(),
            "test_start": test_start.isoformat(),
            "test_end": test_end.isoformat(),
            "n_days": len(brier_scores),
            "mean_brier": round(mean_brier, 6),
            "bss": round(bss, 4),
            "pnl": round(pnl_total, 2),
            "bets": bets,
            "win_rate": round(wins / max(bets, 1), 3),
            "final_bankroll": round(bankroll, 2),
        }
        folds.append(fold_result)
        log.info("[backtest] fold %d: BSS=%.4f Brier=%.4f PnL=%.2f (%d bets)",
                 fold_idx + 1, bss, mean_brier, pnl_total, bets)

        fold_start = test_end + timedelta(days=1)

    # Aggregate
    if folds:
        avg_bss = sum(f["bss"] for f in folds) / len(folds)
        avg_brier = sum(f["mean_brier"] for f in folds) / len(folds)
        total_pnl = sum(f["pnl"] for f in folds)
        total_bets = sum(f["bets"] for f in folds)
    else:
        avg_bss = avg_brier = total_pnl = 0.0
        total_bets = 0

    # Acceptance criteria
    bss_pass = avg_bss >= 0.05
    brier_pass = avg_brier <= 0.22
    pnl_pass = total_pnl >= 0

    result = {
        "status": "PASS" if (bss_pass and brier_pass and pnl_pass) else "FAIL",
        "n_folds": len(folds),
        "avg_bss": round(avg_bss, 4),
        "avg_brier": round(avg_brier, 6),
        "total_pnl": round(total_pnl, 2),
        "total_bets": total_bets,
        "acceptance": {
            "bss_pass": bss_pass,
            "brier_pass": brier_pass,
            "pnl_pass": pnl_pass,
        },
        "folds": folds,
    }

    log.info("[backtest] %s — BSS=%.4f Brier=%.4f PnL=%.2f (%d folds)",
             result["status"], avg_bss, avg_brier, total_pnl, len(folds))

    return result


def main() -> None:
    """CLI entry for backtest."""
    import json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    from kalshicast.db.connection import init_db, get_conn, close_pool
    from kalshicast.config import get_stations

    init_db()
    conn = get_conn()
    try:
        stations = get_stations(active_only=True)
        all_results = {}

        for st in stations[:5]:  # limit to first 5 for speed
            for tt in ("HIGH", "LOW"):
                key = f"{st['station_id']}_{tt}"
                result = walk_forward_backtest(conn, st["station_id"], tt)
                all_results[key] = result
                print(f"  {key}: {result['status']} BSS={result['avg_bss']:.4f}")

        print("\n" + json.dumps(all_results, indent=2, default=str))
    finally:
        conn.close()
        close_pool()
