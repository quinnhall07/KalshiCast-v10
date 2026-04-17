"""Paper simulation — convert BEST_BETS into simulated POSITIONS for backtesting.

Flow:
  market_open (step 7.5):  create_paper_positions() → PAPER_OPEN rows in POSITIONS
  night pipeline (step 3b): settle_paper_positions()  → PAPER_SETTLED + PnL
  dashboard:                get_paper_equity()        → equity curve for the UI

Paper positions use IS_PAPER = 1 and STATUS 'PAPER_OPEN' / 'PAPER_SETTLED' so they
are completely isolated from live execution while sharing the same grading pipeline
(BRIER_SCORES, FORECAST_ERRORS, BSS_MATRIX all work unchanged).

Paper bankroll is fixed at $1,000 for sizing; fees are simulated at 7% of |gross|.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

# Paper mode starting bankroll for contract sizing
PAPER_BANKROLL = 1_000.0
FEE_RATE       = 0.07      # 7 % of |gross pnl|, matching live rollover.py


# ─────────────────────────────────────────────────────────────────────
# Step 7.5 — called from market_open.py after price_shadow_book
# ─────────────────────────────────────────────────────────────────────

def create_paper_positions(conn: Any, pipeline_run_id: str) -> int:
    """Convert IS_SELECTED_FOR_EXECUTION BEST_BETS into PAPER_OPEN POSITIONS.

    Skips any ticker that already has a paper position (safe to re-run).
    Contract count = floor(f_final × PAPER_BANKROLL / contract_price), min 1.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT bb.TICKER, bb.STATION_ID, bb.TARGET_DATE, bb.TARGET_TYPE,
                   bb.BIN_LOWER, bb.BIN_UPPER,
                   bb.CONTRACT_PRICE, bb.F_FINAL, bb.ORDER_TYPE
            FROM BEST_BETS bb
            WHERE bb.PIPELINE_RUN_ID    = :run_id
              AND bb.IS_SELECTED_FOR_EXECUTION = 1
              AND bb.CONTRACT_PRICE     IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM POSITIONS p
                  WHERE p.TICKER   = bb.TICKER
                    AND p.IS_PAPER = 1
              )
        """, {"run_id": pipeline_run_id})
        candidates = cur.fetchall()

    if not candidates:
        log.info("[paper_sim] no new positions to create for run %s", pipeline_run_id[:8])
        return 0

    count = 0
    skipped_no_price = 0
    for row in candidates:
        ticker, sid, td, tt, bl, bu, price, f_final, order_type = row

        # Skip rather than invent a price. An invented 28¢ contract feeds
        # a fake entry cost into PnL and corrupts the BSS grading pipeline.
        if price is None:
            skipped_no_price += 1
            log.warning(
                "[paper_sim] %s has no contract_price; skipping paper position",
                ticker,
            )
            continue

        price_f  = float(price)
        ffinal_f = float(f_final or 0.01)
        contracts = max(1, int(ffinal_f * PAPER_BANKROLL / max(price_f, 0.01)))

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO POSITIONS (
                    POSITION_ID, TICKER, STATION_ID, TARGET_DATE, TARGET_TYPE,
                    BIN_LOWER, BIN_UPPER, ENTRY_PRICE, CONTRACTS,
                    ORDER_TYPE, STATUS, SUBMITTED_AT, IS_PAPER
                ) VALUES (
                    :pos_id, :ticker, :sid,
                    TO_DATE(:td, 'YYYY-MM-DD'), :tt,
                    :bl, :bu, :price, :contracts,
                    :otype, 'PAPER_OPEN', SYSTIMESTAMP, 1
                )
            """, {
                "pos_id":    str(uuid.uuid4()),
                "ticker":    ticker,
                "sid":       sid,
                "td":        str(td)[:10] if td else None,
                "tt":        tt,
                "bl":        float(bl) if bl is not None else None,
                "bu":        float(bu) if bu is not None else None,
                "price":     price_f,
                "contracts": contracts,
                "otype":     order_type or "MAKER",
            })
        count += 1

    conn.commit()
    log.info(
        "[paper_sim] created %d paper positions from run %s%s",
        count, pipeline_run_id[:8],
        f" (skipped {skipped_no_price} with no contract_price)" if skipped_no_price else "",
    )
    return count


# ─────────────────────────────────────────────────────────────────────
# Called from rollover.py (morning) and night.py (after observations)
# ─────────────────────────────────────────────────────────────────────

def settle_paper_positions(conn: Any) -> int:
    """Settle PAPER_OPEN positions whose TARGET_DATE has passed and observations exist.

    Outcome logic and PnL formula are identical to rollover.settle_positions()
    so grading is apples-to-apples with what live trading would produce.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE POSITIONS p SET
                STATUS    = 'PAPER_SETTLED',
                FILLED_AT = SYSTIMESTAMP,
                OUTCOME   = CASE
                    WHEN p.TARGET_TYPE = 'HIGH'
                     AND (SELECT o.OBSERVED_HIGH_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID  = p.STATION_ID
                            AND o.TARGET_DATE = p.TARGET_DATE)
                         BETWEEN p.BIN_LOWER AND p.BIN_UPPER - 0.001
                    THEN 1
                    WHEN p.TARGET_TYPE = 'LOW'
                     AND (SELECT o.OBSERVED_LOW_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID  = p.STATION_ID
                            AND o.TARGET_DATE = p.TARGET_DATE)
                         BETWEEN p.BIN_LOWER AND p.BIN_UPPER - 0.001
                    THEN 1
                    ELSE 0
                END,
                PNL_GROSS = CASE
                    -- WIN: collect (1 - entry) × contracts × $1 face
                    WHEN p.TARGET_TYPE = 'HIGH'
                     AND (SELECT o.OBSERVED_HIGH_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID  = p.STATION_ID
                            AND o.TARGET_DATE = p.TARGET_DATE)
                         BETWEEN p.BIN_LOWER AND p.BIN_UPPER - 0.001
                    THEN (1.0 - p.ENTRY_PRICE) * p.CONTRACTS * 100
                    WHEN p.TARGET_TYPE = 'LOW'
                     AND (SELECT o.OBSERVED_LOW_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID  = p.STATION_ID
                            AND o.TARGET_DATE = p.TARGET_DATE)
                         BETWEEN p.BIN_LOWER AND p.BIN_UPPER - 0.001
                    THEN (1.0 - p.ENTRY_PRICE) * p.CONTRACTS * 100
                    -- LOSS: forfeit entry cost
                    ELSE -p.ENTRY_PRICE * p.CONTRACTS * 100
                END
            WHERE p.STATUS      = 'PAPER_OPEN'
              AND p.IS_PAPER    = 1
              AND p.TARGET_DATE < TRUNC(SYSDATE)
              AND EXISTS (
                  SELECT 1 FROM OBSERVATIONS o
                  WHERE o.STATION_ID  = p.STATION_ID
                    AND o.TARGET_DATE = p.TARGET_DATE
              )
        """)
        count = cur.rowcount or 0

    if count > 0:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE POSITIONS
                SET PNL_NET = PNL_GROSS - ABS(PNL_GROSS) * :fee
                WHERE STATUS   = 'PAPER_SETTLED'
                  AND IS_PAPER = 1
                  AND PNL_NET  IS NULL
            """, {"fee": FEE_RATE})

    conn.commit()
    log.info("[paper_sim] settled %d paper positions", count)
    return count


# ─────────────────────────────────────────────────────────────────────
# Stats for /api/system and /api/paper-equity
# ─────────────────────────────────────────────────────────────────────

def get_paper_stats(conn: Any) -> dict:
    """Aggregate paper trading stats for the dashboard system overlay."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*)                                                AS n_total,
                SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END)           AS n_won,
                SUM(CASE WHEN outcome = 0 THEN 1 ELSE 0 END)           AS n_lost,
                SUM(COALESCE(pnl_net, 0))                              AS cumulative_pnl,
                SUM(CASE
                      WHEN TRUNC(filled_at) = TRUNC(SYSDATE)
                      THEN COALESCE(pnl_net, 0) ELSE 0
                    END)                                                AS daily_pnl,
                COUNT(CASE WHEN status = 'PAPER_OPEN' THEN 1 END)      AS n_open
            FROM POSITIONS
            WHERE IS_PAPER = 1
              AND status IN ('PAPER_OPEN', 'PAPER_SETTLED')
        """)
        row = cur.fetchone()

    if not row or not row[0]:
        return {
            "n_bets_total": 0, "n_bets_won": 0, "n_bets_lost": 0,
            "cumulative_pnl": 0.0, "daily_pnl": 0.0, "n_open": 0,
        }

    n_total = int(row[0])
    return {
        "n_bets_total":   n_total,
        "n_bets_won":     int(row[1]) if row[1] else 0,
        "n_bets_lost":    int(row[2]) if row[2] else 0,
        "cumulative_pnl": float(row[3]) if row[3] else 0.0,
        "daily_pnl":      float(row[4]) if row[4] else 0.0,
        "n_open":         int(row[5]) if row[5] else 0,
    }


def get_paper_equity_curve(conn: Any, window_days: int = 90) -> list[dict]:
    """Daily equity curve for the paper portfolio.

    Returns [{date, daily_pnl, cumulative_pnl, n_bets, win_rate}] sorted ascending.
    Used by /api/paper-equity for the dashboard chart.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                TRUNC(filled_at)                                         AS day,
                SUM(COALESCE(pnl_net, 0))                               AS daily_pnl,
                COUNT(*)                                                 AS n_bets,
                SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END)            AS n_won
            FROM POSITIONS
            WHERE IS_PAPER   = 1
              AND status     = 'PAPER_SETTLED'
              AND filled_at >= TRUNC(SYSDATE) - :window
            GROUP BY TRUNC(filled_at)
            ORDER BY TRUNC(filled_at) ASC
        """, {"window": window_days})
        rows = cur.fetchall()

    result = []
    running = 0.0
    for row in rows:
        day, dpnl, n, nw = row
        dpnl_f = float(dpnl) if dpnl else 0.0
        running += dpnl_f
        result.append({
            "date":            day.strftime("%Y-%m-%d") if hasattr(day, "strftime") else str(day)[:10],
            "daily_pnl":       round(dpnl_f, 4),
            "cumulative_pnl":  round(running, 4),
            "n_bets":          int(n) if n else 0,
            "win_rate":        round(int(nw) / int(n), 4) if n and int(n) > 0 else 0.0,
        })
    return result
