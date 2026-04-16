"""Market-open pipeline — paper mode + live mode (full L4 execution).

Spec §9.3: Runs at 14:00 UTC. Computes ensemble state, prices Shadow Book,
evaluates conviction gates + IBE, computes Kelly sizing, submits orders.

Paper mode: Steps 1-7 only (ensemble + pricing).
Live mode:  All 12 steps including API calls and order submission.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone, timedelta
from typing import Any

from kalshicast.config.params_bootstrap import get_param_int, get_param_float, get_param_bool
from kalshicast.db.connection import get_conn, close_pool
from kalshicast.db.operations import (
    update_pipeline_run, upsert_best_bets, insert_orderbook_snapshot,
    insert_ibe_signal_log, get_previous_shadow_book, insert_system_alert,
)
from kalshicast.pipeline import pipeline_init, RUN_MARKET_OPEN, STATUS_OK, STATUS_PARTIAL, STATUS_ERROR

log = logging.getLogger(__name__)


def main() -> None:
    """Market-open pipeline — paper or live mode."""
    live_mode = "--live" in sys.argv

    pipeline_run_id, _ = pipeline_init(RUN_MARKET_OPEN)

    # Target dates: today + next FORECAST_DAYS
    now_utc = datetime.now(timezone.utc)
    forecast_days = get_param_int("pipeline.forecast_days")
    target_dates = [(now_utc.date() + timedelta(days=d)).isoformat()
                    for d in range(forecast_days)]

    mode_str = "LIVE" if live_mode else "PAPER"
    log.info("Market-open pipeline (%s MODE) run_id=%s, dates=%s",
             mode_str, pipeline_run_id[:8], target_dates)

    status = STATUS_OK
    error_msg = None
    total_ensemble = 0
    total_shadow = 0
    total_bets = 0
    total_orders = 0

    conn = get_conn()
    try:
        # ─────────────────────────────────────────────────────────────
        # Step 0: Sync Kalshi markets (get real tickers)
        # ─────────────────────────────────────────────────────────────
        log.info("Step 0: Syncing Kalshi markets")
        try:
            from kalshicast.collection.kalshi_markets import sync_kalshi_markets
            from kalshicast.execution.kalshi_api import KalshiClient

            client = KalshiClient()
            sync_result = sync_kalshi_markets(conn, client)
            log.info("Step 0 OK: %d markets synced, %d unmatched, %d ignored",
                     sync_result.synced, sync_result.unmatched, sync_result.ignored)
        except Exception as e:
            log.error("Step 0 ERROR: Market sync failed: %s", e)
            # Continue anyway - we may have cached data from previous run

        # Step 3: fetch_bankroll and init client
        bankroll = 1000.0  # Paper mode default
        client = None
        try:
            from kalshicast.execution.kalshi_api import KalshiClient
            client = KalshiClient()
            if live_mode:
                bankroll = client.get_balance()
                log.info("Step 3: live bankroll=$%.2f", bankroll)
            else:
                log.info("Step 3: paper mode bankroll=$%.2f (Connected to API for pricing)", bankroll)
        except Exception as e:
            log.error("Step 3 ERROR: API connection failed: %s", e)
            status = STATUS_PARTIAL
        else:
            log.info("Step 3: paper mode bankroll=$%.2f", bankroll)

        # Step 4: fetch_metar
        if live_mode:
            try:
                from kalshicast.collection.collectors.collect_metar import fetch_metar_observations
                from kalshicast.config import get_stations
                stations = get_stations(active_only=True)
                n_metar = fetch_metar_observations(stations, conn)
                log.info("Step 4: %d METAR observations fetched", n_metar)
            except Exception as e:
                log.warning("Step 4 WARN: METAR fetch failed: %s", e)
        else:
            log.info("Step 4: METAR fetch skipped (paper mode)")

        # Step 5: fetch_afd
        if live_mode:
            try:
                from kalshicast.collection.collectors.collect_afd import fetch_afd_discussions
                # Get unique WFO IDs
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT WFO_ID FROM STATIONS WHERE IS_ACTIVE = 1 AND WFO_ID IS NOT NULL")
                    wfo_ids = [row[0] for row in cur]
                n_afd = fetch_afd_discussions(wfo_ids, conn)
                log.info("Step 5: %d AFD discussions fetched", n_afd)
            except Exception as e:
                log.warning("Step 5 WARN: AFD fetch failed: %s", e)
        else:
            log.info("Step 5: AFD fetch skipped (paper mode)")

        # Step 6: compute_ensemble_state
        try:
            from kalshicast.processing.ensemble import compute_ensemble_state
            for td in target_dates:
                n = compute_ensemble_state(conn, td, pipeline_run_id)
                total_ensemble += n
            log.info("Step 6 OK: %d ensemble states across %d dates",
                     total_ensemble, len(target_dates))
        except Exception as e:
            log.error("Step 6 ERROR: ensemble computation failed: %s", e, exc_info=True)
            status = STATUS_PARTIAL
            insert_system_alert(conn, {
                "alert_type": "ENSEMBLE_COMPUTATION_FAILED",
                "severity_score": 0.85,
                "details": {"error": str(e)[:300], "pipeline_run_id": pipeline_run_id},
            })
            conn.commit()

        if total_ensemble == 0:
            insert_system_alert(conn, {
                "alert_type": "ENSEMBLE_NO_DATA",
                "severity_score": 0.8,
                "details": {
                    "error": "Ensemble computation produced zero states — no forecast data available.",
                    "target_dates": target_dates,
                },
            })
            conn.commit()

        # Step 7: price_shadow_book
        try:
            from kalshicast.pricing.shadow_book import price_shadow_book
            for td in target_dates:
                n = price_shadow_book(conn, td, pipeline_run_id)
                total_shadow += n
            log.info("Step 7 OK: %d shadow book rows across %d dates",
                     total_shadow, len(target_dates))
        except Exception as e:
            log.error("Step 7 ERROR: shadow book pricing failed: %s", e)
            status = STATUS_PARTIAL
            insert_system_alert(conn, {
                "alert_type": "SHADOW_BOOK_PRICING_FAILED",
                "severity_score": 0.8,
                "details": {"error": str(e)[:300], "pipeline_run_id": pipeline_run_id},
            })
            conn.commit()
        
        # Step 8: fetch_market_prices (Required for accurate paper trades AND live trades)
        if client is not None:
            try:
                total_snapshots = _step8_fetch_market_prices(conn, client, pipeline_run_id)
                log.info("Step 8 OK: %d orderbook snapshots", total_snapshots)
            except Exception as e:
                log.error("Step 8 ERROR: market price fetch failed: %s", e)
                status = STATUS_PARTIAL

        # Step 7.5: create_paper_positions (paper mode only)
        if not live_mode:
            try:
                from kalshicast.execution.gates import evaluate_all_gates
                from kalshicast.execution.kelly import smirnov_kelly, full_sizing_chain
                from kalshicast.db.operations import upsert_best_bets

                best_bets_paper = _step9_evaluate_gates_ibe(
                    conn, pipeline_run_id,
                    bankroll=1000.0,
                    target_dates=target_dates,
                    paper_mode=True,
                )

                # Baseline fallback: if gates rejected everything, place
                # one bet per (station, date, type) on the highest-p_win bin
                n_selected = sum(
                    1 for b in best_bets_paper
                    if b.get("is_selected_for_execution")
                )
                if n_selected == 0:
                    log.info("[baseline] No gate-passing bets; applying baseline fallback")
                    baseline_bets = _create_baseline_bets(
                        conn, pipeline_run_id, target_dates,
                    )
                    if baseline_bets:
                        upsert_best_bets(conn, baseline_bets)
                        conn.commit()
                        log.info("[baseline] Created %d baseline bets", len(baseline_bets))

                from kalshicast.pipeline.paper_sim import create_paper_positions
                n_paper = create_paper_positions(conn, pipeline_run_id)
                log.info("Step 7.5 OK: %d paper positions created", n_paper)

                if n_paper == 0:
                    edges_for_alert = [
                        b["p_win"] - b.get("contract_price", b.get("c_vwap", 0))
                        for b in best_bets_paper
                        if b.get("c_vwap") and b["c_vwap"] > 0
                    ]
                    gate_pass_count = sum(
                        1 for b in best_bets_paper
                        if all(b.get("gate_flags", {}).get(g, False)
                               for g in ("edge", "spread", "skill", "lead", "reserved"))
                    )
                    kelly_pass_count = sum(
                        1 for b in best_bets_paper if (b.get("f_star") or 0) > 0
                    )

                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT
                                COUNT(*) AS total_snapshots,
                                SUM(CASE WHEN C_VWAP_COMPUTED > 0
                                     AND AVAILABLE_DEPTH > 0 THEN 1 ELSE 0 END) AS with_depth
                            FROM MARKET_ORDERBOOK_SNAPSHOTS mos
                            WHERE EXISTS (
                                SELECT 1 FROM SHADOW_BOOK sb
                                WHERE sb.TICKER = mos.TICKER
                                  AND sb.PIPELINE_RUN_ID = :run_id
                            )
                        """, {"run_id": pipeline_run_id})
                        snap_row = cur.fetchone()
                        total_snaps = int(snap_row[0]) if snap_row and snap_row[0] else 0
                        snaps_with_depth = int(snap_row[1]) if snap_row and snap_row[1] else 0

                    insert_system_alert(conn, {
                        "alert_type": "PAPER_NO_POSITIONS",
                        "severity_score": 0.4,
                        "details": {
                            "pipeline_run_id": pipeline_run_id,
                            "best_bets_evaluated": len(best_bets_paper),
                            "orderbook_snapshots_total": total_snaps,
                            "orderbook_snapshots_with_depth": snaps_with_depth,
                            "candidates_all_gates_pass": gate_pass_count,
                            "candidates_kelly_positive": kelly_pass_count,
                            "avg_edge": round(
                                sum(edges_for_alert) / len(edges_for_alert), 4
                            ) if edges_for_alert else None,
                            "max_edge": round(max(edges_for_alert), 4)
                                if edges_for_alert else None,
                        },
                    })

            except Exception as e:
                log.warning("Step 7.5 WARN: paper position creation failed: %s", e)
        
        # Steps 8-10: Live execution (gates, IBE, Kelly, orders)
        if live_mode and client is not None:
            
            from kalshicast.execution.risk_manager import evaluate_system_health
            is_offline = evaluate_system_health(conn, bankroll)
            is_halted = get_param_bool("system.trading_halted", default=False)
            
            if is_halted:
                log.warning("EXECUTION ABORTED: System is manually HALTED.")
                status = "HALTED"
            elif is_offline:
                from kalshicast.config.params_bootstrap import get_param_str
                offline_reason = get_param_str("system.offline_reason", default="Unknown Risk")
                log.warning("EXECUTION SKIPPED: System is OFFLINE. Reason: %s", offline_reason)
                status = "OFFLINE"
            else:
                try:
                    best_bets = _step9_evaluate_gates_ibe(
                        conn, pipeline_run_id, bankroll, target_dates)
                    total_bets = len(best_bets)
                    log.info("Step 9 OK: %d best bets evaluated", total_bets)
                except Exception as e:
                    log.error("Step 9 ERROR: gate/IBE evaluation failed: %s", e)
                    best_bets = []
                    status = STATUS_PARTIAL

                try:
                    from kalshicast.execution.orders import execute_best_bets
                    summary = execute_best_bets(client, conn, best_bets)
                    total_orders = summary.get("submitted", 0)
                    log.info("Step 10 OK: %d orders submitted, %d filled, %d skipped, %d errors",
                             summary.get("submitted", 0), summary.get("filled", 0),
                             summary.get("skipped", 0), summary.get("errors", 0))
                    if summary.get("errors", 0) > 0:
                        insert_system_alert(conn, {
                            "alert_type": "ORDER_SUBMISSION_ERRORS",
                            "severity_score": 0.7,
                            "details": {
                                "submitted": summary.get("submitted", 0),
                                "filled": summary.get("filled", 0),
                                "errors": summary.get("errors", 0),
                                "skipped": summary.get("skipped", 0),
                            },
                        })
                        conn.commit()
                except Exception as e:
                    log.error("Step 10 ERROR: order submission failed: %s", e)
                    status = STATUS_PARTIAL
                    insert_system_alert(conn, {
                        "alert_type": "ORDER_SUBMISSION_CRASHED",
                        "severity_score": 0.9,
                        "details": {"error": str(e)[:300], "pipeline_run_id": pipeline_run_id},
                    })
                    conn.commit()
        else:
            log.info("Steps 8-10: SKIPPED (%s mode — no market fetch, gates, or orders)", mode_str)

        # Step 11: update pipeline_day_health
        try:
            _step11_update_health(conn, target_dates, total_ensemble, total_shadow, total_bets)
            log.info("Step 11 OK: pipeline_day_health updated")
        except Exception as e:
            log.warning("Step 11 WARN: health update failed: %s", e)

        if status != STATUS_OK:
            insert_system_alert(conn, {
                "alert_type": f"PIPELINE_MARKET_OPEN_{status}",
                "severity_score": 0.8 if status == STATUS_ERROR else 0.6,
                "details": {
                    "pipeline_run_id": pipeline_run_id,
                    "status": status,
                    "mode": mode_str,
                    "shadow_rows": total_shadow,
                    "bets_placed": total_bets,
                },
            })

        update_pipeline_run(
            conn, pipeline_run_id,
            status=status,
            rows_daily=total_shadow,
        )
        conn.commit()

    except Exception as e:
        log.exception("Market-open pipeline failed: %s", e)
        status = STATUS_ERROR
        error_msg = str(e)[:2000]
        try:
            insert_system_alert(conn, {
                "alert_type": "PIPELINE_MARKET_OPEN_CRASH",
                "severity_score": 0.95,
                "details": {"error": str(e)[:500], "mode": mode_str},
            })
            update_pipeline_run(conn, pipeline_run_id, status=status,
                                error_msg=error_msg)
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()
        close_pool()

    log.info("DONE — market_open (%s): ensemble=%d shadow_book=%d bets=%d orders=%d status=%s",
             mode_str, total_ensemble, total_shadow, total_bets, total_orders, status)


# ─────────────────────────────────────────────────────────────────────
# Step helpers
# ─────────────────────────────────────────────────────────────────────

def _step8_fetch_market_prices(conn, client, run_id: str) -> int:
    """Fetch order books for active Shadow Book tickers."""
    import time as _time
    from kalshicast.execution.vwap import compute_vwap

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT TICKER FROM SHADOW_BOOK
            WHERE PIPELINE_RUN_ID = :run_id AND P_WIN IS NOT NULL
        """, {"run_id": run_id})
        all_tickers = [row[0] for row in cur]

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT mos.TICKER
            FROM (
                SELECT TICKER, AVAILABLE_DEPTH,
                       ROW_NUMBER() OVER (PARTITION BY TICKER ORDER BY SNAPSHOT_UTC DESC) rn
                FROM MARKET_ORDERBOOK_SNAPSHOTS
            ) mos
            WHERE mos.rn = 1
              AND (mos.AVAILABLE_DEPTH IS NULL OR mos.AVAILABLE_DEPTH = 0)
        """)
        previously_empty = {row[0] for row in cur}

    tickers_to_fetch = [t for t in all_tickers if t not in previously_empty]
    n_skipped = len(all_tickers) - len(tickers_to_fetch)

    if n_skipped > 0:
        log.info("[step8] skipping %d tickers with previously-empty books "
                 "(fetching %d of %d)", n_skipped, len(tickers_to_fetch), len(all_tickers))

    count = 0
    n_with_depth = 0
    n_empty = 0
    t_start = _time.monotonic()

    for ticker in tickers_to_fetch:
        try:
            orderbook = client.get_orderbook(ticker)
            c_vwap, depth = compute_vwap(orderbook, 10)

            insert_orderbook_snapshot(conn, {
                "ticker": ticker,
                "yes_book": orderbook.get("yes", []),
                "no_book": orderbook.get("no", []),
                "c_vwap": c_vwap,
                "available_depth": depth,
            })
            count += 1
            if depth > 0 and c_vwap > 0:
                n_with_depth += 1
            else:
                n_empty += 1
        except Exception as e:
            log.warning("Orderbook fetch failed for %s: %s", ticker, e)

    elapsed = _time.monotonic() - t_start
    log.info("[step8] fetched %d orderbooks in %.1fs: %d with depth, %d empty, "
             "%d skipped (previously empty)",
             count, elapsed, n_with_depth, n_empty, n_skipped)

    conn.commit()
    return count


def _step9_evaluate_gates_ibe(
    conn,
    pipeline_run_id: str,
    bankroll: float,
    target_dates: list,
    paper_mode: bool = False,
) -> list:
    """Evaluate conviction gates, IBE signals, Kelly sizing for all candidates.

    Changes from original:
    - Dynamic lead_hours computed per candidate from station timezone (was hardcoded 24.0)
    - BSS bracket lookup uses actual lead_bracket (was hardcoded "h3")
    - Top-5 candidate diagnostics in funnel logging
    """
    from kalshicast.execution.gates import evaluate_all_gates
    from kalshicast.execution.ibe import evaluate_ibe
    from kalshicast.execution.kelly import smirnov_kelly, full_sizing_chain
    from kalshicast.execution.positions import get_remaining_capacity
    from kalshicast.collection.lead_time import compute_lead_hours, classify_lead_hours

    # Get Shadow Book candidates with market prices
    with conn.cursor() as cur:
        if paper_mode:
            cur.execute("""
                SELECT sb.TICKER, sb.STATION_ID, sb.TARGET_DATE, sb.TARGET_TYPE,
                       sb.BIN_LOWER, sb.BIN_UPPER, sb.P_WIN, sb.MU, sb.SIGMA_EFF,
                       sb.TOP_MODEL_ID,
                       COALESCE(km.YES_ASK / 100.0, km.LAST_PRICE / 100.0,
                                km.YES_BID / 100.0, 0.50)
                           AS C_VWAP_COMPUTED,
                       100 AS AVAILABLE_DEPTH
                FROM SHADOW_BOOK sb
                LEFT JOIN KALSHI_MARKETS km ON km.TICKER = sb.TICKER
                WHERE sb.PIPELINE_RUN_ID = :run_id
                  AND sb.P_WIN IS NOT NULL
            """, {"run_id": pipeline_run_id})
        else:
            cur.execute("""
                SELECT sb.TICKER, sb.STATION_ID, sb.TARGET_DATE, sb.TARGET_TYPE,
                       sb.BIN_LOWER, sb.BIN_UPPER, sb.P_WIN, sb.MU, sb.SIGMA_EFF,
                       sb.TOP_MODEL_ID,
                       mos.C_VWAP_COMPUTED, mos.AVAILABLE_DEPTH
                FROM SHADOW_BOOK sb
                LEFT JOIN (
                    SELECT TICKER, C_VWAP_COMPUTED, AVAILABLE_DEPTH,
                           ROW_NUMBER() OVER (PARTITION BY TICKER ORDER BY SNAPSHOT_UTC DESC) rn
                    FROM MARKET_ORDERBOOK_SNAPSHOTS
                ) mos ON mos.TICKER = sb.TICKER AND mos.rn = 1
                WHERE sb.PIPELINE_RUN_ID = :run_id
                  AND sb.P_WIN IS NOT NULL
                  AND mos.C_VWAP_COMPUTED IS NOT NULL
                  AND mos.C_VWAP_COMPUTED > 0
                  AND mos.AVAILABLE_DEPTH > 0
            """, {"run_id": pipeline_run_id})

        candidates = []
        for row in cur:
            candidates.append({
                "ticker": row[0], "station_id": row[1],
                "target_date": str(row[2])[:10] if row[2] else None,
                "target_type": row[3],
                "bin_lower": float(row[4]) if row[4] else None,
                "bin_upper": float(row[5]) if row[5] else None,
                "p_win": float(row[6]) if row[6] else 0.0,
                "mu": float(row[7]) if row[7] else None,
                "sigma_eff": float(row[8]) if row[8] else None,
                "top_model_id": row[9],
                "c_market": float(row[10]) if row[10] else 0.0,
                "available_depth": int(row[11]) if row[11] else 0,
            })

    if not candidates:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM SHADOW_BOOK
                WHERE PIPELINE_RUN_ID = :run_id AND P_WIN IS NOT NULL
            """, {"run_id": pipeline_run_id})
            n_shadow_rows = cur.fetchone()[0] or 0

        if paper_mode:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*),
                           SUM(CASE WHEN COALESCE(YES_ASK, LAST_PRICE, YES_BID)
                                         IS NOT NULL
                                THEN 1 ELSE 0 END)
                    FROM KALSHI_MARKETS km
                    WHERE EXISTS (
                        SELECT 1 FROM SHADOW_BOOK sb
                        WHERE sb.TICKER = km.TICKER
                          AND sb.PIPELINE_RUN_ID = :run_id
                    )
                """, {"run_id": pipeline_run_id})
                km_row = cur.fetchone()
                n_km = int(km_row[0]) if km_row and km_row[0] else 0
                n_priced = int(km_row[1]) if km_row and km_row[1] else 0
            log.warning(
                "[gates] paper_mode: zero candidates. "
                "shadow_book_rows=%d kalshi_markets=%d "
                "(with_prices=%d without=%d)",
                n_shadow_rows, n_km, n_priced, n_km - n_priced,
            )
        else:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN C_VWAP_COMPUTED > 0 AND AVAILABLE_DEPTH > 0
                             THEN 1 ELSE 0 END) AS with_depth,
                        SUM(CASE WHEN C_VWAP_COMPUTED = 0 OR AVAILABLE_DEPTH = 0
                             THEN 1 ELSE 0 END) AS empty
                    FROM MARKET_ORDERBOOK_SNAPSHOTS mos
                    WHERE EXISTS (
                        SELECT 1 FROM SHADOW_BOOK sb
                        WHERE sb.TICKER = mos.TICKER
                          AND sb.PIPELINE_RUN_ID = :run_id
                    )
                """, {"run_id": pipeline_run_id})
                snap_row = cur.fetchone()
                n_snaps = int(snap_row[0]) if snap_row and snap_row[0] else 0
                n_with_depth = int(snap_row[1]) if snap_row and snap_row[1] else 0
                n_empty = int(snap_row[2]) if snap_row and snap_row[2] else 0

            log.warning(
                "[gates] zero candidates with real market depth. "
                "shadow_book_rows=%d orderbook_snapshots=%d "
                "(with_depth=%d empty=%d). "
                "All markets are either illiquid or not fetched.",
                n_shadow_rows, n_snaps, n_with_depth, n_empty,
            )
        return []

    # Get ensemble state for spread info
    ensemble_cache = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT STATION_ID, TARGET_DATE, TARGET_TYPE, S_TK, F_BAR_TK, SIGMA_EFF
            FROM ENSEMBLE_STATE
            WHERE RUN_ID = :run_id
        """, {"run_id": pipeline_run_id})
        for row in cur:
            key = f"{row[0]}|{str(row[1])[:10]}|{row[2]}"
            ensemble_cache[key] = {
                "s_tk": float(row[3]) if row[3] else 3.0,
                "f_bar": float(row[4]) if row[4] else 0.0,
                "sigma_eff": float(row[5]) if row[5] else 3.0,
            }

    # Get BSS info (all brackets)
    bss_cache = {}
    with conn.cursor() as cur:
        cur.execute("SELECT STATION_ID, TARGET_TYPE, LEAD_BRACKET, BSS_1, IS_QUALIFIED FROM BSS_MATRIX")
        for row in cur:
            key = f"{row[0]}|{row[1]}|{row[2]}"
            bss_cache[key] = {
                "bss": float(row[3]) if row[3] else None,
                "qualified": bool(row[4]),
            }

    # Count existing bets for adaptive edge buffer
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM POSITIONS WHERE STATUS IN ('OPEN', 'FILLED')")
        n_bets = cur.fetchone()[0] or 0

    # Get station WFO mapping and timezone
    wfo_map = {}
    tz_map = {}
    with conn.cursor() as cur:
        cur.execute("SELECT STATION_ID, WFO_ID, TIMEZONE FROM STATIONS WHERE IS_ACTIVE = 1")
        for row in cur:
            wfo_map[row[0]] = row[1] or "UNK"
            tz_map[row[0]] = row[2] or "America/New_York"

    # Get MDD for drawdown scaling
    with conn.cursor() as cur:
        cur.execute("SELECT MDD_ALLTIME FROM FINANCIAL_METRICS ORDER BY METRIC_DATE DESC FETCH FIRST 1 ROWS ONLY")
        row = cur.fetchone()
        mdd = float(row[0]) if row and row[0] else 0.0

    # Group candidates by station/date/type for Smirnov Kelly
    from collections import defaultdict
    groups = defaultdict(list)
    for c in candidates:
        key = f"{c['station_id']}|{c['target_date']}|{c['target_type']}"
        groups[key].append(c)

    best_bets = []

    for group_key, group_candidates in groups.items():
        parts = group_key.split("|")
        sid, td, tt = parts[0], parts[1], parts[2]

        ens_key = group_key
        ens = ensemble_cache.get(ens_key, {"s_tk": 3.0, "f_bar": 0.0, "sigma_eff": 3.0})

        # Evaluate gates for each candidate
        for cand in group_candidates:
            # Compute dynamic lead_hours from target_date + station timezone
            _lead_hours = compute_lead_hours(
                station_tz=tz_map.get(sid, "America/New_York"),
                issued_at=datetime.now(timezone.utc).isoformat(),
                target_date=td,
                kind=tt.lower(),
            )
            _lead_bracket = classify_lead_hours(_lead_hours)
            cand["lead_hours"] = _lead_hours
            cand["lead_bracket"] = _lead_bracket

            # Use actual lead bracket for BSS lookup (was hardcoded "h3")
            bss_key = f"{sid}|{tt}|{_lead_bracket}"
            bss_info = bss_cache.get(bss_key, {"bss": None, "qualified": False})

            gate_result = evaluate_all_gates({
                "p_win": cand["p_win"],
                "c_market": cand["c_market"],
                "bankroll": bankroll,
                "n_bets": n_bets,
                "s_tk": ens["s_tk"],
                "bss": bss_info["bss"],
                "was_qualified": bss_info["qualified"],
                "lead_hours": _lead_hours,
            })

            cand["gate_pass"] = gate_result["pass"]
            cand["gate_flags"] = gate_result["flags"]
            cand["gate_details"] = gate_result["details"]

        # Filter to gate-passing candidates
        passing = [c for c in group_candidates if c.get("gate_pass")]
        if not passing:
            for cand in group_candidates:
                best_bets.append(_make_best_bet(cand, pipeline_run_id, selected=False))
            continue

        # Smirnov Kelly on passing bins
        kelly_bins = [{"p_win": c["p_win"], "c_market": c["c_market"],
                       "ticker": c["ticker"]} for c in passing]
        kelly_result = smirnov_kelly(kelly_bins)
        kelly_map = {r["ticker"]: r.get("f_star", 0.0) for r in kelly_result}

        # Evaluate IBE and full sizing for each passing candidate
        for cand in passing:
            f_star = kelly_map.get(cand["ticker"], 0.0)
            if f_star <= 0:
                best_bets.append(_make_best_bet(cand, pipeline_run_id, selected=False))
                continue

            prev_sb = get_previous_shadow_book(conn, cand["ticker"])
            ibe_result = evaluate_ibe(conn, {
                "station_id": sid,
                "target_type": tt,
                "lead_bracket": cand.get("lead_bracket", "h3"),
                "target_date": td,
                "p_win": cand["p_win"],
                "p_previous": prev_sb["p_win"] if prev_sb else None,
                "c_market": cand["c_market"],
                "c_previous": None,
                "f_bar": ens["f_bar"],
                "s_tk": ens["s_tk"],
                "s_previous": None,
                "sigma_hist": ens["sigma_eff"],
                "b_k": 0.0,
            })

            insert_ibe_signal_log(conn, {
                "ticker": cand["ticker"],
                "pipeline_run_id": pipeline_run_id,
                **ibe_result,
            })

            if ibe_result["veto"]:
                best_bets.append(_make_best_bet(
                    cand, pipeline_run_id, selected=False,
                    ibe_composite=ibe_result["composite"],
                    ibe_veto=True, f_star=f_star,
                ))
                continue

            wfo = wfo_map.get(sid, "UNK")
            remaining = get_remaining_capacity(conn, sid, td, wfo, bankroll)

            from kalshicast.execution.kelly import compute_market_convergence
            gamma, gamma_scale = compute_market_convergence({}, "")

            sizing = full_sizing_chain(
                f_star=f_star,
                bss=bss_info["bss"] or 0.0,
                ibe_composite=ibe_result["composite"],
                gamma_scale=gamma_scale,
                mdd=mdd,
                bankroll=bankroll,
                remaining_capacity=remaining,
                c_market=cand["c_market"],
            )

            selected = not sizing.get("skip", True) and sizing.get("contracts", 0) > 0

            bet = _make_best_bet(
                cand, pipeline_run_id, selected=selected,
                ibe_composite=ibe_result["composite"],
                ibe_veto=False,
                f_star=f_star,
                f_final=sizing.get("f_final", 0.0),
                d_scale=sizing.get("d_scale", 1.0),
                gamma=gamma,
                contracts=sizing.get("contracts", 0),
            )
            best_bets.append(bet)

        # Non-passing candidates
        for cand in group_candidates:
            if not cand.get("gate_pass"):
                continue  # Already added above

    # ── Diagnostic logging: candidate elimination funnel ─────────────
    n_candidates_total = len(candidates)
    n_gate_pass = sum(1 for b in best_bets if b.get("gate_flags", {}).get("edge", False))
    n_spread_pass = sum(1 for b in best_bets if b.get("gate_flags", {}).get("spread", False))
    n_skill_pass = sum(1 for b in best_bets if b.get("gate_flags", {}).get("skill", False))
    n_all_gates_pass = sum(
        1 for b in best_bets
        if all(b.get("gate_flags", {}).get(g, False)
               for g in ("edge", "spread", "skill", "lead", "reserved"))
    )
    n_kelly_positive = sum(1 for b in best_bets if (b.get("f_star") or 0) > 0)
    n_ibe_veto = sum(1 for b in best_bets if b.get("ibe_veto"))
    n_selected = sum(1 for b in best_bets if b.get("is_selected_for_execution"))

    edges = [
        b["p_win"] - b.get("contract_price", b.get("c_vwap", 0))
        for b in best_bets
        if b.get("c_vwap") and b["c_vwap"] > 0
    ]
    avg_edge = sum(edges) / len(edges) if edges else 0.0
    max_edge = max(edges) if edges else 0.0

    log.info(
        "[gates] funnel: candidates=%d → edge_gate=%d spread=%d skill=%d "
        "all_gates=%d → kelly_positive=%d ibe_veto=%d → selected=%d "
        "| avg_edge=%.4f max_edge=%.4f",
        n_candidates_total, n_gate_pass, n_spread_pass, n_skill_pass,
        n_all_gates_pass, n_kelly_positive, n_ibe_veto, n_selected,
        avg_edge, max_edge,
    )

    # Top 5 candidates by edge for diagnostics
    if candidates:
        by_edge = sorted(candidates,
                         key=lambda c: c.get("p_win", 0) - c.get("c_market", 0),
                         reverse=True)[:5]
        for c in by_edge:
            flags = c.get("gate_flags", {})
            flag_str = " ".join(
                f"{g}={'Y' if p else 'N'}" for g, p in flags.items()
            ) if flags else "no_flags"
            log.info(
                "[gates] top5: %s %s/%s p=%.4f c=%.4f edge=%.4f "
                "lead=%.1fh(%s) | %s",
                c.get("station_id", "?"), c.get("target_date", "?"),
                c.get("target_type", "?"),
                c.get("p_win", 0), c.get("c_market", 0),
                c.get("p_win", 0) - c.get("c_market", 0),
                c.get("lead_hours", 0), c.get("lead_bracket", "?"),
                flag_str,
            )

    if n_candidates_total > 0 and n_selected == 0:
        if n_all_gates_pass == 0:
            gate_names = ["edge", "spread", "skill", "lead"]
            fail_counts = {
                g: sum(1 for b in best_bets
                       if not b.get("gate_flags", {}).get(g, True))
                for g in gate_names
            }
            worst_gate = max(fail_counts, key=fail_counts.get)
            bottleneck = f"gate_{worst_gate}_rejected_{fail_counts[worst_gate]}"
        elif n_kelly_positive == 0:
            bottleneck = "kelly_no_positive_edge"
        elif n_ibe_veto > 0:
            bottleneck = f"ibe_vetoed_{n_ibe_veto}"
        else:
            bottleneck = "sizing_chain_below_minimum"
        log.warning("[gates] bottleneck: %s", bottleneck)
    # ── END funnel logging ───────────────────────────────────────────

    # Write all best bets
    upsert_best_bets(conn, best_bets)
    conn.commit()

    return [b for b in best_bets if b.get("is_selected_for_execution")]


def _create_baseline_bets(conn: Any, pipeline_run_id: str, target_dates: list) -> list:
    """Baseline fallback: pick highest-p_win bin per (station, date, type).

    Used when the full gate chain rejects all candidates or when Kalshi market
    prices are unavailable.  Places one small bet per group so the paper-sim
    pipeline has positions to settle and grade.

    Minimum criteria:
    - p_win > 0.20 (model assigns meaningful probability)
    - One bet per (station, date, type) group (the best bin)
    - Fixed sizing: 1 % of $1 000 paper bankroll
    """
    from collections import defaultdict

    with conn.cursor() as cur:
        cur.execute("""
            SELECT sb.TICKER, sb.STATION_ID, sb.TARGET_DATE, sb.TARGET_TYPE,
                   sb.BIN_LOWER, sb.BIN_UPPER, sb.P_WIN,
                   COALESCE(km.YES_ASK / 100.0, km.LAST_PRICE / 100.0,
                            km.YES_BID / 100.0, 0.50) AS C_MARKET
            FROM SHADOW_BOOK sb
            LEFT JOIN KALSHI_MARKETS km ON km.TICKER = sb.TICKER
            WHERE sb.PIPELINE_RUN_ID = :run_id
              AND sb.P_WIN IS NOT NULL
              AND sb.P_WIN > 0.20
        """, {"run_id": pipeline_run_id})
        rows = cur.fetchall()

    if not rows:
        return []

    # Group by (station, date, type), pick highest p_win
    groups: dict[str, list] = defaultdict(list)
    for r in rows:
        key = f"{r[1]}|{r[2]}|{r[3]}"
        groups[key].append(r)

    bets = []
    for key, group in groups.items():
        best = max(group, key=lambda r: float(r[6]))
        ticker, sid, td, tt, bl, bu, p_win, c_market = best
        p_win_f = float(p_win)
        c_market_f = float(c_market)

        bets.append({
            "ticker": ticker,
            "pipeline_run_id": pipeline_run_id,
            "station_id": sid,
            "target_date": str(td)[:10] if td else None,
            "target_type": tt,
            "bin_lower": float(bl) if bl is not None else None,
            "bin_upper": float(bu) if bu is not None else None,
            "p_win": p_win_f,
            "contract_price": c_market_f,
            "c_vwap": c_market_f,
            "ev_net": (p_win_f - c_market_f) * 100,
            "order_type": "BASELINE",
            "f_star": 0.01,
            "f_final": 0.01,
            "is_selected_for_execution": True,
            "pipeline_run_status": "OK",
            "gate_flags": {"edge": True, "spread": True, "skill": True,
                           "lead": True, "reserved": True},
            "contracts": max(1, int(0.01 * 1000 / max(c_market_f, 0.01))),
        })

    return bets


def _make_best_bet(
    cand: dict,
    pipeline_run_id: str,
    *,
    selected: bool = False,
    ibe_composite: float | None = None,
    ibe_veto: bool = False,
    f_star: float = 0.0,
    f_final: float = 0.0,
    d_scale: float = 1.0,
    gamma: float = 1.0,
    contracts: int = 0,
) -> dict:
    """Build a BEST_BETS row dict."""
    return {
        "ticker": cand["ticker"],
        "pipeline_run_id": pipeline_run_id,
        "station_id": cand.get("station_id"),
        "target_date": cand.get("target_date"),
        "target_type": cand.get("target_type"),
        "bin_lower": cand.get("bin_lower"),
        "bin_upper": cand.get("bin_upper"),
        "p_win": cand.get("p_win"),
        "contract_price": cand.get("c_market"),
        "ev_net": (cand.get("p_win", 0) - cand.get("c_market", 0)) * 100 if cand.get("p_win") else None,
        "order_type": None,
        "c_vwap": cand.get("c_market"),
        "f_star": f_star,
        "f_final": f_final,
        "ibe_composite": ibe_composite,
        "ibe_veto": ibe_veto,
        "d_scale": d_scale,
        "gamma_convergence": gamma,
        "is_selected_for_execution": selected,
        "pipeline_run_status": "OK",
        "gate_flags": cand.get("gate_flags"),
        "contracts": contracts,
        # Pass through for order execution
        "s_tk": cand.get("s_tk"),
        "lead_hours": cand.get("lead_hours", 24.0),
    }


def _step11_update_health(conn: Any, target_dates: list[str],
                          total_ensemble: int, total_shadow: int,
                          total_bets: int) -> None:
    """Update PIPELINE_DAY_HEALTH for each target date."""
    for td in target_dates:
        with conn.cursor() as cur:
            cur.execute("""
                MERGE INTO PIPELINE_DAY_HEALTH tgt USING DUAL
                ON (tgt.TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD'))
                WHEN MATCHED THEN UPDATE SET
                    RUN_TS = SYSTIMESTAMP,
                    STATIONS_FORECASTED = :sf,
                    STATIONS_SCORED = :ss,
                    IS_HEALTHY = CASE WHEN :sf > 0 THEN 1 ELSE 0 END
                WHEN NOT MATCHED THEN INSERT (
                    TARGET_DATE, RUN_TS, STATIONS_FORECASTED, STATIONS_SCORED, IS_HEALTHY
                ) VALUES (
                    TO_DATE(:td, 'YYYY-MM-DD'), SYSTIMESTAMP, :sf, :ss,
                    CASE WHEN :sf > 0 THEN 1 ELSE 0 END
                )
            """, {"td": td, "sf": total_ensemble, "ss": total_bets})
    conn.commit()


if __name__ == "__main__":
    main()
