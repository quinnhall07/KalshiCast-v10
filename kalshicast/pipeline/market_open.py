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

from kalshicast.config.params_bootstrap import get_param_int, get_param_float, load_db_overrides, get_param_bool
from kalshicast.db.connection import init_db, get_conn, close_pool
from kalshicast.db.schema import ensure_schema, seed_config_tables
from kalshicast.db.operations import (
    new_run_id, insert_pipeline_run, update_pipeline_run,
    load_all_params, upsert_best_bets, insert_orderbook_snapshot,
    insert_ibe_signal_log, get_previous_shadow_book,
)

log = logging.getLogger(__name__)


def main() -> None:
    """Market-open pipeline — paper or live mode."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    # Parse --live flag
    live_mode = "--live" in sys.argv

    # Step 1: init_db + PIPELINE_RUNS
    init_db()
    conn = get_conn()
    try:
        ensure_schema(conn)
        seed_config_tables(conn)
    finally:
        conn.close()

    pipeline_run_id = new_run_id()
    conn = get_conn()
    try:
        insert_pipeline_run(conn, pipeline_run_id, "market_open")
        conn.commit()
    finally:
        conn.close()

    # Step 2: load_params
    conn = get_conn()
    try:
        db_params = load_all_params(conn)
        load_db_overrides(db_params)
    finally:
        conn.close()

    is_halted = get_param_bool("system.trading_halted", default=False)
    
    if is_halted:
        log.warning("TRADING HALTED: 'system.trading_halted' is set to True in the database. Aborting execution.")
        return

    # Target dates: today + next FORECAST_DAYS
    now_utc = datetime.now(timezone.utc)
    forecast_days = get_param_int("pipeline.forecast_days")
    target_dates = [(now_utc.date() + timedelta(days=d)).isoformat()
                    for d in range(forecast_days)]

    mode_str = "LIVE" if live_mode else "PAPER"
    log.info("Market-open pipeline (%s MODE) run_id=%s, dates=%s",
             mode_str, pipeline_run_id[:8], target_dates)

    status = "OK"
    error_msg = None
    total_ensemble = 0
    total_shadow = 0
    total_bets = 0
    total_orders = 0

    conn = get_conn()
    try:
        # Step 3: fetch_bankroll
        bankroll = 1000.0  # Paper mode default
        if live_mode:
            try:
                from kalshicast.execution.kalshi_api import KalshiClient
                client = KalshiClient()
                bankroll = client.get_balance()
                log.info("Step 3: live bankroll=$%.2f", bankroll)
            except Exception as e:
                log.error("Step 3 ERROR: bankroll fetch failed: %s", e)
                log.info("Step 3: falling back to paper bankroll=$%.2f", bankroll)
                status = "PARTIAL"
        else:
            client = None
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
            log.error("Step 6 ERROR: ensemble computation failed: %s", e)
            status = "PARTIAL"

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
            status = "PARTIAL"

        # Steps 8-10: Live execution (gates, IBE, Kelly, orders)
        if live_mode and client is not None:
            # Step 8: fetch_market_prices
            try:
                total_bets = _step8_fetch_market_prices(conn, client, pipeline_run_id)
                log.info("Step 8 OK: %d orderbook snapshots", total_bets)
            except Exception as e:
                log.error("Step 8 ERROR: market price fetch failed: %s", e)
                status = "PARTIAL"

            # Step 9: evaluate_gates_and_ibe
            try:
                best_bets = _step9_evaluate_gates_ibe(
                    conn, pipeline_run_id, bankroll, target_dates)
                total_bets = len(best_bets)
                log.info("Step 9 OK: %d best bets evaluated", total_bets)
            except Exception as e:
                log.error("Step 9 ERROR: gate/IBE evaluation failed: %s", e)
                best_bets = []
                status = "PARTIAL"

            # Step 10: submit_orders
            try:
                from kalshicast.execution.orders import execute_best_bets
                summary = execute_best_bets(client, conn, best_bets)
                total_orders = summary.get("submitted", 0)
                log.info("Step 10 OK: %d orders submitted, %d filled, %d skipped, %d errors",
                         summary.get("submitted", 0), summary.get("filled", 0),
                         summary.get("skipped", 0), summary.get("errors", 0))
            except Exception as e:
                log.error("Step 10 ERROR: order submission failed: %s", e)
                status = "PARTIAL"
        else:
            log.info("Steps 8-10: SKIPPED (%s mode — no market fetch, gates, or orders)", mode_str)

        # Step 11: update pipeline_day_health
        try:
            _step11_update_health(conn, target_dates, total_ensemble, total_shadow, total_bets)
            log.info("Step 11 OK: pipeline_day_health updated")
        except Exception as e:
            log.warning("Step 11 WARN: health update failed: %s", e)

        # Step 12: update_pipeline_run
        update_pipeline_run(
            conn, pipeline_run_id,
            status=status,
            rows_daily=total_shadow,
        )
        conn.commit()

    except Exception as e:
        log.exception("Market-open pipeline failed: %s", e)
        status = "ERROR"
        error_msg = str(e)[:2000]
        try:
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

def _step8_fetch_market_prices(conn: Any, client: Any, run_id: str) -> int:
    """Fetch order books for all active Shadow Book tickers."""
    from kalshicast.execution.vwap import compute_vwap

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT TICKER FROM SHADOW_BOOK
            WHERE PIPELINE_RUN_ID = :run_id AND P_WIN IS NOT NULL
        """, {"run_id": run_id})
        tickers = [row[0] for row in cur]

    count = 0
    for ticker in tickers:
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
        except Exception as e:
            log.warning("Orderbook fetch failed for %s: %s", ticker, e)

    conn.commit()
    return count


def _step9_evaluate_gates_ibe(
    conn: Any,
    pipeline_run_id: str,
    bankroll: float,
    target_dates: list[str],
) -> list[dict]:
    """Evaluate conviction gates, IBE signals, Kelly sizing for all candidates."""
    from kalshicast.execution.gates import evaluate_all_gates
    from kalshicast.execution.ibe import evaluate_ibe
    from kalshicast.execution.kelly import smirnov_kelly, full_sizing_chain
    from kalshicast.execution.positions import get_remaining_capacity

    # Get Shadow Book candidates with market prices
    with conn.cursor() as cur:
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
        return []

    # Get ensemble state for spread info
    ensemble_cache: dict[str, dict] = {}
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

    # Get BSS info
    bss_cache: dict[str, dict] = {}
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

    # Get station WFO mapping
    wfo_map: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT STATION_ID, WFO_ID FROM STATIONS WHERE IS_ACTIVE = 1")
        for row in cur:
            wfo_map[row[0]] = row[1] or "UNK"

    # Get MDD for drawdown scaling
    with conn.cursor() as cur:
        cur.execute("SELECT MDD_ALLTIME FROM FINANCIAL_METRICS ORDER BY METRIC_DATE DESC FETCH FIRST 1 ROWS ONLY")
        row = cur.fetchone()
        mdd = float(row[0]) if row and row[0] else 0.0

    # Group candidates by station/date/type for Smirnov Kelly
    from collections import defaultdict
    groups: dict[str, list[dict]] = defaultdict(list)
    for c in candidates:
        key = f"{c['station_id']}|{c['target_date']}|{c['target_type']}"
        groups[key].append(c)

    best_bets: list[dict] = []

    for group_key, group_candidates in groups.items():
        parts = group_key.split("|")
        sid, td, tt = parts[0], parts[1], parts[2]

        ens_key = group_key
        ens = ensemble_cache.get(ens_key, {"s_tk": 3.0, "f_bar": 0.0, "sigma_eff": 3.0})

        # Evaluate gates for each candidate
        for cand in group_candidates:
            bss_key = f"{sid}|{tt}|h3"  # Default bracket
            bss_info = bss_cache.get(bss_key, {"bss": None, "qualified": False})

            gate_result = evaluate_all_gates({
                "p_win": cand["p_win"],
                "c_market": cand["c_market"],
                "bankroll": bankroll,
                "n_bets": n_bets,
                "s_tk": ens["s_tk"],
                "bss": bss_info["bss"],
                "was_qualified": bss_info["qualified"],
                "lead_hours": 24.0,  # Default; compute from target_date in production
            })

            cand["gate_pass"] = gate_result["pass"]
            cand["gate_flags"] = gate_result["flags"]
            cand["gate_details"] = gate_result["details"]

        # Filter to gate-passing candidates
        passing = [c for c in group_candidates if c.get("gate_pass")]
        if not passing:
            # Still record all as non-selected
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

            # IBE evaluation
            prev_sb = get_previous_shadow_book(conn, cand["ticker"])
            ibe_result = evaluate_ibe(conn, {
                "station_id": sid,
                "target_type": tt,
                "lead_bracket": "h3",
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

            # Log IBE
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

            # Full sizing chain
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

    # Write all best bets
    upsert_best_bets(conn, best_bets)
    conn.commit()

    return [b for b in best_bets if b.get("is_selected_for_execution")]


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
        "lead_hours": 24.0,
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
