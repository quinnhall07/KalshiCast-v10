"""Order orchestration — maker/taker decision, submission, retry.

Spec §7.7: Fee-aware order type selection, tranche execution, audit logging.
"""

from __future__ import annotations

import logging
import math
import time
import uuid
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float, get_param_int
from kalshicast.execution.vwap import compute_vwap, check_staleness, split_tranches

log = logging.getLogger(__name__)


def compute_ev_net(p_win: float, c_price: float, fee_rate: float) -> float:
    """Net EV per contract in cents: (p - c) × 100 - fee."""
    fee_cents = math.ceil(fee_rate * c_price * (1 - c_price) * 100)
    return (p_win - c_price) * 100 - fee_cents


def maker_or_taker(lead_hours: float, p_win: float, c_bid: float, c_ask: float) -> dict:
    """Decide MAKER vs TAKER based on fill probability and net EV.

    P_fill = 1 - exp(-lead_hours / h_half).
    Compare EV_net for each order type.
    """
    h_half = get_param_float("fee.maker_fill_prob_h_half")
    taker_rate = get_param_float("fee.taker_rate")
    maker_rate = get_param_float("fee.maker_rate")

    p_fill = 1.0 - math.exp(-lead_hours / max(h_half, 0.1))

    ev_taker = compute_ev_net(p_win, c_ask, taker_rate)
    ev_maker_raw = compute_ev_net(p_win, c_bid, maker_rate)
    ev_maker = p_fill * ev_maker_raw

    if p_fill > 0.80 and ev_maker > ev_taker:
        return {
            "order_type": "MAKER",
            "price": c_bid,
            "ev_net": round(ev_maker, 4),
            "p_fill": round(p_fill, 4),
        }

    return {
        "order_type": "TAKER",
        "price": c_ask,
        "ev_net": round(ev_taker, 4),
        "p_fill": round(p_fill, 4),
    }


def submit_single_order(
    client: Any,
    ticker: str,
    price: float,
    quantity: int,
    order_type: str,
    conn: Any,
) -> dict:
    """Submit a single order to Kalshi and log to ORDER_LOG.

    Returns order result dict with status.
    """
    order_id = str(uuid.uuid4())
    client_order_id = str(uuid.uuid4())

    expiration_type = "maker-only" if order_type == "MAKER" else "immediate-or-cancel"

    result = {
        "order_id": order_id,
        "ticker": ticker,
        "contracts": quantity,
        "limit_price": price,
        "order_type": order_type,
        "status": "PENDING",
        "error_msg": None,
        "kalshi_response": None,
    }

    try:
        response = client.submit_order(
            ticker,
            side="buy",
            order_type="limit",
            limit_price=price,
            quantity=quantity,
            client_order_id=client_order_id,
            expiration_type=expiration_type,
        )
        result["kalshi_response"] = response
        result["status"] = "SUBMITTED"
        log.info("Order submitted: %s %d×%s @ %.2f (%s)",
                 ticker, quantity, order_type, price, response.get("order", {}).get("status", "?"))
    except Exception as e:
        result["status"] = "REJECTED"
        result["error_msg"] = str(e)[:2000]
        log.error("Order failed: %s %d×%s @ %.2f: %s",
                  ticker, quantity, order_type, price, e)

    # Log to ORDER_LOG
    try:
        import json
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ORDER_LOG (
                    ORDER_ID, TICKER, CONTRACTS, LIMIT_PRICE,
                    ORDER_TYPE, SUBMITTED_AT, KALSHI_RESPONSE_JSON,
                    STATUS, ERROR_MSG
                ) VALUES (
                    :oid, :ticker, :qty, :price,
                    :otype, SYSTIMESTAMP, :resp,
                    :status, :err
                )
            """, {
                "oid": order_id,
                "ticker": ticker,
                "qty": quantity,
                "price": price,
                "otype": order_type,
                "resp": json.dumps(result.get("kalshi_response")) if result.get("kalshi_response") else None,
                "status": result["status"],
                "err": result.get("error_msg"),
            })
    except Exception as e:
        log.error("Failed to log order: %s", e)

    return result


def execute_best_bets(
    client: Any,
    conn: Any,
    best_bets: list[dict],
) -> dict:
    """Execute all selected best bets.

    For each bet:
    1. Fetch orderbook → compute VWAP → staleness check
    2. Maker/taker decision
    3. Tranche if needed
    4. Submit with retry
    5. Write POSITIONS

    Returns summary: {submitted, filled, skipped, errors}.
    """
    tranche_delay = get_param_int("vwap.tranche_delay_sec")
    maker_timeout = get_param_int("order.maker_timeout_sec")

    summary = {"submitted": 0, "filled": 0, "skipped": 0, "errors": 0}

    for bet in best_bets:
        if not bet.get("is_selected_for_execution"):
            summary["skipped"] += 1
            continue

        ticker = bet["ticker"]
        contracts = bet.get("contracts", 0)
        if contracts < 1:
            summary["skipped"] += 1
            continue

        # Step 1: Fetch orderbook and compute VWAP
        try:
            orderbook = client.get_orderbook(ticker)
        except Exception as e:
            log.error("Orderbook fetch failed for %s: %s", ticker, e)
            summary["errors"] += 1
            continue

        c_vwap, depth = compute_vwap(orderbook, contracts)
        if depth == 0:
            log.warning("No depth for %s, skipping", ticker)
            summary["skipped"] += 1
            continue

        # Best ask price
        yes_book = orderbook.get("yes", [])
        c_best = yes_book[0]["price"] / 100.0 if yes_book else c_vwap

        stale = check_staleness(c_vwap, c_best)
        if stale["abort"]:
            log.warning("Order book stale for %s (delta=%.4f), aborting", ticker, stale["delta"])
            summary["skipped"] += 1
            continue

        # Step 2: Maker/taker decision
        no_book = orderbook.get("no", [])
        c_bid = (1.0 - no_book[0]["price"] / 100.0) if no_book else c_vwap
        c_ask = c_best

        decision = maker_or_taker(
            bet.get("lead_hours", 24.0),
            bet["p_win"],
            c_bid, c_ask,
        )

        # Step 3: Tranche
        tranches = split_tranches(contracts)

        # Step 4: Submit tranches
        position_id = str(uuid.uuid4())
        total_filled = 0

        for i, tranche_qty in enumerate(tranches):
            if i > 0:
                time.sleep(tranche_delay)

                # Re-fetch orderbook for subsequent tranches
                try:
                    orderbook = client.get_orderbook(ticker)
                    c_vwap, depth = compute_vwap(orderbook, tranche_qty)
                    if depth == 0:
                        log.warning("Depth exhausted for %s tranche %d", ticker, i + 1)
                        break
                except Exception as e:
                    log.error("Orderbook re-fetch failed: %s", e)
                    break

            result = submit_single_order(
                client, ticker, decision["price"], tranche_qty,
                decision["order_type"], conn,
            )

            if result.get("status") == "SUBMITTED":
                summary["submitted"] += 1
                total_filled += tranche_qty
            else:
                summary["errors"] += 1

            # Link order to position
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE ORDER_LOG SET POSITION_ID = :pid
                        WHERE ORDER_ID = :oid
                    """, {"pid": position_id, "oid": result["order_id"]})
            except Exception:
                pass

        # Step 5: Write position
        if total_filled > 0:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        MERGE INTO POSITIONS tgt USING DUAL
                        ON (tgt.POSITION_ID = :pid)
                        WHEN NOT MATCHED THEN INSERT (
                            POSITION_ID, TICKER, STATION_ID, TARGET_DATE, TARGET_TYPE,
                            BIN_LOWER, BIN_UPPER, ENTRY_PRICE, CONTRACTS,
                            ORDER_TYPE, SUBMITTED_AT, STATUS, S_TK_AT_ENTRY
                        ) VALUES (
                            :pid, :ticker, :sid, TO_DATE(:td, 'YYYY-MM-DD'), :tt,
                            :bl, :bu, :ep, :qty,
                            :otype, SYSTIMESTAMP, 'OPEN', :stk
                        )
                    """, {
                        "pid": position_id,
                        "ticker": ticker,
                        "sid": bet.get("station_id"),
                        "td": str(bet.get("target_date", ""))[:10],
                        "tt": bet.get("target_type"),
                        "bl": bet.get("bin_lower"),
                        "bu": bet.get("bin_upper"),
                        "ep": decision["price"],
                        "qty": total_filled,
                        "otype": decision["order_type"],
                        "stk": bet.get("s_tk"),
                    })
                summary["filled"] += 1
            except Exception as e:
                log.error("Position insert failed: %s", e)

        conn.commit()

    return summary
