"""Position tracking and 5 limit checks.

Spec §7.5: Single, station-day, station-total, correlated, portfolio limits.
All expressed as fractions of current bankroll.
"""

from __future__ import annotations

import logging
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float

log = logging.getLogger(__name__)


def get_open_position_value(conn: Any, *, station_id: str | None = None,
                            target_date: str | None = None,
                            wfo_id: str | None = None) -> float:
    """Sum of (ENTRY_PRICE × CONTRACTS) for open positions, optionally filtered."""
    # Update this line to count both live and paper positions
    conditions = ["p.STATUS IN ('OPEN', 'PAPER_OPEN')"]
    binds: dict = {}

    if station_id:
        conditions.append("p.STATION_ID = :sid")
        binds["sid"] = station_id
    if target_date:
        conditions.append("p.TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD')")
        binds["td"] = target_date
    if wfo_id:
        conditions.append("s.WFO_ID = :wfo")
        binds["wfo"] = wfo_id

    where = " AND ".join(conditions)

    join_clause = ""
    if wfo_id:
        join_clause = "JOIN STATIONS s ON s.STATION_ID = p.STATION_ID"

    sql = f"""
        SELECT COALESCE(SUM(p.ENTRY_PRICE * p.CONTRACTS), 0)
        FROM POSITIONS p {join_clause}
        WHERE {where}
    """
    with conn.cursor() as cur:
        cur.execute(sql, binds)
        row = cur.fetchone()
        return float(row[0]) if row and row[0] else 0.0


def check_single_limit(f_proposed: float) -> float:
    """Limit 1: Cap single position fraction."""
    cap = get_param_float("position.max_single_fraction")
    return min(f_proposed, cap)


def check_station_day_limit(conn: Any, station_id: str, target_date: str,
                            bankroll: float) -> float:
    """Limit 2: Remaining capacity for this station on this day."""
    cap = get_param_float("position.max_station_day_fraction")
    current = get_open_position_value(conn, station_id=station_id, target_date=target_date)
    max_dollar = cap * bankroll
    remaining = max(0.0, max_dollar - current)
    return remaining / max(bankroll, 1.0)


def check_station_total_limit(conn: Any, station_id: str, bankroll: float) -> float:
    """Limit 3: Remaining capacity for this station (all dates)."""
    cap = get_param_float("position.max_station_fraction")
    current = get_open_position_value(conn, station_id=station_id)
    max_dollar = cap * bankroll
    remaining = max(0.0, max_dollar - current)
    return remaining / max(bankroll, 1.0)


def check_correlated_limit(conn: Any, wfo_id: str, bankroll: float) -> float:
    """Limit 4: Remaining capacity for all stations sharing this WFO."""
    cap = get_param_float("position.max_correlated_fraction")
    current = get_open_position_value(conn, wfo_id=wfo_id)
    max_dollar = cap * bankroll
    remaining = max(0.0, max_dollar - current)
    return remaining / max(bankroll, 1.0)


def check_portfolio_limit(conn: Any, bankroll: float) -> float:
    """Limit 5: Remaining total portfolio capacity."""
    cap = get_param_float("position.max_total_fraction")
    current = get_open_position_value(conn)
    max_dollar = cap * bankroll
    remaining = max(0.0, max_dollar - current)
    return remaining / max(bankroll, 1.0)


def get_remaining_capacity(
    conn: Any,
    station_id: str,
    target_date: str,
    wfo_id: str,
    bankroll: float,
) -> float:
    """Return minimum of all 5 limits as remaining fraction of bankroll."""
    limits = [
        get_param_float("position.max_single_fraction"),
        check_station_day_limit(conn, station_id, target_date, bankroll),
        check_station_total_limit(conn, station_id, bankroll),
        check_correlated_limit(conn, wfo_id, bankroll),
        check_portfolio_limit(conn, bankroll),
    ]
    return max(0.0, min(limits))
