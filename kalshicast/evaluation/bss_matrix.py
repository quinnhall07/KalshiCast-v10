"""BSS skill matrix — 200-cell qualification tracker.

Spec §8.1.6: 20 stations × 5 lead brackets × 2 types = 200 cells.
Hysteresis: enter at BSS ≥ 0.07, exit below 0.03.
"""

from __future__ import annotations

import logging
from typing import Any

from kalshicast.config.params_bootstrap import get_param_int, get_param_float
from kalshicast.db.operations import refresh_bss_matrix as _refresh_db

log = logging.getLogger(__name__)


def get_qualification_hysteresis(current_bss: float | None,
                                  was_qualified: bool,
                                  enter_threshold: float | None = None,
                                  exit_threshold: float | None = None) -> bool:
    """Hysteresis qualification logic. §8.1.6.

    Enter at bss_enter (default 0.07), exit below bss_exit (default 0.03).
    Once qualified, stays qualified until BSS drops below exit.
    """
    if enter_threshold is None:
        enter_threshold = get_param_float("gate.bss_enter")
    if exit_threshold is None:
        exit_threshold = get_param_float("gate.bss_exit")

    if current_bss is None:
        return False

    if was_qualified:
        # Stay qualified until below exit threshold
        return current_bss >= exit_threshold
    else:
        # Enter only if above enter threshold
        return current_bss >= enter_threshold


def refresh_bss_matrix(conn: Any) -> int:
    """Recompute BSS for all cells and apply hysteresis.

    1. Run SQL MERGE to compute BS_model and BS_clim per cell
    2. Apply hysteresis logic to update IS_QUALIFIED
    3. Compute H_STAR_S (max qualified bracket per station)

    Returns count of cells updated.
    """
    window = get_param_int("eval.bss_window_days")
    n = _refresh_db(conn, window)

    if n > 0:
        # Apply hysteresis in Python
        _apply_hysteresis(conn)
        conn.commit()

    log.info("[bss_matrix] refreshed %d cells (window=%d days)", n, window)
    return n


def _apply_hysteresis(conn: Any) -> None:
    """Read BSS_MATRIX, apply hysteresis to IS_QUALIFIED, update."""
    from kalshicast.db.operations import get_bss_matrix_all

    cells = get_bss_matrix_all(conn)
    enter = get_param_float("gate.bss_enter")
    exit_ = get_param_float("gate.bss_exit")

    for cell in cells:
        bss = float(cell["bss_1"]) if cell.get("bss_1") is not None else None
        was_q = bool(cell.get("is_qualified"))
        new_q = get_qualification_hysteresis(bss, was_q, enter, exit_)

        if new_q != was_q:
            with conn.cursor() as cur:
                if new_q and not was_q:
                    cur.execute("""
                        UPDATE BSS_MATRIX SET IS_QUALIFIED = 1, ENTERED_AT = SYSTIMESTAMP
                        WHERE STATION_ID = :sid AND TARGET_TYPE = :tt AND LEAD_BRACKET = :lb
                    """, {"sid": cell["station_id"], "tt": cell["target_type"],
                          "lb": cell["lead_bracket"]})
                elif not new_q and was_q:
                    cur.execute("""
                        UPDATE BSS_MATRIX SET IS_QUALIFIED = 0, EXITED_AT = SYSTIMESTAMP
                        WHERE STATION_ID = :sid AND TARGET_TYPE = :tt AND LEAD_BRACKET = :lb
                    """, {"sid": cell["station_id"], "tt": cell["target_type"],
                          "lb": cell["lead_bracket"]})

    # Compute H_STAR_S per station
    _update_h_star(conn)


def _update_h_star(conn: Any) -> None:
    """Set H_STAR_S = max qualified lead bracket per station."""
    bracket_order = {"h5": 5, "h4": 4, "h3": 3, "h2": 2, "h1": 1}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT STATION_ID, MAX(LEAD_BRACKET) AS max_lb
            FROM BSS_MATRIX
            WHERE IS_QUALIFIED = 1
            GROUP BY STATION_ID
        """)
        for station_id, max_lb in cur:
            with conn.cursor() as cur2:
                cur2.execute("""
                    UPDATE BSS_MATRIX SET H_STAR_S = :h_star
                    WHERE STATION_ID = :sid
                """, {"h_star": max_lb, "sid": station_id})
