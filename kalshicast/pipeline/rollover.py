"""Date rollover — METAR initialization, Shadow Book finalization, settlement.

Runs at the start of each day to prepare tables and settle expired positions.
Includes paper position settlement for simulation mode.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from kalshicast.db.operations import new_run_id

log = logging.getLogger(__name__)


def init_metar_daily_max(conn: Any, target_date: str) -> int:
    """Insert placeholder METAR_DAILY_MAX rows for today's active stations."""
    with conn.cursor() as cur:
        cur.execute("""
            MERGE INTO METAR_DAILY_MAX tgt
            USING (
                SELECT STATION_ID FROM STATIONS WHERE IS_ACTIVE = 1
            ) src
            ON (tgt.STATION_ID = src.STATION_ID
                AND tgt.LOCAL_DATE = TO_DATE(:td, 'YYYY-MM-DD'))
            WHEN NOT MATCHED THEN INSERT (
                STATION_ID, LOCAL_DATE, OBS_COUNT, LAST_UPDATED_UTC
            ) VALUES (
                src.STATION_ID, TO_DATE(:td, 'YYYY-MM-DD'), 0, SYSTIMESTAMP
            )
        """, {"td": target_date})
        count = cur.rowcount or 0
    conn.commit()
    log.info("[rollover] initialized %d METAR_DAILY_MAX rows for %s", count, target_date)
    return count


def finalize_shadow_book(conn: Any, target_date: str) -> int:
    """Mark previous day's Shadow Book entries as finalized (set UPDATED_AT)."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE SHADOW_BOOK
            SET UPDATED_AT = SYSTIMESTAMP
            WHERE TARGET_DATE = TO_DATE(:td, 'YYYY-MM-DD')
              AND UPDATED_AT IS NULL
        """, {"td": target_date})
        count = cur.rowcount or 0
    conn.commit()
    log.info("[rollover] finalized %d shadow book entries for %s", count, target_date)
    return count


def settle_positions(conn: Any) -> int:
    """Settle live OPEN positions against real observations."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE POSITIONS p SET
                STATUS = 'SETTLED',
                OUTCOME = CASE
                    WHEN p.TARGET_TYPE = 'HIGH' AND
                         (SELECT o.OBSERVED_HIGH_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID = p.STATION_ID AND o.TARGET_DATE = p.TARGET_DATE)
                         >= p.BIN_LOWER AND
                         (SELECT o.OBSERVED_HIGH_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID = p.STATION_ID AND o.TARGET_DATE = p.TARGET_DATE)
                         < p.BIN_UPPER THEN 1
                    WHEN p.TARGET_TYPE = 'LOW' AND
                         (SELECT o.OBSERVED_LOW_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID = p.STATION_ID AND o.TARGET_DATE = p.TARGET_DATE)
                         >= p.BIN_LOWER AND
                         (SELECT o.OBSERVED_LOW_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID = p.STATION_ID AND o.TARGET_DATE = p.TARGET_DATE)
                         < p.BIN_UPPER THEN 1
                    ELSE 0
                END,
                PNL_GROSS = CASE
                    WHEN p.TARGET_TYPE = 'HIGH' AND
                         (SELECT o.OBSERVED_HIGH_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID = p.STATION_ID AND o.TARGET_DATE = p.TARGET_DATE)
                         >= p.BIN_LOWER AND
                         (SELECT o.OBSERVED_HIGH_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID = p.STATION_ID AND o.TARGET_DATE = p.TARGET_DATE)
                         < p.BIN_UPPER
                    THEN (1.0 - p.ENTRY_PRICE) * p.CONTRACTS * 100
                    WHEN p.TARGET_TYPE = 'LOW' AND
                         (SELECT o.OBSERVED_LOW_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID = p.STATION_ID AND o.TARGET_DATE = p.TARGET_DATE)
                         >= p.BIN_LOWER AND
                         (SELECT o.OBSERVED_LOW_F FROM OBSERVATIONS o
                          WHERE o.STATION_ID = p.STATION_ID AND o.TARGET_DATE = p.TARGET_DATE)
                         < p.BIN_UPPER
                    THEN (1.0 - p.ENTRY_PRICE) * p.CONTRACTS * 100
                    ELSE -p.ENTRY_PRICE * p.CONTRACTS * 100
                END,
                FILLED_AT = SYSTIMESTAMP
            WHERE p.STATUS = 'OPEN'
              AND p.IS_PAPER = 0          -- live only
              AND p.TARGET_DATE < TRUNC(SYSDATE)
              AND EXISTS (
                  SELECT 1 FROM OBSERVATIONS o
                  WHERE o.STATION_ID = p.STATION_ID
                    AND o.TARGET_DATE = p.TARGET_DATE
              )
        """)
        count = cur.rowcount or 0

    if count > 0:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE POSITIONS SET
                    PNL_NET = PNL_GROSS - ABS(PNL_GROSS) * 0.07
                WHERE STATUS = 'SETTLED'
                  AND IS_PAPER = 0
                  AND PNL_NET IS NULL
            """)

    conn.commit()
    log.info("[rollover] settled %d live positions", count)
    return count


def purge_resolved_alerts(conn: Any) -> int:
    """Delete resolved alerts older than alerts.retention_days."""
    from kalshicast.config.params_bootstrap import get_param_int
    retention = get_param_int("alerts.retention_days", default=7)
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM SYSTEM_ALERTS
            WHERE IS_RESOLVED = 1
              AND RESOLVED_TS < SYSTIMESTAMP - NUMTODSINTERVAL(:days, 'DAY')
        """, {"days": retention})
        count = cur.rowcount or 0
    conn.commit()
    log.info("[rollover] purged %d resolved alerts (retention=%d days)", count, retention)
    return count


def run_rollover(conn: Any) -> dict:
    """Master rollover sequence — run at start of each day."""
    from kalshicast.pipeline.paper_sim import settle_paper_positions

    today     = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    metar_init      = init_metar_daily_max(conn, today)
    sb_final        = finalize_shadow_book(conn, yesterday)
    settled         = settle_positions(conn)
    paper_settled   = settle_paper_positions(conn)
    alerts_purged   = purge_resolved_alerts(conn)

    return {
        "date":                    today,
        "metar_initialized":       metar_init,
        "shadow_book_finalized":   sb_final,
        "positions_settled":       settled,
        "paper_positions_settled": paper_settled,
        "alerts_purged":           alerts_purged,
    }
