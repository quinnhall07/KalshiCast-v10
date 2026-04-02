"""Risk management evaluation for determining OFFLINE status."""

import logging
from typing import Any
from kalshicast.config.params_bootstrap import get_param_float, get_param_bool

log = logging.getLogger(__name__)

def evaluate_system_health(conn: Any, bankroll: float) -> bool:
    """
    Evaluates algorithmic risk conditions.
    Returns True if the system should go OFFLINE, False if ONLINE.
    """
    is_offline = False
    reasons = []

    # Condition 1: Max Drawdown Exceeded
    mdd_alltime = 0.0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MDD_ALLTIME FROM FINANCIAL_METRICS ORDER BY METRIC_DATE DESC FETCH FIRST 1 ROWS ONLY")
            row = cur.fetchone()
            if row and row[0]:
                mdd_alltime = float(row[0])
    except Exception as e:
        log.warning("Could not fetch MDD for risk check: %s", e)

    mdd_halt_limit = get_param_float("drawdown.mdd_halt", default=0.20)
    if mdd_alltime >= mdd_halt_limit:
        is_offline = True
        reasons.append(f"MDD ({mdd_alltime*100:.1f}%) >= Limit ({mdd_halt_limit*100:.1f}%)")

    # Condition 2: Minimum Bankroll
    if bankroll <= 5.0:
        is_offline = True
        reasons.append(f"Bankroll (${bankroll:.2f}) is $5.00 or below")
    
    # Condition 3: Warmup Period (Phase 2 Paper Trading)
    days_active = 0
    try:
        with conn.cursor() as cur:
            # Count how many successful daily pipeline runs have completed
            cur.execute("SELECT COUNT(DISTINCT TARGET_DATE) FROM PIPELINE_DAY_HEALTH WHERE IS_HEALTHY = 1")
            row = cur.fetchone()
            if row:
                days_active = int(row[0])
    except Exception as e:
        log.warning("Could not fetch days_active for risk check: %s", e)

    # Use get_param_int (ensure it is imported from kalshicast.config.params_bootstrap)
    from kalshicast.config.params_bootstrap import get_param_int
    warmup_required = get_param_int("system.warmup_days", default=30)
    
    if days_active < warmup_required:
        is_offline = True
        reasons.append(f"Warmup Active (Day {days_active}/{warmup_required})")
  
    # Determine state transitions
    currently_offline = get_param_bool("system.trading_offline", default=False)
    reason_str = " | ".join(reasons) if is_offline else "NOMINAL"

    # Trigger alerts if state changed
    if is_offline and not currently_offline:
        _insert_alert(conn, "SYSTEM_OFFLINE", "CRITICAL", f"Algorithmic trading suspended. Reasons: {reason_str}")
        log.error("SYSTEM GOING OFFLINE: %s", reason_str)
    elif not is_offline and currently_offline:
        _insert_alert(conn, "SYSTEM_RECOVERY", "INFO", "Risk parameters normalized. Trading algorithms back online.")
        log.info("SYSTEM RECOVERED: Back online.")

    # Save current state to database so the dashboard can read it
    _set_param(conn, "system.trading_offline", "true" if is_offline else "false", "bool")
    _set_param(conn, "system.offline_reason", reason_str, "str")

    return is_offline

def _insert_alert(conn: Any, alert_type: str, severity: str, detail: str) -> None:
    try:
        with conn.cursor() as cur:
            # Note: Depending on your DB schema, you may use SYS_GUID() or another ID generator
            cur.execute("""
                INSERT INTO SYSTEM_ALERTS (ID, TYPE, SEVERITY, DETAIL, TS, RESOLVED)
                VALUES (SYS_GUID(), :at, :sev, :det, SYSTIMESTAMP, 0)
            """, {"at": alert_type, "sev": severity, "det": detail})
        conn.commit()
    except Exception as e:
        log.error("Failed to insert alert: %s", e)

def _set_param(conn: Any, key: str, value: str, dtype: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM PARAMS WHERE PARAM_KEY = :k", {"k": key})
            if cur.fetchone()[0] > 0:
                cur.execute("UPDATE PARAMS SET PARAM_VALUE = :v, LAST_CHANGED_AT = SYSTIMESTAMP WHERE PARAM_KEY = :k", {"v": value, "k": key})
            else:
                cur.execute("INSERT INTO PARAMS (PARAM_KEY, PARAM_VALUE, DTYPE) VALUES (:k, :v, :d)", {"k": key, "v": value, "d": dtype})
        conn.commit()
    except Exception as e:
        log.error("Failed to set param %s: %s", key, e)
