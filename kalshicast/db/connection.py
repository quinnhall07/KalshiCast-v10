"""Oracle Autonomous Database connection pool (thin mode — no Oracle Client needed).

Required env vars:
  ORACLE_USER     — database username (e.g. ADMIN)
  ORACLE_PASSWORD — database password
  ORACLE_DSN      — connection string
"""

from __future__ import annotations

import logging
import time
import os
from datetime import datetime, timezone
from typing import Any

import oracledb

logger = logging.getLogger(__name__)

_pool: oracledb.ConnectionPool | None = None

# SYS_GUID() formatted as lowercase dashed UUID
GUID_EXPR = (
    "(SELECT LOWER("
    "  SUBSTR(g, 1, 8) || '-' ||"
    "  SUBSTR(g, 9, 4) || '-' ||"
    "  SUBSTR(g, 13, 4) || '-' ||"
    "  SUBSTR(g, 17, 4) || '-' ||"
    "  SUBSTR(g, 21)"
    ") FROM (SELECT RAWTOHEX(SYS_GUID()) g FROM DUAL))"
)


def to_dt(iso: str) -> datetime:
    """Convert ISO-8601 string to timezone-aware UTC datetime.

    oracledb binds datetime objects natively as TIMESTAMP WITH TIME ZONE,
    eliminating ORA-01821 format mask issues.
    """
    s = str(iso).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    s = s.replace(" ", "T", 1)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _oracle_params() -> dict[str, Any]:
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("ORACLE_DSN")
    if not (user and password and dsn):
        raise RuntimeError(
            "Missing Oracle env vars: ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN"
        )
    params: dict[str, Any] = {"user": user, "password": password, "dsn": dsn}
    wallet_dir = os.getenv("ORACLE_WALLET_DIR")
    if wallet_dir:
        params["config_dir"] = wallet_dir
        params["wallet_location"] = wallet_dir
        params["wallet_password"] = password
    return params


def _ensure_pool() -> oracledb.ConnectionPool:
    global _pool
    if _pool is None:
        params = _oracle_params()
        _pool = oracledb.create_pool(
            **params,
            min=1,
            max=4,
            increment=1,
        )
    return _pool


def get_conn(*, retries: int = 3, backoff: float = 2.0) -> oracledb.Connection:
    """Acquire a connection from the pool with retry on transient failures."""
    for attempt in range(1, retries + 1):
        try:
            pool = _ensure_pool()
            conn = pool.acquire()
            conn.autocommit = False
            return conn
        except oracledb.Error as exc:
            if attempt == retries:
                raise
            wait = backoff ** attempt
            logger.warning(
                "DB connection attempt %d/%d failed (%s), retrying in %.0fs…",
                attempt, retries, exc, wait,
            )
            # Reset the pool so the next attempt creates a fresh one
            close_pool()
            time.sleep(wait)
    raise RuntimeError("unreachable")  # satisfies type checker


def close_pool() -> None:
    """Shut down the connection pool."""
    global _pool
    if _pool is not None:
        _pool.close(force=True)
        _pool = None


def init_db() -> None:
    """Create pool and smoke-test the connection."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM DUAL")
    finally:
        conn.close()
