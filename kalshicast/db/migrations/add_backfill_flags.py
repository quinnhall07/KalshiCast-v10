# kalshicast/db/migrations/add_backfill_flags.py
"""One-time schema migration: add IS_BACKFILL and LEAD_HOURS_APPROX flags.

IS_BACKFILL = 1 on FORECAST_RUNS and OBSERVATIONS rows loaded by the backfill
pipeline. Live pipelines always write 0 (the DEFAULT). Existing rows get 0
automatically — Oracle applies the DEFAULT on ALTER TABLE ADD, so no UPDATE needed.

LEAD_HOURS_APPROX = 1 on FORECAST_ERRORS rows where the lead time was
reconstructed from the backfill date window rather than a true issued_at
timestamp. Used for optional BSS down-weighting.
"""
from __future__ import annotations
import logging
from typing import Any

log = logging.getLogger(__name__)

# Each tuple: (migration_name, DDL). Names are checked against ORA-01430.
MIGRATIONS: list[tuple[str, str]] = [
    (
        "FORECAST_RUNS.IS_BACKFILL",
        "ALTER TABLE FORECAST_RUNS ADD (IS_BACKFILL NUMBER(1) DEFAULT 0)",
    ),
    (
        "OBSERVATIONS.IS_BACKFILL",
        "ALTER TABLE OBSERVATIONS ADD (IS_BACKFILL NUMBER(1) DEFAULT 0)",
    ),
    (
        "FORECAST_ERRORS.LEAD_HOURS_APPROX",
        "ALTER TABLE FORECAST_ERRORS ADD (LEAD_HOURS_APPROX NUMBER(1) DEFAULT 0)",
    ),
]


def run_migrations(conn: Any) -> list[str]:
    """Apply pending flag columns. Returns list of applied migration names."""
    applied: list[str] = []
    for name, sql in MIGRATIONS:
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            applied.append(name)
            log.info("[backfill_migration] applied: %s", name)
        except Exception as e:
            err = str(e).upper()
            if "ORA-01430" in err or "ORA-00957" in err:
                log.debug("[backfill_migration] already exists, skipping: %s", name)
            else:
                log.warning("[backfill_migration] %s failed: %s", name, e)
    return applied


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from kalshicast.db.connection import init_db, get_conn, close_pool
    init_db()
    conn = get_conn()
    try:
        applied = run_migrations(conn)
        print(f"Applied {len(applied)} migration(s): {applied}")
    finally:
        conn.close()
        close_pool()