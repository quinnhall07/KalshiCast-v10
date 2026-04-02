"""One-time schema migration: add IS_PAPER column to POSITIONS.

Run this once against your Oracle DB before deploying paper_sim.py.
The ensure_schema() function in schema.py also calls this safely on startup.

Usage (standalone):
    python -m kalshicast.db.migrations.add_is_paper
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

# These run inside ensure_schema() — each must be idempotent (catches ORA-01430)
MIGRATIONS: list[tuple[str, str]] = [
    (
        "POSITIONS.IS_PAPER",
        "ALTER TABLE POSITIONS ADD (IS_PAPER NUMBER(1) DEFAULT 0 NOT NULL)",
    ),
]


def run_migrations(conn: Any) -> list[str]:
    """Apply any pending column migrations. Returns list of applied migration names."""
    applied = []
    for name, sql in MIGRATIONS:
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            applied.append(name)
            log.info("[migration] applied: %s", name)
        except Exception as e:
            # ORA-01430: column already exists → skip silently
            # ORA-01735: invalid ALTER TABLE option (already not null) → skip
            err = str(e).upper()
            if "ORA-01430" in err or "ORA-00957" in err:
                log.debug("[migration] skipped (already exists): %s", name)
            else:
                log.warning("[migration] %s failed: %s", name, e)
    return applied


# ── Standalone entry point ────────────────────────────────────────────────────
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
