"""Pipeline orchestration — morning, night, market_open."""

from __future__ import annotations

import logging
from typing import Any

# Pipeline run types
RUN_MORNING = "morning"
RUN_NIGHT = "night"
RUN_MARKET_OPEN = "market_open"

# Pipeline run statuses
STATUS_OK = "OK"
STATUS_PARTIAL = "PARTIAL"
STATUS_ERROR = "ERROR"
STATUS_RUNNING = "RUNNING"

# Position statuses
POS_OPEN = "OPEN"
POS_FILLED = "FILLED"
POS_SETTLED = "SETTLED"

# Target types
TARGET_HIGH = "HIGH"
TARGET_LOW = "LOW"

from kalshicast.config.params_bootstrap import load_db_overrides
from kalshicast.db.connection import init_db, get_conn
from kalshicast.db.schema import ensure_schema, seed_config_tables
from kalshicast.db.operations import (
    new_run_id, insert_pipeline_run, load_all_params,
)

log = logging.getLogger(__name__)


def pipeline_init(run_type: str) -> tuple[str, dict[str, Any]]:
    """Shared init sequence for all pipelines.

    1. init_db + ensure_schema + seed_config_tables
    2. Create pipeline run
    3. Load params + apply DB overrides

    Returns (pipeline_run_id, db_params).
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

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
        insert_pipeline_run(conn, pipeline_run_id, run_type)
        conn.commit()
    finally:
        conn.close()

    conn = get_conn()
    try:
        db_params = load_all_params(conn)
        load_db_overrides(db_params)
    finally:
        conn.close()

    return pipeline_run_id, db_params
