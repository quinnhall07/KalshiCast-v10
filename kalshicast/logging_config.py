"""Centralized logging configuration for KalshiCast.

Use ``setup_logging()`` from any entrypoint instead of calling
``logging.basicConfig`` directly. This ensures a consistent format
across pipelines and prevents duplicate handler installation.
"""
from __future__ import annotations

import logging
import os
import sys

DEFAULT_FORMAT = "%(asctime)s %(levelname)-7s %(name)s :: %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%dT%H:%M:%SZ"


def setup_logging(level: int | None = None, *, force: bool = False) -> None:
    """Configure root logger once for the process.

    Honors the ``KALSHICAST_LOG_LEVEL`` env var (DEBUG/INFO/WARN/ERROR) when
    ``level`` is None. ``force=True`` reinstalls handlers even if previously
    configured — use in entrypoints, not library code.
    """
    resolved = level if level is not None else _level_from_env()
    logging.basicConfig(
        level=resolved,
        format=DEFAULT_FORMAT,
        datefmt=DEFAULT_DATEFMT,
        stream=sys.stdout,
        force=force,
    )


def _level_from_env() -> int:
    raw = os.getenv("KALSHICAST_LOG_LEVEL", "INFO").upper().strip()
    return getattr(logging, raw, logging.INFO)
