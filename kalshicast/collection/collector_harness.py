"""Shared retry, throttling, and validation logic for collectors.

Extracted from morning.py. Provider semaphores prevent API rate limit violations.
"""

from __future__ import annotations

import logging
import random
import threading
from typing import Any, Callable, Dict

import requests

from kalshicast.config.params_bootstrap import get_param_int, get_param_float

log = logging.getLogger(__name__)

# Provider semaphores — initialized lazily from params
_semaphores: dict[str, threading.Semaphore] | None = None


def _get_semaphores() -> dict[str, threading.Semaphore]:
    global _semaphores
    if _semaphores is None:
        _semaphores = {
            "TOM": threading.Semaphore(get_param_int("collection.tom_concurrency")),
            "WAPI": threading.Semaphore(get_param_int("collection.wapi_concurrency")),
            "VCR": threading.Semaphore(get_param_int("collection.vcr_concurrency")),
            "NWS": threading.Semaphore(get_param_int("collection.nws_concurrency")),
            "OME": threading.Semaphore(get_param_int("collection.ome_concurrency")),
        }
    return _semaphores


def get_semaphore(provider_group: str) -> threading.Semaphore:
    """Get the rate-limiting semaphore for a provider group."""
    sems = _get_semaphores()
    return sems.get(provider_group, threading.Semaphore(4))


def is_retryable_error(e: Exception) -> bool:
    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(e, requests.HTTPError):
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
        if code is None:
            return True
        return code == 429 or code >= 500
    return False


def sleep_jittered_exponential(attempt: int, *, cap_s: float = 30.0) -> float:
    """Compute jittered exponential backoff sleep duration."""
    base = get_param_float("collection.base_sleep_seconds")
    exp = base * (2 ** (attempt - 1))
    exp = min(cap_s, exp)
    duration = random.random() * exp
    return duration


def call_with_retry(
    fetcher: Callable[..., Dict[str, Any]],
    station: dict,
    source_id: str,
    provider_group: str,
    *,
    max_attempts: int | None = None,
) -> Dict[str, Any] | None:
    """Call a fetcher with retry and provider semaphore.

    Returns the payload dict on success, None on exhausted retries.
    """
    if max_attempts is None:
        max_attempts = get_param_int("collection.max_retry_attempts")

    sem = get_semaphore(provider_group)
    station_id = station.get("station_id", "???")

    for attempt in range(1, max_attempts + 1):
        sem.acquire()
        try:
            payload = fetcher(station)
            return payload
        except Exception as e:
            if attempt < max_attempts and is_retryable_error(e):
                sleep_s = sleep_jittered_exponential(attempt)
                log.warning(
                    "[%s/%s] attempt %d/%d failed (%s), retrying in %.1fs",
                    source_id, station_id, attempt, max_attempts, e, sleep_s,
                )
                import time
                time.sleep(sleep_s)
            else:
                log.error(
                    "[%s/%s] failed after %d attempts: %s",
                    source_id, station_id, attempt, e,
                )
                return None
        finally:
            sem.release()

    return None
