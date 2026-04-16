"""Kalshi REST API client with RSA-PSS authentication.

Spec §9.3 steps 3, 8, 10: Fetch balance, order books, submit orders.
Ported from old sync_bins.py auth headers + extended to full client.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from typing import Any
from urllib.parse import urlparse

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from kalshicast.config.params_bootstrap import get_param_int, get_param_float

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


class KalshiClient:
    """Kalshi REST API client with RSA-PSS request signing."""

    def __init__(
        self,
        api_key_id: str | None = None,
        private_key_pem: str | None = None,
        base_url: str | None = None,
    ) -> None:
        self.api_key_id = api_key_id or os.environ.get("KALSHI_KEY_ID", "")
        pem = private_key_pem or os.environ.get("KALSHI_PRIVATE_KEY", "")
        self.base_url = (base_url or os.environ.get("KALSHI_API_BASE", "")).rstrip("/") or DEFAULT_BASE_URL

        self._private_key = None
        if pem:
            self._private_key = serialization.load_pem_private_key(
                pem.encode("utf-8"), password=None,
            )

        self._session = requests.Session()
        self._session.headers["Content-Type"] = "application/json"

    def get_events(self, *, status: str = "open", series_ticker: str | None = None,
                   limit: int = 100) -> list[dict]:
        """GET /events — fetch events with optional filtering.
        
        Args:
            status: Filter by status (open, closed, settled)
            series_ticker: Filter by series (e.g., KXHIGH, KXLOW)
            limit: Max results to return
        
        Returns:
            List of event dicts with nested markets
        """
        params: dict[str, Any] = {
            "limit": limit,
            "with_nested_markets": "true",
        }
        if status:
            params["status"] = status
        if series_ticker:
            params["series_ticker"] = series_ticker
        
        data = self._request("GET", "/events", params=params)
        return data.get("events", [])

    # ── Authentication ───────────────────────────────────────────────

    def _sign_headers(self, method: str, path: str) -> dict[str, str]:
        """Build RSA-PSS signed authentication headers."""
        if not self.api_key_id or not self._private_key:
            return {}

        timestamp = str(int(time.time() * 1000))
        msg_string = f"{timestamp}{method.upper()}{path}"
        signature = self._private_key.sign(
            msg_string.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        }

    # ── Generic request ──────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
        retry_max: int | None = None,
        backoff_sec: float | None = None,
    ) -> dict[str, Any]:
        """Execute authenticated API request with retry."""
        if retry_max is None:
            retry_max = get_param_int("order.retry_max")
        if backoff_sec is None:
            backoff_sec = get_param_float("order.retry_backoff_sec")

        url = f"{self.base_url}{path}"
        parsed_path = urlparse(url).path

        last_exc: Exception | None = None
        for attempt in range(1, retry_max + 1):
            auth_headers = self._sign_headers(method, parsed_path)
            try:
                resp = self._session.request(
                    method, url,
                    params=params,
                    json=json_body,
                    headers=auth_headers,
                    timeout=30,
                )
                resp.raise_for_status()
                return resp.json() if resp.text else {}
            except requests.HTTPError as e:
                last_exc = e
                status = getattr(e.response, "status_code", 0)
                if status == 429 or status >= 500:
                    wait = backoff_sec * (2 ** (attempt - 1))
                    log.warning("Kalshi API %s %s: %s (attempt %d/%d, retry in %.1fs)",
                                method, path, e, attempt, retry_max, wait)
                    time.sleep(wait)
                    continue
                raise
            except (requests.Timeout, requests.ConnectionError) as e:
                last_exc = e
                wait = backoff_sec * (2 ** (attempt - 1))
                log.warning("Kalshi API %s %s: %s (attempt %d/%d, retry in %.1fs)",
                            method, path, e, attempt, retry_max, wait)
                time.sleep(wait)

        raise RuntimeError(f"Kalshi API {method} {path} failed after {retry_max} attempts") from last_exc

    # ── Portfolio ─────────────────────────────────────────────────────

    def get_balance(self) -> float:
        """GET /portfolio/balance → bankroll in dollars."""
        data = self._request("GET", "/portfolio/balance")
        # Kalshi returns balance in cents
        return data.get("balance", 0) / 100.0

    # ── Markets ───────────────────────────────────────────────────────

    def get_orderbook(self, ticker: str, *, depth: int | None = None) -> dict:
        """GET /markets/{ticker}/orderbook."""
        if depth is None:
            depth = get_param_int("vwap.depth_levels")
        return self._request("GET", f"/markets/{ticker}/orderbook", params={"depth": depth})

    # ── Orders ────────────────────────────────────────────────────────

    def submit_order(
        self,
        ticker: str,
        *,
        side: str = "buy",
        order_type: str = "limit",
        limit_price: float,
        quantity: int,
        client_order_id: str | None = None,
        expiration_type: str = "immediate-or-cancel",
    ) -> dict:
        """POST /portfolio/orders → submit a new order."""
        body: dict[str, Any] = {
            "ticker": ticker,
            "side": side,
            "type": order_type,
            "count": quantity,
            "yes_price": int(limit_price * 100),  # Kalshi uses integer cents
        }
        if client_order_id:
            body["client_order_id"] = client_order_id
        if expiration_type:
            body["expiration_ts"] = None  # Let Kalshi handle expiration
            body["action"] = "buy"

        return self._request("POST", "/portfolio/orders", json_body=body)

    def cancel_order(self, order_id: str) -> dict:
        """DELETE /portfolio/orders/{order_id}."""
        return self._request("DELETE", f"/portfolio/orders/{order_id}")

    def get_positions(self, *, limit: int = 200) -> list[dict]:
        """GET /portfolio/positions."""
        data = self._request("GET", "/portfolio/positions", params={"limit": limit})
        return data.get("market_positions", [])
