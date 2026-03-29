"""AFD (Area Forecast Discussion) collector.

Spec §4.11: Fetch AFD text from NWS API, extract confidence/disagreement signals.
Used for sigma_mod adjustment in ensemble.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

import requests

from kalshicast.config import HEADERS

log = logging.getLogger(__name__)

NWS_PRODUCTS_URL = "https://api.weather.gov/products/types/AFD/locations"


def _extract_signals(text: str) -> dict:
    """Extract confidence/disagreement signals from AFD text.

    Returns dict with: confidence_flag, model_disagreement_flag,
    directional_note, sigma_multiplier.
    """
    text_lower = text.lower()

    # Confidence flag
    confidence = "NEUTRAL"
    high_conf_words = ["high confidence", "confident", "highly likely", "strong agreement"]
    low_conf_words = ["low confidence", "uncertain", "unclear", "difficult forecast",
                      "challenging", "tricky", "split"]
    if any(w in text_lower for w in high_conf_words):
        confidence = "HIGH"
    elif any(w in text_lower for w in low_conf_words):
        confidence = "LOW"

    # Model disagreement
    disagreement_words = ["model spread", "models disagree", "discrepancy between",
                          "model divergence", "run-to-run", "ensemble spread"]
    model_disagreement = 1 if any(w in text_lower for w in disagreement_words) else 0

    # Directional note
    directional = None
    if any(w in text_lower for w in ["warmer than", "above normal", "higher than"]):
        directional = "WARM_BIAS"
    elif any(w in text_lower for w in ["cooler than", "below normal", "lower than"]):
        directional = "COOL_BIAS"

    # Sigma multiplier: widen spread if low confidence or model disagreement
    sigma_mult = 1.00
    if confidence == "LOW":
        sigma_mult += 0.15
    if model_disagreement:
        sigma_mult += 0.10

    return {
        "confidence_flag": confidence,
        "model_disagreement_flag": model_disagreement,
        "directional_note": directional,
        "sigma_multiplier": round(sigma_mult, 2),
    }


def fetch_afd_discussions(
    wfo_ids: list[str],
    conn: Any,
) -> int:
    """Fetch latest AFD discussions for each WFO and extract signals.

    Returns count of AFDs processed.
    """
    count = 0

    for wfo in wfo_ids:
        try:
            # Get latest AFD product list
            resp = requests.get(
                f"{NWS_PRODUCTS_URL}/{wfo}",
                headers={**HEADERS, "Accept": "application/ld+json"},
                timeout=15,
            )
            resp.raise_for_status()
            products = resp.json().get("@graph", [])

            if not products:
                continue

            # Fetch the latest AFD
            latest = products[0]
            product_url = latest.get("@id", "")
            if not product_url:
                continue

            detail_resp = requests.get(
                product_url,
                headers={**HEADERS, "Accept": "application/ld+json"},
                timeout=15,
            )
            detail_resp.raise_for_status()
            product = detail_resp.json()

            discussion_text = product.get("productText", "")
            issued_str = product.get("issuanceTime", "")

            if not discussion_text:
                continue

            issued_utc = datetime.now(timezone.utc)
            if issued_str:
                try:
                    issued_utc = datetime.fromisoformat(issued_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass

            # Get all stations for this WFO
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT STATION_ID FROM STATIONS
                    WHERE WFO_ID = :wfo AND IS_ACTIVE = 1
                """, {"wfo": wfo})
                station_ids = [row[0] for row in cur]

            # Insert AFD_TEXT
            for sid in station_ids:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO AFD_TEXT (
                            STATION_ID, WFO_ID, ISSUED_UTC, DISCUSSION_TEXT
                        ) VALUES (
                            :sid, :wfo, :issued, :text
                        )
                    """, {
                        "sid": sid,
                        "wfo": wfo,
                        "issued": issued_utc,
                        "text": discussion_text[:4000],  # CLOB limit safety
                    })

                # Extract and insert signals
                signals = _extract_signals(discussion_text)
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO AFD_SIGNALS (
                            STATION_ID, ISSUED_UTC, CONFIDENCE_FLAG,
                            MODEL_DISAGREEMENT_FLAG, DIRECTIONAL_NOTE, SIGMA_MULTIPLIER
                        ) VALUES (
                            :sid, :issued, :conf, :disagree, :dir, :sigma
                        )
                    """, {
                        "sid": sid,
                        "issued": issued_utc,
                        "conf": signals["confidence_flag"],
                        "disagree": signals["model_disagreement_flag"],
                        "dir": signals["directional_note"],
                        "sigma": signals["sigma_multiplier"],
                    })

            conn.commit()
            count += 1
            log.info("[afd] %s: confidence=%s disagree=%d sigma_mult=%.2f",
                     wfo, signals["confidence_flag"],
                     signals["model_disagreement_flag"],
                     signals["sigma_multiplier"])

        except Exception as e:
            log.warning("AFD fetch failed for %s: %s", wfo, e)

    log.info("[afd] processed %d/%d WFO discussions", count, len(wfo_ids))
    return count
