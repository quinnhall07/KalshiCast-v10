"""Synthetic data generators for unit tests.

Generate realistic-looking data without requiring DB connections.
"""

from __future__ import annotations

import math
import random
from datetime import date, timedelta


def generate_station_forecasts(
    n_days: int = 10,
    n_sources: int = 5,
    base_temp: float = 75.0,
    sigma: float = 3.0,
) -> list[dict]:
    """Generate synthetic forecast rows."""
    rows = []
    start = date(2026, 3, 1)
    sources = [f"SRC_{i}" for i in range(n_sources)]

    for d in range(n_days):
        target_date = start + timedelta(days=d)
        for src in sources:
            f_raw = base_temp + random.gauss(0, sigma)
            rows.append({
                "station_id": "KJFK",
                "source_id": src,
                "target_date": target_date.isoformat(),
                "target_type": "HIGH",
                "lead_bracket": "h2",
                "f_raw": round(f_raw, 1),
                "f_adjusted": round(f_raw - 0.5, 1),
                "bss_weight": round(random.uniform(0.05, 0.30), 4),
            })
    return rows


def generate_observations(
    n_days: int = 10,
    base_high: float = 76.0,
    base_low: float = 58.0,
    sigma: float = 2.0,
) -> list[dict]:
    """Generate synthetic observation rows."""
    rows = []
    start = date(2026, 3, 1)

    for d in range(n_days):
        target_date = start + timedelta(days=d)
        high = round(base_high + random.gauss(0, sigma), 1)
        low = round(base_low + random.gauss(0, sigma), 1)
        rows.append({
            "station_id": "KJFK",
            "target_date": target_date.isoformat(),
            "observed_high_f": high,
            "observed_low_f": low,
            "source": "CLI_DAILY",
        })
    return rows


def generate_shadow_book(
    n_bins: int = 5,
    center: float = 75.0,
    bin_width: float = 2.0,
) -> list[dict]:
    """Generate synthetic shadow book bins."""
    bins = []
    start = center - (n_bins * bin_width / 2)

    probs = []
    for i in range(n_bins):
        mid = start + (i + 0.5) * bin_width
        p = math.exp(-0.5 * ((mid - center) / 3.0) ** 2)
        probs.append(p)

    total = sum(probs)
    probs = [p / total for p in probs]

    for i in range(n_bins):
        lower = start + i * bin_width
        upper = lower + bin_width
        bins.append({
            "bin_lower": lower,
            "bin_upper": upper,
            "p_win": round(probs[i], 6),
            "ticker": f"KTEMP-26MAR29-T{int(lower)}to{int(upper)}",
        })
    return bins


def generate_orderbook(depth: int = 5, base_price: int = 40) -> dict:
    """Generate synthetic orderbook."""
    yes_levels = []
    for i in range(depth):
        yes_levels.append({
            "price": base_price + i,
            "quantity": random.randint(10, 100),
        })

    no_levels = []
    for i in range(depth):
        no_levels.append({
            "price": 100 - base_price - depth + i,
            "quantity": random.randint(10, 100),
        })

    return {"yes": yes_levels, "no": no_levels}


def generate_kalman_history(
    n_steps: int = 20,
    b_init: float = 0.0,
    u_init: float = 4.0,
) -> list[dict]:
    """Generate synthetic Kalman history."""
    rows = []
    b_k = b_init
    u_k = u_init
    start = date(2026, 2, 1)

    for i in range(n_steps):
        epsilon = random.gauss(b_k, 1.5)
        R_k = 2.0
        Q_k = 0.01
        u_prior = u_k + Q_k
        k_k = u_prior / (u_prior + R_k)
        b_k = b_k + k_k * (epsilon - b_k)
        u_k = (1 - k_k) * u_prior

        rows.append({
            "history_date": (start + timedelta(days=i)).isoformat(),
            "b_k": round(b_k, 4),
            "u_k": round(u_k, 4),
            "q_k": Q_k,
            "r_k": R_k,
            "k_k": round(k_k, 4),
            "epsilon_k": round(epsilon, 4),
            "state_version": i + 1,
            "is_amendment": False,
        })
    return rows


def generate_brier_scores(
    n: int = 50,
    mean_brier: float = 0.20,
) -> list[dict]:
    """Generate synthetic Brier score rows."""
    rows = []
    start = date(2026, 1, 1)

    for i in range(n):
        bs = max(0.0, min(1.0, random.gauss(mean_brier, 0.05)))
        rows.append({
            "target_date": (start + timedelta(days=i)).isoformat(),
            "station_id": "KJFK",
            "target_type": "HIGH",
            "brier_score": round(bs, 6),
        })
    return rows


def generate_positions(
    n: int = 10,
    bankroll: float = 1000.0,
) -> list[dict]:
    """Generate synthetic position rows."""
    rows = []
    start = date(2026, 3, 1)

    for i in range(n):
        entry_price = round(random.uniform(0.20, 0.80), 2)
        contracts = random.randint(1, 10)
        rows.append({
            "station_id": "KJFK",
            "target_date": (start + timedelta(days=i)).isoformat(),
            "target_type": random.choice(["HIGH", "LOW"]),
            "bin_lower": 72.0,
            "bin_upper": 74.0,
            "ticker": f"KTEMP-TICKER-{i}",
            "entry_price": entry_price,
            "contracts": contracts,
            "order_type": random.choice(["MAKER", "TAKER"]),
            "status": "OPEN",
        })
    return rows
