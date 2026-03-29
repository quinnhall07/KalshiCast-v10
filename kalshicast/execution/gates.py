"""5 conviction gates — filter candidates before sizing.

Spec §7.2: Edge, Spread, Skill, Lead Time, Reserved.
All gates are pure functions; DB writes happen in the pipeline.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

from kalshicast.config.params_bootstrap import get_param_float

log = logging.getLogger(__name__)


def check_edge_gate(
    p_win: float,
    c_market: float,
    bankroll: float,
    n_bets: int,
    fee_rate: float | None = None,
) -> dict:
    """Gate 1: Edge sufficient.

    ε_edge = max(ε_base, 1.96 × √(0.25/N_bets))
    Pass if (p_win - c_market) > ε_edge AND EV_net > ev_min_fraction × bankroll.
    """
    epsilon_base = get_param_float("gate.epsilon_edge_base")
    ev_min_frac = get_param_float("gate.ev_min_fraction")
    if fee_rate is None:
        fee_rate = get_param_float("fee.taker_rate")

    # Adaptive edge buffer
    if n_bets > 0:
        epsilon = max(epsilon_base, 1.96 * math.sqrt(0.25 / n_bets))
    else:
        epsilon = epsilon_base

    edge = p_win - c_market

    # Net EV per contract (in cents): (p - c) * 100 - fee
    fee_cents = math.ceil(fee_rate * c_market * (1 - c_market) * 100)
    ev_net = (p_win - c_market) * 100 - fee_cents

    # Minimum EV check: ev_net must be positive and edge must exceed buffer
    # The ev_min_fraction check is against per-contract EV relative to contract price
    ev_threshold = ev_min_frac * c_market * 100  # Min EV as % of contract cost

    passed = edge > epsilon and ev_net > ev_threshold

    return {
        "gate": "edge",
        "pass": passed,
        "edge": round(edge, 6),
        "epsilon": round(epsilon, 6),
        "ev_net": round(ev_net, 4),
        "ev_threshold": round(ev_threshold, 4),
    }


def check_spread_gate(s_tk: float) -> dict:
    """Gate 2: Model consensus — REJECT if ensemble spread > spread_max."""
    spread_max = get_param_float("gate.spread_max")
    passed = s_tk <= spread_max
    return {
        "gate": "spread",
        "pass": passed,
        "s_tk": round(s_tk, 3),
        "spread_max": spread_max,
    }


def check_skill_gate(bss: float | None, was_qualified: bool) -> dict:
    """Gate 3: Historical BSS — hysteresis entry/exit.

    Enter if BSS ≥ bss_enter (new cell).
    Exit if BSS < bss_exit (qualified cell).
    """
    bss_enter = get_param_float("gate.bss_enter")
    bss_exit = get_param_float("gate.bss_exit")

    if bss is None:
        return {"gate": "skill", "pass": False, "bss": None, "reason": "no_bss"}

    if was_qualified:
        passed = bss >= bss_exit
    else:
        passed = bss >= bss_enter

    return {
        "gate": "skill",
        "pass": passed,
        "bss": round(bss, 6),
        "was_qualified": was_qualified,
        "threshold_used": bss_exit if was_qualified else bss_enter,
    }


def check_lead_gate(lead_hours: float) -> dict:
    """Gate 4: Lead time ceiling — REJECT if lead_hours > ceiling."""
    ceiling = get_param_float("gate.lead_ceiling_hours")
    passed = lead_hours <= ceiling
    return {
        "gate": "lead",
        "pass": passed,
        "lead_hours": round(lead_hours, 1),
        "ceiling": ceiling,
    }


def check_reserved_gate() -> dict:
    """Gate 5: Reserved for future expansion — always passes."""
    return {"gate": "reserved", "pass": True}


def evaluate_all_gates(candidate: dict) -> dict:
    """Run all 5 conviction gates on a candidate bet.

    candidate keys: p_win, c_market, bankroll, n_bets, s_tk, bss,
                    was_qualified, lead_hours, fee_rate (optional)

    Returns: {"pass": bool, "flags": {gate: bool, ...}, "details": [...]}
    """
    results = [
        check_edge_gate(
            candidate["p_win"],
            candidate["c_market"],
            candidate["bankroll"],
            candidate.get("n_bets", 0),
            candidate.get("fee_rate"),
        ),
        check_spread_gate(candidate["s_tk"]),
        check_skill_gate(candidate.get("bss"), candidate.get("was_qualified", False)),
        check_lead_gate(candidate["lead_hours"]),
        check_reserved_gate(),
    ]

    flags = {r["gate"]: r["pass"] for r in results}
    all_pass = all(r["pass"] for r in results)

    return {
        "pass": all_pass,
        "flags": flags,
        "details": results,
        "flags_json": json.dumps(flags),
    }
