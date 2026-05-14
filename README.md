# KalshiCast

> Quantitative pricing and automated execution pipeline for Kalshi weather contracts.

[![CI](https://img.shields.io/badge/CI-pending-lightgrey)](.github/workflows)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)](https://www.python.org/)
[![Status](https://img.shields.io/badge/status-beta-orange)](#status)

---

## Overview

**KalshiCast** is a Python research and trading system that prices and trades
**weather event contracts** on [Kalshi](https://kalshi.com). It combines numerical
weather prediction (NWP), live station observations, statistical forecasting and a
disciplined execution stack into a single pipeline.

Key features:

- **5-layer pipeline** — L1 collection (METAR / NWS / WeatherAPI / VisualCrossing /
  Tomorrow.io), L2 processing (Kalman filter, regime detection, ensemble weighting),
  L3 pricing (shadow book, skew-normal mixtures, Kalshi bin truncation),
  L4 execution (Kelly sizing, conviction gates, IBE signals, VWAP, order routing,
  risk manager), L5 evaluation (Brier score, BSS matrix, calibration, adverse
  selection).
- **Mixture-of-normals pricing** with skewness-aware bin truncation, dead-zone
  closing and renormalization (see §6.5 of the architecture spec).
- **Kelly sizing with conviction gates** — adaptive `ε_edge`, mutually-exclusive
  Smirnov Kelly, bankroll-fraction risk caps, and IBE signals (KCV, MPDS, HMAS,
  FCT, SCAS).
- **Database-driven parameters** — every tunable lives in an Oracle `params` table;
  Python constants are bootstrap defaults only.
- **Automated scheduling** via GitHub Actions workflows for morning forecasts,
  night settlement, intraday market-open execution, calibration and health checks.

**Audience:** quantitative traders, weather-market researchers and Kalshi
participants who want a reproducible, auditable pricing/execution stack.

## Status

**Beta** — internal research tool that is being prepared for public release.
The pipeline runs against live markets but parameters, dashboards and ops tooling
are still maturing.

> **Not investment advice.** KalshiCast trades real money against a real exchange.
> Run it in paper mode first, read the code, and understand the risks before
> enabling live execution. See [Disclaimer](#disclaimer).

## Architecture

KalshiCast is organised into five layers (collection → processing → pricing →
execution → evaluation) feeding an Oracle Autonomous Database that holds all
state (forecasts, Kalman state, params, shadow book, orders, fills, metrics).
Three pipeline orchestrators (`morning`, `night`, `market_open`) chain the
layers together; supporting commands handle schema, health, calibration and
backtesting.

For the full design — formulas, gates, schemas and issue-resolution log — see
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Requirements

- **Python** 3.11 or 3.12
- **Oracle Autonomous Database** (free tier works) with wallet credentials, or a
  compatible Oracle 19c+ instance
- **Kalshi API** credentials (`KALSHI_KEY_ID` + RSA private key)
- **Weather API keys** — at least one of:
  - [WeatherAPI](https://www.weatherapi.com/)
  - [Visual Crossing](https://www.visualcrossing.com/)
  - [Tomorrow.io](https://www.tomorrow.io/)
- Optional: a CI environment (GitHub Actions) for scheduled pipeline runs

## Quick Start

```bash
# 1. Clone
git clone https://github.com/<owner>/Kalshicast-v10.git
cd Kalshicast-v10

# 2. Install
pip install -e ".[dev]"
pre-commit install

# 3. Configure secrets
cp .env.example .env
# ...then edit .env and fill in Oracle, Kalshi and weather API credentials

# 4. Bootstrap database schema
python -m kalshicast schema

# 5. Run the morning pipeline (paper mode — no live orders)
python -m kalshicast morning
```

For a step-by-step walkthrough including troubleshooting, see
[`docs/QUICKSTART.md`](docs/QUICKSTART.md).

## CLI Commands

KalshiCast exposes a single entry point: `python -m kalshicast <command>`.

| Command        | Purpose                                                       |
|----------------|---------------------------------------------------------------|
| `schema`       | Create / migrate database tables and seed config              |
| `morning`      | Collect forecasts, run L1+L2, build shadow book               |
| `night`        | Settle contracts, update Kalman state, recompute metrics      |
| `market_open`  | Intraday execution: price, gate, size and route orders        |
| `observations` | Pull live METAR/observation data for active stations          |
| `health`       | Diagnostics — DB, missed runs, METAR freshness, MDD, alerts   |
| `rollover`     | Roll positions / contracts at end-of-day                      |
| `calibrate`    | Re-tune params from realized BSS / fill quality / drawdowns   |
| `backtest`     | Replay historical data through the pipeline                   |

Full reference: [`docs/CLI.md`](docs/CLI.md).

## Project Layout

```
Kalshicast-v10/
├── kalshicast/           # main package
│   ├── collection/       # L1 — METAR, NWS, weather APIs, AFD text
│   ├── processing/       # L2 — Kalman filter, ensembles, regimes
│   ├── pricing/          # L3 — shadow book, mixtures, bin truncation
│   ├── execution/        # L4 — Kelly, gates, IBE, VWAP, orders, risk
│   ├── evaluation/       # L5 — Brier, BSS matrix, calibration
│   ├── pipeline/         # orchestrators: morning, night, market_open, ...
│   ├── db/               # Oracle connection pool, schema, migrations
│   ├── ml_v1/            # experimental ML scaffolding
│   ├── config/           # stations, params loader
│   └── tests/            # pytest suite
├── docs/                 # architecture spec, quickstart, CLI reference
├── scripts/              # one-off ops scripts
├── .github/workflows/    # morning / night / market_open / health / calibration
├── pyproject.toml
└── README.md
```

## Development

```bash
pip install -e ".[dev]"
pre-commit install
pytest
```

CI runs `pytest` plus pre-commit hooks on every push. Before opening a PR, please
read [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — full design specification
  (5-layer pipeline, formulas, schemas, issue-resolution log).
- [`docs/QUICKSTART.md`](docs/QUICKSTART.md) — first-run guide.
- [`docs/CLI.md`](docs/CLI.md) — per-command reference.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — dev workflow and code style.
- [`SECURITY.md`](SECURITY.md) — vulnerability reporting policy.

## Disclaimer

KalshiCast is provided **for research and educational purposes**. It is **not
financial advice**. Trading prediction-market contracts involves substantial
risk of loss, including total loss of capital. The authors and contributors make
no representations about the accuracy, profitability or fitness of this software
for any purpose.

**Use paper-trading mode first.** Read the code, review the architecture spec,
understand every gate and risk control, and trade at your own risk.

## License

[MIT](LICENSE) © KalshiCast contributors.

## Acknowledgments

Built on the shoulders of the open-source NWP / statistics ecosystem
(`numpy`, `scipy`, `pandas`, `statsmodels`, `oracledb`) and the public data
feeds provided by the National Weather Service, Aviation Weather Center,
WeatherAPI, Visual Crossing and Tomorrow.io.
