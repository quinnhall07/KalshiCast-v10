# KalshiCast Quick Start

This guide walks you from a clean checkout to a first paper-mode run of the
KalshiCast pipeline.

## 1. Prerequisites

- **Python 3.11 or 3.12** (`python --version`)
- **Git**
- **Oracle Autonomous Database** account (the always-free tier is sufficient) —
  you will need the wallet zip and the connection DSN.
- **Kalshi API credentials**:
  - `KALSHI_KEY_ID` (API key ID)
  - An RSA private key file (path goes into `KALSHI_PRIVATE_KEY_PATH`)
- **Weather API keys** — at least one of:
  - WeatherAPI — https://www.weatherapi.com/
  - Visual Crossing — https://www.visualcrossing.com/
  - Tomorrow.io — https://www.tomorrow.io/

## 2. Install

```bash
git clone https://github.com/<owner>/Kalshicast-v10.git
cd Kalshicast-v10

# Editable install with dev extras (pytest, pre-commit, ruff, etc.)
pip install -e ".[dev]"

# Install git hooks (ruff, format checks)
pre-commit install
```

If you use a virtualenv manager (`venv`, `uv`, `poetry`), activate it before
the install step.

## 3. Configure

Copy the example env file and fill in real credentials:

```bash
cp .env.example .env
```

`.env` keys:

| Key                       | What it is                                                  |
|---------------------------|-------------------------------------------------------------|
| `ORACLE_USER`             | DB user (default `ADMIN`)                                   |
| `ORACLE_PASSWORD`         | DB password                                                 |
| `ORACLE_DSN`              | Oracle connection DSN (e.g. `mydb_high`)                    |
| `WEATHERAPI_KEY`          | WeatherAPI access key                                       |
| `VISUALCROSSING_KEY`      | Visual Crossing access key                                  |
| `TOMORROW_API_KEY`        | Tomorrow.io access key                                      |
| `KALSHI_KEY_ID`           | Kalshi API key ID                                           |
| `KALSHI_PRIVATE_KEY_PATH` | Filesystem path to the RSA private key PEM                  |
| `KALSHICAST_LOG_LEVEL`    | Optional: `DEBUG` / `INFO` / `WARNING` / `ERROR`            |

For the Oracle wallet, extract the zip somewhere safe and point the standard
`TNS_ADMIN` environment variable at that directory (e.g. add it to `.env` or
your shell profile).

## 4. Bootstrap the schema

```bash
python -m kalshicast schema
```

This creates all required tables and seeds the `params` / `stations` config
tables with their bootstrap defaults. Re-running is safe — existing tables
are skipped.

## 5. First run — paper mode

By default no live orders are sent. Start with the morning collection pass:

```bash
python -m kalshicast morning
```

Then the night settlement / Kalman update:

```bash
python -m kalshicast night
```

These two together exercise L1 through L3 (collection, processing, pricing)
without touching the broker.

## 6. Verify

```bash
python -m kalshicast health
```

You should see a "HEALTHY" report with the database green, no missed runs and
fresh METAR readings.

## 7. Common errors

- **`ORA-12154` / `DPY-6005`** — Oracle DSN unreachable. Check that
  `TNS_ADMIN` points at the wallet directory and `ORACLE_DSN` matches a
  service in `tnsnames.ora`.
- **`Permission denied` on private key** — `chmod 600` on the Kalshi PEM and
  make sure `KALSHI_PRIVATE_KEY_PATH` is an absolute path.
- **`429 Too Many Requests` from weather APIs** — the collectors back off
  automatically; lower the active station count in `config.stations` while you
  test, or upgrade your API plan.
- **"No station data" / empty METAR table** — first run `python -m kalshicast
  observations` to seed the live observation table before the morning pass.
- **Schema mismatch after a pull** — re-run `python -m kalshicast schema`.

## 8. Next steps

- Run `python -m kalshicast backtest` to replay historical data through the
  pipeline and inspect Brier / BSS metrics.
- Run `python -m kalshicast calibrate` after you have at least a few weeks of
  realized data — it re-tunes parameters in the `params` table.
- Enable the GitHub Actions workflows in `.github/workflows/` (morning, night,
  market_open, health, calibration) once you are comfortable with manual runs.
- Read [`ARCHITECTURE.md`](ARCHITECTURE.md) end-to-end before flipping any
  live-execution switches.
