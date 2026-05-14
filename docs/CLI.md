# KalshiCast CLI Reference

KalshiCast exposes a single entry point:

```bash
python -m kalshicast <command> [args...]
```

The dispatcher is `kalshicast/__main__.py`. All commands read the same `.env`
(Oracle + Kalshi + weather API credentials).

The nine commands are:

| Command        | Layer      | Typical schedule (UTC)         |
|----------------|------------|--------------------------------|
| `schema`       | DB         | Manual / on deploy             |
| `morning`      | L1+L2+L3   | Daily, ~12:00 UTC              |
| `night`        | L1+L2+L5   | Daily, ~04:00 UTC              |
| `market_open`  | L4         | Intraday (hourly during hours) |
| `observations` | L1         | Intraday or ad-hoc             |
| `health`       | Ops        | Every 1–6 hours                |
| `rollover`     | L4/L5      | Daily at contract rollover     |
| `calibrate`    | L5         | Weekly                         |
| `backtest`     | Research   | Manual                         |

Common environment variables read by every command:
`ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`, `TNS_ADMIN`. Command-specific
variables are noted below.

---

## `schema`

**Synopsis:** `python -m kalshicast schema`

**Does:** Connects to the Oracle database, calls `ensure_schema()` to create any
missing tables (idempotent — existing tables are skipped), then calls
`seed_config_tables()` to populate `params` and `stations` with bootstrap
defaults.

**Env:** Oracle credentials only.

**Example:**

```bash
python -m kalshicast schema
# → Created 23 tables: params, stations, forecasts, ...
# → Config tables seeded.
```

**Schedule:** Manual — run on first deploy and after any pull that adds tables
or seed rows.

---

## `morning`

**Synopsis:** `python -m kalshicast morning`

**Does:** Daily forecast collection and pricing.
1. L1 — fetch forecasts from WeatherAPI / Visual Crossing / Tomorrow.io / NWS.
2. L1 — pull AFD text and METAR snapshots for active stations.
3. L2 — run the Kalman filter, regime detector and ensemble weighter.
4. L3 — build the shadow book (mixture-of-normals → bin truncation).

**Env:** Oracle + `WEATHERAPI_KEY`, `VISUALCROSSING_KEY`, `TOMORROW_API_KEY`.

**Example:** `python -m kalshicast morning`

**Schedule:** Once per day, well before market open (cron: `0 12 * * *`).
Triggered by `.github/workflows/morning.yml`.

---

## `night`

**Synopsis:** `python -m kalshicast night`

**Does:** End-of-day settlement and bookkeeping.
1. Re-collect late observations (CLI amendment window).
2. Settle contracts whose markets have closed.
3. Update Kalman state from realized observations.
4. L5 — recompute Brier / BSS / calibration metrics, adverse-selection deltas.

**Env:** Oracle + Kalshi (`KALSHI_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`).

**Example:** `python -m kalshicast night`

**Schedule:** Once per day, after settlement (cron: `0 4 * * *`).
Triggered by `.github/workflows/night.yml`.

---

## `market_open`

**Synopsis:** `python -m kalshicast market_open [--live]`

**Does:** Intraday execution loop.
1. Re-price against fresh METAR / live forecast.
2. Apply conviction gates (Skill, Spread, Edge, Liquidity, Drawdown).
3. Compute mutually-exclusive Kelly sizes (Smirnov 1973).
4. Run IBE signals (KCV, MPDS, HMAS, FCT, SCAS) — veto or scale.
5. Route orders (limit / VWAP-tranched) and reconcile fills.

Without `--live`, runs in paper mode (no orders submitted).

**Env:** Oracle + Kalshi creds.

**Example:** `python -m kalshicast market_open --live`

**Schedule:** Hourly (or finer) during market hours. Triggered by
`.github/workflows/market_open.yml`.

---

## `observations`

**Synopsis:** `python -m kalshicast observations`

**Does:** Fetches the latest METAR / live-weather observations for every active
station in `config.stations` and writes them to the observations table. Used to
seed truncation reference values for L3.

**Env:** Oracle + weather API keys.

**Example:** `python -m kalshicast observations`

**Schedule:** Intraday, or ad-hoc when METAR data is stale.

---

## `health`

**Synopsis:** `python -m kalshicast health`

**Does:** Prints a human-readable diagnostics report covering DB connectivity,
missed pipeline runs (vs. expected schedule), METAR freshness per station,
max-drawdown (all-time and 90-day), and unresolved alerts.

**Env:** Oracle credentials only.

**Example:** `python -m kalshicast health`

**Schedule:** Every 1–6 hours. Triggered by `.github/workflows/health.yml`.

---

## `rollover`

**Synopsis:** `python -m kalshicast rollover`

**Does:** Performs end-of-day position and contract rollover — closes expiring
contracts, transfers state to next-day series and emits a JSON summary.

**Env:** Oracle + Kalshi creds.

**Example:** `python -m kalshicast rollover`

**Schedule:** Daily at contract expiration boundary.

---

## `calibrate`

**Synopsis:** `python -m kalshicast calibrate`

**Does:** Re-tunes parameters in the `params` table from realized data
(BSS, fill quality, drawdown, adverse-selection delta). Emits the list of
updated keys and their new values.

**Env:** Oracle credentials only.

**Example:** `python -m kalshicast calibrate`

**Schedule:** Weekly. Triggered by `.github/workflows/calibration.yml`.

---

## `backtest`

**Synopsis:** `python -m kalshicast backtest`

**Does:** Replays historical forecasts and observations through the pipeline
without any network or broker side effects. Produces Brier / BSS / PnL summary
artifacts for offline analysis.

**Env:** Oracle credentials (read-only is sufficient).

**Example:** `python -m kalshicast backtest`

**Schedule:** Manual / research-only.
