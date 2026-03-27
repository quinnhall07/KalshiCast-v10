# KALSHICAST — Quantitative Scoring System

## Version 10.0

**March 2026 — Confidential Internal Document**

---

# Section 0: Changelog and Resolution Log

## Changelog from v9

| Change | Rationale |
|--------|-----------|
| **Entropy-regularized ensemble weighting replaces simple w_m_min floor** | The w_m_min = 0.05/M floor from v9 prevents zero-weight models but does not address the BSS feedback loop that drives weight concentration. Entropy regularization actively penalizes concentrated weight distributions. See Section 5. |
| **Decoupled Location-Scale architecture retained from v9** | Top-model selection for Kalman input confirmed as superior to weighted ensemble mean. No change to the fundamental location/scale separation. |
| **Formal mutually-exclusive Kelly with worked 3-bin example** | v9 specified the Smirnov (1973) algorithm but deferred the numeric example. v10 includes a complete worked example using a 3-bin temperature market. See Section 7. |
| **classify_lead_hours() function fully specified** | v9 used lead time brackets h1–h5 without defining the exact mapping function from continuous float hours. v10 provides the complete function. See Section 5. |
| **Zero-variance G₁ fallback explicitly formalized** | v9 stated the fallback in prose. v10 states it as a formal conditional at the start of the skewness computation. See Section 5. |
| **VWAP staleness detection with delta test and tranche execution** | v9 identified the static-book problem. v10 specifies the detection formula, alert threshold, and tranche execution rule for orders >50 contracts. See Section 7. |
| **Fee partial-fill: conservative assumption stated** | v10 mandates modeling fees at per-fill rounding (the worst case) rather than aggregate-order rounding. See Section 7. |
| **All dollar thresholds expressed as bankroll fractions** | $10 starting bankroll renders fixed-dollar caps meaningless. Every position limit, exposure cap, and minimum bet is now a fraction of current bankroll b. See Section 7. |
| **params table replaces all hardcoded constants** | Every tunable parameter is stored in the database params table with key, default, valid range, and owning formula. Python constants are bootstrap defaults only. See Section 3. |
| **state_version gap detection and recovery** | Kalman state table includes state_version integer. Gap detection triggers U_k inflation (option b from v9_thinking). See Section 5. |
| **METAR intraday observation collector fully specified** | New L1 collector. Fetches live METAR data from Aviation Weather Center. Powers distribution truncation in L3. See Sections 4 and 6. |
| **AFD text signal collector fully specified** | New L1 collector. Fetches NWS Area Forecast Discussion. NLP keyword extraction produces per-station confidence flag and σ_eff multiplier. See Sections 4 and 5. |
| **compute_metrics.py and sync_bins.py responsibilities redistributed** | compute_metrics.py (L5) no longer writes to shadow_book (L3). sync_bins.py (L4) no longer computes probability distributions (L3). All pricing lives in pricing/shadow_book.py. See Section 1. |
| **Oracle-compatible DDL throughout** | All CREATE TABLE statements use Oracle syntax: NUMBER, VARCHAR2, GENERATED ALWAYS AS IDENTITY, no SERIAL, no PostgreSQL extensions. See Section 2. |
| **IBE signals (KCV, MPDS, HMAS, FCT, SCAS) fully specified** | v9_thinking defined these conceptually. v10 provides complete formulas, units, input sources, output ranges, and the veto/scaling combination mechanism. See Section 7. |
| **Conviction Gates expanded to five conditions with exact thresholds** | All gate conditions, thresholds, and failure behaviors explicitly stated. See Section 7. |
| **Adaptive ε_edge formula** | Replaces static 0.03 with max(0.03, 1.96 × √(0.25/N_bets)). Tightens automatically as bet record grows. See Section 7. |
| **BSS pattern classifier automated** | Four degradation patterns (row, column, diagonal, convergence) detected automatically. Replaces weekly human matrix review. See Section 8. |
| **Pipeline orchestration fully specified** | Three run types (morning, night, market_open) with exact ordered function call sequences, layer annotations, and failure handling. See Section 9. |
| **Continuous Φ function replaces step tiers** | Φ(BSS) = min(1.0, BSS / 0.25), clipped to minimum 0.10 above Skill Gate threshold. Eliminates discontinuities. See Section 7. |
| **Weighted ensemble spread S_weighted_{t,k} added** | BSS-weighted spread used in σ_eff; raw unweighted spread used in Spread Gate. See Section 5. |
| **Exponentially weighted Q_base** | Replaces uniform-weight 180-day rolling window with EWM (span=90). Reduces seasonal lag. See Section 5. |
| **Minimum viable EV_net defined as bankroll fraction** | min_bet_floor = 0.025 × b. Bets producing sub-floor dollar amounts are skipped. See Section 7. |
| **CLI amendment detection** | Re-fetch CLI for 3 days post-observation. Retroactive Kalman correction on amendment. See Section 9. |
| **Maker adverse selection test** | Delta test over 90-day window comparing fill_quality_maker vs. fill_quality_taker. See Section 8. |

## Issue Resolution Log

Every open issue from the context/ documents is accounted for below.

### From v8-v9 general comments (138 items)

| Issue | Status | Resolution |
|-------|--------|------------|
| Garbled text in ensemble aggregation (Item 1) | **Resolved** | Corrected in v9 and carried forward. Section 5. |
| Covariance-penalized ensemble (Item 2) | **Resolved — ARCH DEFINED, NOT LIVE** | Full minimum-variance formula specified with 60-day data dependency. Section 5. |
| Skew-normal Python implementation (Item 3) | **Resolved** | scipy.stats.skewnorm.cdf canonical. Section 6. |
| Bimodal regime detection thresholds (Item 4) | **Resolved — ARCH DEFINED, NOT LIVE** | IQR/S > 1.35 trigger, 1D K-means, mixture-of-normals. Section 6. |
| Formal Kelly for mutually exclusive outcomes (Item 5) | **Resolved** | Smirnov 1973 algorithm with worked 3-bin example. Section 7. |
| BIC guard formula (Item 6) | **Resolved** | BIC = n ln(RSS/n) + k ln(n). Section 10. |
| Shadow book schema missing skew-normal params (CC-1) | **Resolved** | shadow_book table includes alpha_s, xi_s, omega_s. Section 2. |
| Continuous vs. categorical lead time mismatch (CC-2) | **Resolved** | classify_lead_hours() function defined. Section 5. |
| Missing BSS matrix infrastructure (CC-3) | **Resolved** | bss_matrix table with incremental update. Section 2. |
| Zero-variance G₁ fallback (Gap-1) | **Resolved** | Explicit IF variance == 0 THEN G₁ = 0.0. Section 6. |
| VWAP depth limitations (Gap-2) | **Resolved** | Penalty + tranche execution. Section 7. |
| Fee partial-fill ambiguity (Gap-3) | **Resolved** | Conservative per-fill rounding assumed. Section 7. |
| t-distribution vs. skew-normal (CC-1) | **Resolved** | Student's t fully replaced by skew-normal. Section 6. |
| Shadow book row structure ticker vs. bin_even (CC-2) | **Resolved** | Single insertion path via pricing/shadow_book.py. Section 1. |
| dashboard_stats missing skewness (CC-4) | **Resolved** | skewness_sample and skewness_unbiased columns added. Section 2. |
| Lead-time stratification brackets vs. lead_days (CC-5) | **Resolved** | lead_bracket (h1–h5) classification. Section 2. |
| position_already_open update logic (Gap-3) | **Resolved** | positions table with status lifecycle. Section 2. |
| Q_k J initialization boundary (Gap-4) | **Resolved** | J = min(5, completed_run_pairs_since_last_observation). Section 5. |
| BSS baseline definition (Gap-5) | **Resolved** | Two baselines: Baseline 1 (climatological — equal probability across all bins) and Baseline 2 (market — Kalshi implied prices). Section 8. |
| Gamma P_market definition (Gap-6) | **Resolved** | P_market = Kalshi market YES price c_i. Gamma measures market price concentration, not model probability concentration. Section 7. |
| Five items not fully transferred to v9 (ρ derivation, FDR/churn, drawdown reminder, cross-station rationale, skill gate forward ref) | **Resolved** | All five items integrated at their point of use. Sections 7, 8. |
| Station-day cap absent from tuning table | **Resolved** | Restored: station_day_cap = 0.10 × b. Section 3. |

### From v9_thinking (7 unsolved problems + 5 IBE signals + 5 next signals)

| Issue | Status | Resolution |
|-------|--------|------------|
| BSS weight monopoly convergence (Area 1.1) | **Resolved** | Entropy regularization with λ_entropy default = 0.1. Section 5. |
| Kalman state continuity (Area 1.2) | **Resolved** | state_version integer with gap detection, option b recovery. Section 5. |
| VWAP static book (Area 1.3) | **Resolved** | Delta test + tranche execution for >50 contracts. Section 7. |
| σ_eff cross-lead-time covariance (Area 1.4) | **Deferred** | Rationale: requires open-position risk tracking infrastructure that is premature at $10 bankroll with zero open positions. Simpler partial fix (store S_{t,k} at entry, flag delta_S > 1.0°F) included as a monitoring metric in Section 8. |
| NWS CLI amendments (Area 1.5) | **Resolved** | 3-day re-fetch, retroactive correction. Section 9. |
| GitHub Actions reliability (Area 1.6) | **Resolved** | Pipeline health table, heartbeat, missed-run detection. Section 9. |
| Bayesian shrinkage instability (Area 1.7) | **Resolved** | Rolling median σ_global instead of mean. Stations with σ > 2× their 90-day rolling mean excluded from global calculation. Section 5. |
| KCV signal | **Resolved** | Full formula with normalization. Section 7. |
| MPDS signal | **Resolved** | Full formula with veto threshold. Section 7. |
| HMAS signal | **Resolved** | Full formula with consensus measurement. Section 7. |
| FCT signal | **Resolved** | Full formula with convergence/divergence detection. Section 7. |
| SCAS signal | **Resolved** | Full formula with seasonal bias surface. Activates after N ≥ 90 days at seasonal period. Section 7. |
| IBE combination mechanism | **Resolved** | Veto tier + scaling tier with weighted geometric mean. Section 7. |
| METAR live intraday (Area 5.5) | **Resolved** | Collector + truncated skew-normal. Sections 4, 6. |
| AFD text signal (Area 5.1) | **Resolved** | Collector + NLP keyword extraction + σ_eff multiplier. Sections 4, 7. |
| Kalshi microstructure (Area 5.2) | **Deferred** | Rationale: requires historical Kalshi price data that is not yet collected. Convergence curve analysis specified as future enhancement after 180 days of market_orderbook_snapshots data. |
| ENSO/AO/NAO regime stratification (Area 5.3) | **Deferred** | Rationale: requires 5+ years of regime-tagged error data. Specified as architecture for v11+ after 3 years of operation. |
| Neighboring station cross-correlation (Area 5.4) | **Deferred** | Rationale: the 20×20 lag-correlation matrix is computable after 90 days but adds significant complexity for modest edge. Specified as Phase 4 enhancement. |

### From v8_audit (69 items + 6 code conflicts + 7 gaps)

All 69 inline comment resolutions from the audit are incorporated. Code conflicts CC-1 through CC-6 are resolved (see above). Gaps GAP-1 through GAP-7 are resolved.

### From v8 suggestions (6 recovered ideas)

| Idea | Status | Resolution |
|------|--------|------------|
| Bayesian shrinkage on σ and g | **Resolved** | Applied per (model, station, lead_bracket) triple. Section 5. |
| K-fold vs. walk-forward validation | **Resolved** | Walk-forward mandated; k-fold explicitly rejected for time-series. Section 10. |
| BIC feature selection | **Resolved** | Guard specified in Section 10. |
| Sharpe ratio formula and benchmarks | **Resolved** | Fully restored with interpretive context. Section 8. |
| Model forecast convergence tracking | **Resolved** | FCT signal in IBE. Section 7. |
| Skew-normal conversion architecture | **Resolved** | Proper (ξ, ω, α) conversion replaces blending weight. Section 6. |

---

# Section 1: System Architecture

## The Five-Layer Model

KalshiCast is organized into five layers with strict no-upward-dependency enforcement. Each layer may read from layers below it and write only to its own tables.

| Layer | Name | Responsibility | Depends On |
|-------|------|---------------|------------|
| L1 | Data Collection | Fetch raw forecast data and observations from external APIs. Write raw records to DB. Know nothing about pricing. | External APIs, DB write |
| L2 | Processing | Kalman correction, ensemble aggregation, σ computation, skewness estimation. Reads L1 output, writes derived state. | L1 DB tables |
| L3 | Pricing | Shadow Book generation. Reads L2 state, writes P(win) per bin. Handles skew-normal, METAR truncation, bimodal detection. | L2 DB tables |
| L4 | Execution | IBE signals, gate evaluation, Kelly sizing, order submission. Reads L3. Writes positions, executes API orders. | L3 DB tables, Kalshi API |
| L5 | Evaluation | Scoring, BSS computation, alert generation, calibration. Reads across L1–L4. Writes metrics. Never writes to L1–L4 state. | All layers, read-only |

### Four Design Invariants

1. **Correct by construction.** Every computation has a single canonical path. There is no "compute_metrics.py sometimes inserts shadow book rows, sync_bins.py sometimes inserts different rows" ambiguity. Each function owns its responsibility entirely.

2. **Observable at every layer.** Every pipeline run, every Kalman state, every Shadow Book price, every IBE signal is persisted and queryable.

3. **Restartable from any point.** If the 04:00 UTC run fails, the 08:00 UTC run must produce correct results from whatever state was last written — not broken results from a half-updated state.

4. **Separable into read and write paths.** The betting engine reads. The pipeline writes. These two concerns never share a code path.

## Full Directory Layout

```
kalshicast/
  config/
    stations.py          # Station definitions — STATIONS list (20 active stations)
    sources.py           # SOURCES dict — model configs (9 models)
    params.py            # All tunable parameters with defaults (bootstrap file)
    secrets.py           # Env var loading only — no logic
  db/
    connection.py        # get_conn(), connection pool, state_version guard, run_lock
    schema.py            # CREATE TABLE statements — authoritative schema source
    migrations/          # Numbered migration scripts
    readers.py           # Read-only query functions (one per entity type)
    writers.py           # Write/upsert functions (one per entity type)
  collection/            # L1 — Data Collection
    collector_harness.py # Shared retry, validation, semaphore logic
    lead_time.py         # compute_lead_hours(), classify_lead_hours()
    sources_registry.py  # Dynamic loader for enabled model sources
    collectors/
      base.py            # Abstract Collector interface + shared time_axis + ForecastBundle type
      collect_nws.py     # NWS 7-day point forecast (6 collectors share collect_ome_model pattern)
      collect_ome.py     # Open-Meteo base model
      collect_ome_model.py # Open-Meteo GFS/EC/ICON/GEM ensemble models
      collect_wapi.py    # WeatherAPI
      collect_vcr.py     # Visual Crossing
      collect_tom.py     # Tomorrow.io
      collect_metar.py   # NEW: live METAR intraday temperature from Aviation Weather Center
      collect_afd.py     # NEW: NWS Area Forecast Discussion text + NLP extraction
      collect_cli.py     # Extracted from night.py — owns all NWS CLI observation logic
  processing/            # L2 — Processing
    ensemble.py          # BSS-weighted mean, entropy regularization, staleness decay, top-model selection
    spread.py            # S_{t,k}, S_weighted_{t,k}, σ_eff computation
    kalman.py            # Kalman filter — all 5 steps, state_version continuity, gap recovery
    sigma.py             # RMSE computation, Bayesian shrinkage per (model, station, lead_bracket)
    skewness.py          # g_s, G₁_s computation with zero-variance fallback
    lead_time.py         # classify_lead_hours() — continuous float → h1–h5 bracket
  pricing/               # L3 — Pricing
    shadow_book.py       # Skew-normal parameterization + P(win) computation + single DB write path
    truncation.py        # METAR truncation logic — truncated skew-normal
    bimodal.py           # Bimodal regime detection [ARCH DEFINED, NOT LIVE]
  execution/             # L4 — Execution
    ibe/
      signals.py         # KCV, MPDS, HMAS, FCT, SCAS computation
      engine.py          # Veto tier + scaling tier combination → COMPOSITE
    gates.py             # All 5 Conviction Gate conditions
    kelly.py             # f*, Φ continuous function, formal mutually-exclusive Kelly (Smirnov 1973)
    sizing.py            # Γ filter, D_scale, regional cap, cross-station cap, station-day cap, jitter
    vwap.py              # Order book scanning, c_VWAP computation, tranche execution
    orders.py            # Kalshi API order submission, auth, retry logic
  evaluation/            # L5 — Evaluation
    scoring.py           # BS, BSS, MAE, RMSE per run — incremental update
    calibration.py       # CAL, Market CAL — weekly with 90-day trailing window
    financial.py         # SR_$, SR, MDD, FDR, EUR, Sharpe
    alerts.py            # Alert generation — all severity levels
    bss_classifier.py    # Automated BSS pattern classifier (row/column/diagonal/convergence)
  pipeline/              # Orchestration
    morning.py           # L1+L2+L3: ingest → process → price → PREVIEW
    night.py             # L1(CLI)+L2+L5: observation → Kalman → score → evaluate
    market_open.py       # L3+L4: finalize pricing → gate → execute
    backtest.py          # Harness: runs any pipeline_fn against history chronologically
    health.py            # Pipeline heartbeat, missed-run detection, run_lock management
  tests/
    synthetic/           # Synthetic data generators for offline testing
    unit/                # One test file per module
    integration/         # Full pipeline run against synthetic DB
```

## Interface Contracts

### ForecastBundle (L1 output — every collector returns this)

```
ForecastBundle = {
  'source_id': str,           # e.g. 'NWS', 'OME_GFS'
  'station_id': str,          # e.g. 'KNYC'
  'issued_at': str,           # UTC ISO-8601, e.g. '2026-03-22T20:00:00Z'
  'init_time': str,           # UTC ISO-8601 — underlying model initialization time
  'daily': [DailyRow],        # always 4 rows (4 target days)
  'hourly': [HourlyRow],      # always 96 rows (96 hours)
}

DailyRow = {
  'target_date': str,         # YYYY-MM-DD
  'high_f': float,            # forecast high in °F
  'low_f': float,             # forecast low in °F
  'lead_hours_high': float,   # hours from issued_at to projected hour of max temp
  'lead_hours_low': float,    # hours from issued_at to projected hour of min temp
}

HourlyRow = {
  'valid_time_utc': str,      # UTC ISO-8601
  'temperature_f': float,
  'dewpoint_f': float | None,
  'humidity_pct': float | None,
  'wind_speed_mph': float | None,
  'wind_dir_deg': int | None,
  'cloud_cover_pct': int | None,
  'precip_prob_pct': int | None,
  'precip_type_code': int | None,  # WMO weather code
}
```

### KalmanState (L2 — processing/kalman.py)

```
KalmanState = {
  'station_id': str,
  'target_type': str,         # 'high' or 'low'
  'B_k': float,               # current bias estimate (°F)
  'U_k': float,               # current uncertainty (°F²)
  'Q_base': float,            # bias drift variance (°F²)
  'state_version': int,       # increments atomically on every successful write
  'last_observation_date': str,# YYYY-MM-DD
  'last_updated_utc': str,    # UTC ISO-8601
}
```

### ShadowBookRow (L3 — pricing/shadow_book.py)

```
ShadowBookRow = {
  'ticker': str,              # Kalshi market ticker (primary key)
  'station_id': str,
  'target_date': str,
  'target_type': str,         # 'high' or 'low'
  'bin_lower': float,         # °F, inclusive
  'bin_upper': float,         # °F, exclusive
  'mu': float,                # bias-corrected expected mean (°F)
  'sigma_eff': float,         # spread-adjusted std dev (°F)
  'G1_s': float,              # unbiased skewness estimator
  'alpha_s': float,           # skew-normal shape parameter
  'xi_s': float,              # skew-normal location parameter (°F)
  'omega_s': float,           # skew-normal scale parameter (°F)
  'p_win': float,             # P(bin_lower ≤ T < bin_upper)
  'metar_truncated': bool,    # True if truncated distribution applied
  't_obs_max': float | None,  # max observed temp so far today (°F)
  'top_model_id': str,        # model selected as F_{t,k}^(top)
  'pipeline_run_id': str,
}
```

### IBESignal (L4 — execution/ibe/signals.py)

```
IBESignal = {
  'ticker': str,
  'pipeline_run_id': str,
  'kcv_normalized': float,    # Kalman Correction Velocity, normalized
  'kcv_mod': float,           # modifier [0.5, 1.0]
  'mpds_k': float,            # Market Price Drift Signal
  'mpds_mod': float,          # modifier [0.3, 1.0]
  'hmas': float,              # High-BSS Model Agreement Score [0, 1]
  'hmas_mod': float,          # modifier [0.7, 1.3]
  'fct': float,               # Forecast Convergence Trajectory
  'fct_mod': float,           # modifier [0.5, 1.4]
  'scas': float,              # Station Calendar Anomaly Score
  'scas_mod': float,          # modifier [0.6, 1.0]
  'composite': float,         # weighted geometric mean of modifiers
  'veto_triggered': bool,
  'veto_reason': str | None,
}
```

### PositionRecord (L4 — execution/orders.py)

```
PositionRecord = {
  'position_id': str,         # UUID
  'ticker': str,
  'station_id': str,
  'target_date': str,
  'target_type': str,
  'bin_lower': float,
  'bin_upper': float,
  'entry_price': float,       # actual fill price per contract
  'contracts': int,
  'order_type': str,          # 'taker' or 'maker'
  'submitted_at': str,        # UTC ISO-8601
  'filled_at': str | None,
  'actual_fill_price': float | None,
  'status': str,              # 'submitted' | 'filled' | 'cancelled' | 'resolved'
  'outcome': int | None,      # 1=win, 0=loss, None=pending
  'pnl_gross': float | None,
  'pnl_net': float | None,    # after fees
  'fill_quality': float | None, # p_win - c_fill
  'S_tk_at_entry': float,     # ensemble spread at time of entry (for risk monitoring)
}
```

## Existing File → New Module Mapping

| Existing File | New Module(s) | What Changes |
|--------------|---------------|--------------|
| `db.py` (33KB) | `db/connection.py`, `db/readers.py`, `db/writers.py` | Split into read/write separation. Remove all SQL that mixes pricing logic with DB operations. Add state_version guard. |
| `morning.py` | `pipeline/morning.py` | Becomes pure orchestrator. All computation moves to processing/ and pricing/. |
| `night.py` (stub) | `pipeline/night.py` + `collectors/collect_cli.py` | CLI logic extracted to collector. Night pipeline expanded to full Kalman + scoring loop. |
| `compute_metrics.py` | `evaluation/scoring.py` + `processing/sigma.py` + `processing/skewness.py` | **No longer writes to shadow_book.** Scoring logic moves to L5. RMSE/skewness computation moves to L2. |
| `sync_bins.py` | `execution/vwap.py` + `execution/orders.py` | **No longer computes probability distributions.** Bin parsing moves to a market structure helper. Auth logic preserved. |
| `config.py` | `config/stations.py` + `config/sources.py` | Station and source configs separated. NWS points URL cache added to stations. |
| `etl_utils.py` | `processing/lead_time.py` | compute_lead_hours() preserved. classify_lead_hours() added. |
| `sources_registry.py` | `config/sources.py` | Merged into source configuration. |
| `cli_observations.py` | `collectors/collect_cli.py` | CLI bulletin fetching and parsing extracted as a proper collector. |
| `collectors/*.py` | `collectors/*.py` | Ported with ForecastBundle interface alignment. Add init_time tracking. |
| `utils/health_check.py` | `pipeline/health.py` | Expanded with pipeline_runs heartbeat and missed-run detection. |
| `utils/time_axis.py` | `collectors/base.py` | Merged into collector base class. |
| `utils/populate_fake_data.py` | `tests/synthetic/` | Moved to test infrastructure. |
| `utils/reset_db.py` | `db/migrations/` | Schema reset becomes a numbered migration. |
| `utils/rollover.py` | `pipeline/morning.py` | Rollover logic integrated into pipeline orchestration. |

---

# Section 2: Database Schema

All DDL is Oracle Autonomous Database compatible. Schema owner: ADMIN. Tablespace: DATA.

## Configuration Tables (read-only in production)

### stations

Owner: Config. Written at deployment. Read by all layers.

Stores the 20 active NWS ASOS observation stations.

```sql
CREATE TABLE ADMIN.STATIONS (
  STATION_ID        VARCHAR2(10)   NOT NULL,
  NAME              VARCHAR2(100),
  CITY              VARCHAR2(50),
  STATE_CODE        VARCHAR2(2),
  TIMEZONE          VARCHAR2(40),
  LAT               NUMBER(9,6),
  LON               NUMBER(10,6),
  ELEVATION_FT      NUMBER(6,0),
  WFO_ID            VARCHAR2(4),
  IS_ACTIVE         NUMBER(1)      DEFAULT 1,
  IS_RELIABLE       NUMBER(1)      DEFAULT 1,
  RELIABILITY_NOTE  VARCHAR2(500),
  FLAGGED_AT        TIMESTAMP(6),
  FORECAST_URL      VARCHAR2(500),
  GRID_URL          VARCHAR2(500),
  HOURLY_URL        VARCHAR2(500),
  CONSTRAINT PK_STATIONS PRIMARY KEY (STATION_ID)
);
```

### sources

Owner: Config. Written at deployment. Read by L1, L2.

Stores the 9 model source definitions.

```sql
CREATE TABLE ADMIN.SOURCES (
  SOURCE_ID         VARCHAR2(20)   NOT NULL,
  NAME              VARCHAR2(100),
  MODULE_PATH       VARCHAR2(200),
  FUNC_NAME         VARCHAR2(100),
  PARAMS_JSON       CLOB,
  IS_ENABLED        NUMBER(1)      DEFAULT 1,
  UPDATE_CYCLE_HOURS NUMBER(4,1),
  CONSTRAINT PK_SOURCES PRIMARY KEY (SOURCE_ID)
);
```

### params

Owner: Config. Written by calibration process. Read by all layers.

Single source of truth for all tunable parameters.

```sql
CREATE TABLE ADMIN.PARAMS (
  PARAM_KEY         VARCHAR2(100)  NOT NULL,
  PARAM_VALUE       VARCHAR2(200),
  VALID_RANGE       VARCHAR2(100),
  DESCRIPTION       VARCHAR2(500),
  OWNING_FORMULA    VARCHAR2(200),
  IS_CALIBRATION_REQUIRED NUMBER(1) DEFAULT 0,
  LAST_CHANGED_AT   TIMESTAMP(6)   DEFAULT SYSTIMESTAMP,
  CHANGED_BY        VARCHAR2(100),
  CHANGE_REASON     VARCHAR2(500),
  CONSTRAINT PK_PARAMS PRIMARY KEY (PARAM_KEY)
);
```

## L1: Raw Collection Tables

### pipeline_runs

Owner: L1. Written at start/end of every pipeline execution. Read by all layers.

Heartbeat for every pipeline execution. run_id is the foreign key for all derived data.

```sql
CREATE TABLE ADMIN.PIPELINE_RUNS (
  RUN_ID            VARCHAR2(36)   NOT NULL,
  RUN_TYPE          VARCHAR2(20),
  SCHEDULED_UTC     TIMESTAMP(6),
  STARTED_UTC       TIMESTAMP(6),
  COMPLETED_UTC     TIMESTAMP(6),
  STATUS            VARCHAR2(20),
  M_K               NUMBER(3,0),
  ERROR_MSG         VARCHAR2(2000),
  CONSTRAINT PK_PIPELINE_RUNS PRIMARY KEY (RUN_ID)
);

CREATE INDEX IDX_PIPELINE_RUNS_SCHED ON ADMIN.PIPELINE_RUNS (SCHEDULED_UTC);
```

### forecast_runs

Owner: L1. Written by collectors. Read by L2.

One row per (pipeline run, source, station).

```sql
CREATE TABLE ADMIN.FORECAST_RUNS (
  RUN_ID            VARCHAR2(36)   NOT NULL,
  SOURCE_ID         VARCHAR2(20),
  STATION_ID        VARCHAR2(10),
  ISSUED_AT         TIMESTAMP(6),
  INIT_TIME         TIMESTAMP(6),
  RAW_PAYLOAD_JSON  CLOB,
  INGESTED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP,
  CONSTRAINT PK_FORECAST_RUNS PRIMARY KEY (RUN_ID)
);
```

### forecasts_daily

Owner: L1. Written by collectors. Read by L2, L5.

Four rows per forecast_run (4 target days).

```sql
CREATE TABLE ADMIN.FORECASTS_DAILY (
  RUN_ID            VARCHAR2(36),
  SOURCE_ID         VARCHAR2(20),
  STATION_ID        VARCHAR2(10),
  TARGET_DATE       DATE,
  HIGH_F            NUMBER(5,1),
  LOW_F             NUMBER(5,1),
  LEAD_HOURS_HIGH   NUMBER(6,1),
  LEAD_HOURS_LOW    NUMBER(6,1),
  CREATED_AT        TIMESTAMP(6)   DEFAULT SYSTIMESTAMP
) PARTITION BY RANGE (TARGET_DATE) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION P_INIT VALUES LESS THAN (DATE '2026-04-01'));

CREATE INDEX IDX_FCDAILY_LOOKUP ON ADMIN.FORECASTS_DAILY (STATION_ID, TARGET_DATE, SOURCE_ID);
```

### forecasts_hourly

Owner: L1. Written by collectors. Read by L2.

96 rows per forecast_run.

```sql
CREATE TABLE ADMIN.FORECASTS_HOURLY (
  RUN_ID            VARCHAR2(36),
  SOURCE_ID         VARCHAR2(20),
  STATION_ID        VARCHAR2(10),
  VALID_TIME_UTC    TIMESTAMP(6),
  TEMPERATURE_F     NUMBER(5,1),
  DEWPOINT_F        NUMBER(5,1),
  HUMIDITY_PCT      NUMBER(5,2),
  WIND_SPEED_MPH    NUMBER(5,1),
  WIND_DIR_DEG      NUMBER(3,0),
  CLOUD_COVER_PCT   NUMBER(3,0),
  PRECIP_PROB_PCT   NUMBER(3,0),
  PRECIP_TYPE_CODE  NUMBER(3,0),
  WEATHERCODE       NUMBER(3,0)
) PARTITION BY RANGE (VALID_TIME_UTC) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION P_INIT VALUES LESS THAN (TIMESTAMP '2026-04-01 00:00:00'));
```

### observations

Owner: L1. Written by collect_cli.py. Read by L2, L5.

Official NWS CLI observations with amendment tracking.

```sql
CREATE TABLE ADMIN.OBSERVATIONS (
  STATION_ID        VARCHAR2(10)   NOT NULL,
  TARGET_DATE       DATE           NOT NULL,
  OBSERVED_HIGH_F   NUMBER(5,1),
  OBSERVED_LOW_F    NUMBER(5,1),
  SOURCE            VARCHAR2(10)   DEFAULT 'CLI',
  INGESTED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP,
  AMENDED           NUMBER(1)      DEFAULT 0,
  AMENDED_AT        TIMESTAMP(6),
  ORIGINAL_HIGH_F   NUMBER(5,1),
  ORIGINAL_LOW_F    NUMBER(5,1),
  CONSTRAINT PK_OBSERVATIONS PRIMARY KEY (STATION_ID, TARGET_DATE)
);
```

### metar_observations

Owner: L1. Written by collect_metar.py. Read by L3.

Live METAR feed. One row per METAR report per station.

```sql
CREATE TABLE ADMIN.METAR_OBSERVATIONS (
  STATION_ID        VARCHAR2(10),
  OBSERVED_UTC      TIMESTAMP(6),
  TEMPERATURE_F     NUMBER(5,1),
  DEW_POINT_F       NUMBER(5,1),
  WIND_SPEED_KT     NUMBER(5,1),
  WIND_DIR_DEG      NUMBER(3,0),
  RAW_METAR         VARCHAR2(500),
  INGESTED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP
) PARTITION BY RANGE (OBSERVED_UTC) INTERVAL (NUMTODSINTERVAL(7, 'DAY'))
(PARTITION P_INIT VALUES LESS THAN (TIMESTAMP '2026-04-01 00:00:00'));

CREATE INDEX IDX_METAR_LOOKUP ON ADMIN.METAR_OBSERVATIONS (STATION_ID, OBSERVED_UTC DESC);
```

### metar_daily_max

Owner: L1. Written by collect_metar.py. Read by L3.

Running high/low per station per calendar day.

```sql
CREATE TABLE ADMIN.METAR_DAILY_MAX (
  STATION_ID        VARCHAR2(10)   NOT NULL,
  LOCAL_DATE        DATE           NOT NULL,
  T_OBS_MAX_F       NUMBER(5,1),
  T_OBS_MIN_F       NUMBER(5,1),
  OBS_COUNT         NUMBER(4,0)    DEFAULT 0,
  LAST_OBS_AT       TIMESTAMP(6),
  LAST_UPDATED_UTC  TIMESTAMP(6),
  CONSTRAINT PK_METAR_DAILY_MAX PRIMARY KEY (STATION_ID, LOCAL_DATE)
);
```

### afd_text

Owner: L1. Written by collect_afd.py. Read by L4 (IBE).

NWS Area Forecast Discussion raw text.

```sql
CREATE TABLE ADMIN.AFD_TEXT (
  STATION_ID        VARCHAR2(10),
  WFO_ID            VARCHAR2(4),
  ISSUED_UTC        TIMESTAMP(6),
  DISCUSSION_TEXT   CLOB,
  FETCHED_AT        TIMESTAMP(6)   DEFAULT SYSTIMESTAMP
);

CREATE INDEX IDX_AFD_LOOKUP ON ADMIN.AFD_TEXT (STATION_ID, ISSUED_UTC DESC);
```

### afd_signals

Owner: L1. Written by collect_afd.py NLP layer. Read by L2 (σ_eff computation).

Parsed AFD signals.

```sql
CREATE TABLE ADMIN.AFD_SIGNALS (
  STATION_ID        VARCHAR2(10),
  ISSUED_UTC        TIMESTAMP(6),
  CONFIDENCE_FLAG   VARCHAR2(10),
  MODEL_DISAGREEMENT_FLAG NUMBER(1),
  DIRECTIONAL_NOTE  VARCHAR2(200),
  SIGMA_MULTIPLIER  NUMBER(4,2)    DEFAULT 1.00
);
```

## L2: Processing / Derived State Tables

### kalman_states

Owner: L2. Written by kalman.py. Read by L2, L3.

One row per (station, target_type). Separate Kalman filters for high and low.

```sql
CREATE TABLE ADMIN.KALMAN_STATES (
  STATION_ID        VARCHAR2(10)   NOT NULL,
  TARGET_TYPE       VARCHAR2(4)    NOT NULL,
  B_K               NUMBER(10,6),
  U_K               NUMBER(10,6),
  Q_BASE            NUMBER(10,6),
  STATE_VERSION     NUMBER(10,0)   DEFAULT 0,
  TOP_MODEL_ID      VARCHAR2(20),
  LAST_OBSERVATION_DATE DATE,
  LAST_UPDATED_UTC  TIMESTAMP(6),
  CONSTRAINT PK_KALMAN_STATES PRIMARY KEY (STATION_ID, TARGET_TYPE)
);
```

### kalman_history

Owner: L2. Append-only log. Read by L5 for debugging.

```sql
CREATE TABLE ADMIN.KALMAN_HISTORY (
  STATION_ID        VARCHAR2(10),
  TARGET_TYPE       VARCHAR2(4),
  PIPELINE_RUN_ID   VARCHAR2(36),
  B_K               NUMBER(10,6),
  U_K               NUMBER(10,6),
  Q_K               NUMBER(10,6),
  R_K               NUMBER(10,6),
  K_K               NUMBER(10,6),
  EPSILON_K         NUMBER(10,6),
  STATE_VERSION     NUMBER(10,0),
  IS_AMENDMENT      NUMBER(1)      DEFAULT 0,
  CREATED_AT        TIMESTAMP(6)   DEFAULT SYSTIMESTAMP
) PARTITION BY RANGE (CREATED_AT) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION P_INIT VALUES LESS THAN (TIMESTAMP '2026-04-01 00:00:00'));
```

### ensemble_state

Owner: L2. Written by ensemble.py. Read by L3, L4.

```sql
CREATE TABLE ADMIN.ENSEMBLE_STATE (
  RUN_ID            VARCHAR2(36)   NOT NULL,
  STATION_ID        VARCHAR2(10)   NOT NULL,
  TARGET_DATE       DATE           NOT NULL,
  TARGET_TYPE       VARCHAR2(4)    NOT NULL,
  F_TK_TOP          NUMBER(6,2),
  TOP_MODEL_ID      VARCHAR2(20),
  F_BAR_TK          NUMBER(6,2),
  S_TK              NUMBER(6,3),
  S_WEIGHTED_TK     NUMBER(6,3),
  SIGMA_EFF         NUMBER(6,3),
  M_K               NUMBER(3,0),
  WEIGHT_JSON       CLOB,
  STALE_MODEL_IDS   VARCHAR2(200),
  CONSTRAINT PK_ENSEMBLE_STATE PRIMARY KEY (RUN_ID, STATION_ID, TARGET_DATE, TARGET_TYPE)
);
```

### dashboard_stats

Owner: L2. Rebuilt daily. Read by L2, L3, L5.

Precomputed accuracy statistics per (station, source, target_type, lead_bracket, window_days).

```sql
CREATE TABLE ADMIN.DASHBOARD_STATS (
  STATION_ID        VARCHAR2(10),
  SOURCE_ID         VARCHAR2(20),
  TARGET_TYPE       VARCHAR2(4),
  LEAD_BRACKET      VARCHAR2(2),
  WINDOW_DAYS       NUMBER(4,0),
  N                 NUMBER(6,0),
  BIAS              NUMBER(8,4),
  MAE               NUMBER(8,4),
  RMSE_RAW          NUMBER(8,4),
  RMSE_ADJ          NUMBER(8,4),
  SKEWNESS_SAMPLE   NUMBER(8,5),
  SKEWNESS_UNBIASED NUMBER(8,5),
  P10               NUMBER(8,4),
  P25               NUMBER(8,4),
  P50               NUMBER(8,4),
  P75               NUMBER(8,4),
  P90               NUMBER(8,4),
  COMPUTED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP
);

CREATE INDEX IDX_DSTATS_LOOKUP ON ADMIN.DASHBOARD_STATS (STATION_ID, SOURCE_ID, TARGET_TYPE, LEAD_BRACKET);
```

### bss_matrix

Owner: L5 (written by scoring, but read by L2 for top-model selection and L4 for gates).

The 20×5 skill matrix (expandable to 20×5×2 for high/low).

```sql
CREATE TABLE ADMIN.BSS_MATRIX (
  STATION_ID        VARCHAR2(10)   NOT NULL,
  TARGET_TYPE       VARCHAR2(4)    NOT NULL,
  LEAD_BRACKET      VARCHAR2(2)    NOT NULL,
  WINDOW_DAYS       NUMBER(4,0),
  BS_MODEL          NUMBER(10,6),
  BS_BASELINE_1     NUMBER(10,6),
  BS_BASELINE_2     NUMBER(10,6),
  BSS_1             NUMBER(10,6),
  BSS_2             NUMBER(10,6),
  IS_QUALIFIED      NUMBER(1)      DEFAULT 0,
  ENTERED_AT        TIMESTAMP(6),
  EXITED_AT         TIMESTAMP(6),
  H_STAR_S          VARCHAR2(2),
  N_OBSERVATIONS    NUMBER(6,0),
  COMPUTED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP,
  CONSTRAINT PK_BSS_MATRIX PRIMARY KEY (STATION_ID, TARGET_TYPE, LEAD_BRACKET)
);
```

### model_weights

Owner: L2. Written by ensemble.py per run. Read by L2, L5.

```sql
CREATE TABLE ADMIN.MODEL_WEIGHTS (
  RUN_ID            VARCHAR2(36),
  STATION_ID        VARCHAR2(10),
  SOURCE_ID         VARCHAR2(20),
  LEAD_BRACKET      VARCHAR2(2),
  W_M               NUMBER(8,6),
  BSS_M             NUMBER(8,6),
  IS_STALE          NUMBER(1),
  STALE_DECAY_FACTOR NUMBER(6,4),
  COMPUTED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP
);
```

## L3: Pricing Tables

### shadow_book

Owner: L3. Written by shadow_book.py. Read by L4, L5.

One row per Kalshi market contract. ticker is the canonical identifier.

```sql
CREATE TABLE ADMIN.SHADOW_BOOK (
  TICKER            VARCHAR2(100)  NOT NULL,
  STATION_ID        VARCHAR2(10),
  TARGET_DATE       DATE,
  TARGET_TYPE       VARCHAR2(4),
  BIN_LOWER         NUMBER(5,1),
  BIN_UPPER         NUMBER(5,1),
  MU                NUMBER(6,2),
  SIGMA_EFF         NUMBER(6,3),
  G1_S              NUMBER(8,5),
  ALPHA_S           NUMBER(10,6),
  XI_S              NUMBER(8,3),
  OMEGA_S           NUMBER(8,3),
  P_WIN             NUMBER(8,6),
  METAR_TRUNCATED   NUMBER(1)      DEFAULT 0,
  T_OBS_MAX         NUMBER(5,1),
  TOP_MODEL_ID      VARCHAR2(20),
  PIPELINE_RUN_ID   VARCHAR2(36),
  CREATED_AT        TIMESTAMP(6)   DEFAULT SYSTIMESTAMP,
  UPDATED_AT        TIMESTAMP(6),
  CONSTRAINT PK_SHADOW_BOOK PRIMARY KEY (TICKER)
);

CREATE INDEX IDX_SB_STATION_DATE ON ADMIN.SHADOW_BOOK (STATION_ID, TARGET_DATE, TARGET_TYPE);
```

### shadow_book_history

Owner: L3. Append-only. Read by L4 (MPDS signal).

```sql
CREATE TABLE ADMIN.SHADOW_BOOK_HISTORY (
  ID                NUMBER GENERATED ALWAYS AS IDENTITY,
  TICKER            VARCHAR2(100),
  P_WIN             NUMBER(8,6),
  MU                NUMBER(6,2),
  SIGMA_EFF         NUMBER(6,3),
  PIPELINE_RUN_ID   VARCHAR2(36),
  RECORDED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP
) PARTITION BY RANGE (RECORDED_AT) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION P_INIT VALUES LESS THAN (TIMESTAMP '2026-04-01 00:00:00'));
```

### regime_flags

Owner: L3. Written by bimodal.py. Read by L5.

```sql
CREATE TABLE ADMIN.REGIME_FLAGS (
  STATION_ID        VARCHAR2(10),
  TARGET_DATE       DATE,
  TARGET_TYPE       VARCHAR2(4),
  PIPELINE_RUN_ID   VARCHAR2(36),
  IQR_F             NUMBER(6,3),
  S_TK_F            NUMBER(6,3),
  BIMODAL_TRIGGERED NUMBER(1),
  CENTROID_1        NUMBER(6,2),
  CENTROID_2        NUMBER(6,2),
  CLUSTER_SIZE_1    NUMBER(3,0),
  CLUSTER_SIZE_2    NUMBER(3,0),
  RECORDED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP
);
```

## L4: Execution Tables

### best_bets

Owner: L4. Rebuilt every run. Read by L4 (order execution), front-end.

```sql
CREATE TABLE ADMIN.BEST_BETS (
  TICKER              VARCHAR2(100) NOT NULL,
  PIPELINE_RUN_ID     VARCHAR2(36),
  STATION_ID          VARCHAR2(10),
  TARGET_DATE         DATE,
  TARGET_TYPE         VARCHAR2(4),
  BIN_LOWER           NUMBER(5,1),
  BIN_UPPER           NUMBER(5,1),
  P_WIN               NUMBER(8,6),
  CONTRACT_PRICE      NUMBER(6,4),
  EV_NET              NUMBER(8,6),
  EV_THRESHOLD_H      NUMBER(8,6),
  ORDER_TYPE          VARCHAR2(10),
  C_VWAP              NUMBER(6,4),
  C_VWAP_NET          NUMBER(6,4),
  F_STAR              NUMBER(8,6),
  F_OP                NUMBER(8,6),
  F_FINAL             NUMBER(8,6),
  IBE_COMPOSITE       NUMBER(6,4),
  IBE_VETO            NUMBER(1)      DEFAULT 0,
  D_SCALE             NUMBER(6,4),
  GAMMA_CONVERGENCE   NUMBER(6,4),
  RANK_WITHIN_STATION_DAY NUMBER(3,0),
  IS_SELECTED_FOR_EXECUTION NUMBER(1),
  PIPELINE_RUN_STATUS VARCHAR2(12),
  ALL_GATE_FLAGS_JSON CLOB,
  CONSTRAINT PK_BEST_BETS PRIMARY KEY (TICKER)
);
```

### positions

Owner: L4. Written by orders.py. Read by L4 (dedup), L5.

```sql
CREATE TABLE ADMIN.POSITIONS (
  POSITION_ID       VARCHAR2(36)   NOT NULL,
  TICKER            VARCHAR2(100),
  STATION_ID        VARCHAR2(10),
  TARGET_DATE       DATE,
  TARGET_TYPE       VARCHAR2(4),
  BIN_LOWER         NUMBER(5,1),
  BIN_UPPER         NUMBER(5,1),
  ENTRY_PRICE       NUMBER(6,4),
  CONTRACTS         NUMBER(6,0),
  ORDER_TYPE        VARCHAR2(10),
  SUBMITTED_AT      TIMESTAMP(6),
  FILLED_AT         TIMESTAMP(6),
  ACTUAL_FILL_PRICE NUMBER(6,4),
  STATUS            VARCHAR2(12),
  OUTCOME           NUMBER(1),
  PNL_GROSS         NUMBER(10,4),
  PNL_NET           NUMBER(10,4),
  FILL_QUALITY      NUMBER(8,6),
  S_TK_AT_ENTRY     NUMBER(6,3),
  CONSTRAINT PK_POSITIONS PRIMARY KEY (POSITION_ID)
);

CREATE INDEX IDX_POS_OPEN ON ADMIN.POSITIONS (STATION_ID, TARGET_DATE, TARGET_TYPE, STATUS);
```

### order_log

Owner: L4. Append-only. Read by L5.

```sql
CREATE TABLE ADMIN.ORDER_LOG (
  ORDER_ID          VARCHAR2(36)   NOT NULL,
  POSITION_ID       VARCHAR2(36),
  TICKER            VARCHAR2(100),
  CONTRACTS         NUMBER(6,0),
  LIMIT_PRICE       NUMBER(6,4),
  ORDER_TYPE        VARCHAR2(10),
  SUBMITTED_AT      TIMESTAMP(6),
  KALSHI_RESPONSE_JSON CLOB,
  STATUS            VARCHAR2(20),
  ERROR_MSG         VARCHAR2(2000),
  CONSTRAINT PK_ORDER_LOG PRIMARY KEY (ORDER_ID)
) PARTITION BY RANGE (SUBMITTED_AT) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION P_INIT VALUES LESS THAN (TIMESTAMP '2026-04-01 00:00:00'));
```

### market_orderbook_snapshots

Owner: L4. Written by vwap.py. Read by L4.

```sql
CREATE TABLE ADMIN.MARKET_ORDERBOOK_SNAPSHOTS (
  TICKER            VARCHAR2(100),
  SNAPSHOT_UTC      TIMESTAMP(6),
  YES_BOOK_JSON     CLOB,
  NO_BOOK_JSON      CLOB,
  C_VWAP_COMPUTED   NUMBER(6,4),
  AVAILABLE_DEPTH   NUMBER(6,0)
);

CREATE INDEX IDX_ORDERBOOK_LOOKUP ON ADMIN.MARKET_ORDERBOOK_SNAPSHOTS (TICKER, SNAPSHOT_UTC DESC);
```

## L5: Evaluation Tables

### forecast_errors

Owner: L2. Written by processing/errors.py. Read by L2 (σ computation), L5 (scoring).

```sql
CREATE TABLE ADMIN.FORECAST_ERRORS (
  STATION_ID        VARCHAR2(10),
  SOURCE_ID         VARCHAR2(20),
  TARGET_DATE       DATE,
  TARGET_TYPE       VARCHAR2(4),
  LEAD_BRACKET      VARCHAR2(2),
  LEAD_HOURS        NUMBER(6,1),
  RUN_ID            VARCHAR2(36),
  F_RAW             NUMBER(6,2),
  F_ADJUSTED        NUMBER(6,2),
  OBSERVED          NUMBER(6,2),
  ERROR_RAW         NUMBER(8,4),
  ERROR_ADJUSTED    NUMBER(8,4)
) PARTITION BY RANGE (TARGET_DATE) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION P_INIT VALUES LESS THAN (DATE '2026-04-01'));

CREATE INDEX IDX_FCERR_LOOKUP ON ADMIN.FORECAST_ERRORS (STATION_ID, SOURCE_ID, TARGET_TYPE, LEAD_BRACKET, TARGET_DATE);
```

### brier_scores

Owner: L5. Written by scoring.py. Read by L5.

```sql
CREATE TABLE ADMIN.BRIER_SCORES (
  TICKER            VARCHAR2(100)  NOT NULL,
  P_WIN_AT_GRADING  NUMBER(8,6),
  OUTCOME           NUMBER(1),
  BRIER_SCORE       NUMBER(10,8),
  GRADED_AT         TIMESTAMP(6)   DEFAULT SYSTIMESTAMP,
  CONSTRAINT PK_BRIER_SCORES PRIMARY KEY (TICKER)
);
```

### calibration_history

Owner: L5. Written by calibration.py weekly.

```sql
CREATE TABLE ADMIN.CALIBRATION_HISTORY (
  ID                NUMBER GENERATED ALWAYS AS IDENTITY,
  COMPUTED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP,
  RECORD_TYPE       VARCHAR2(20),
  STATION_ID        VARCHAR2(10),
  WINDOW_DAYS       NUMBER(4,0),
  N_PREDICTIONS     NUMBER(6,0),
  CAL_SYSTEM        NUMBER(8,6),
  CAL_MARKET        NUMBER(8,6),
  BUCKET_DATA_JSON  CLOB,
  PARAM_KEY         VARCHAR2(100),
  OLD_VALUE         VARCHAR2(200),
  NEW_VALUE         VARCHAR2(200),
  BIC_OLD           NUMBER(12,4),
  BIC_NEW           NUMBER(12,4),
  METRIC_TRIGGER    VARCHAR2(200),
  CONSTRAINT PK_CALIBRATION_HISTORY PRIMARY KEY (ID)
);
```

### financial_metrics

Owner: L5. Append-only. Read by L5 for MDD, D_scale.

```sql
CREATE TABLE ADMIN.FINANCIAL_METRICS (
  METRIC_DATE       DATE           NOT NULL,
  COMPUTED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP,
  BANKROLL          NUMBER(12,4),
  PORTFOLIO_VALUE   NUMBER(12,4),
  DAILY_PNL         NUMBER(12,4),
  CUMULATIVE_PNL    NUMBER(12,4),
  MDD_ALLTIME       NUMBER(8,4),
  MDD_ROLLING_90    NUMBER(8,4),
  SR_DOLLAR         NUMBER(8,4),
  SR_SIMPLE         NUMBER(8,4),
  SHARPE_ROLLING_30 NUMBER(8,4),
  FDR               NUMBER(8,4),
  EUR               NUMBER(8,4),
  CAL               NUMBER(8,6),
  MARKET_CAL        NUMBER(8,6),
  N_BETS_TOTAL      NUMBER(8,0),
  N_BETS_WON        NUMBER(8,0),
  N_BETS_LOST       NUMBER(8,0),
  GROSS_PROFIT      NUMBER(12,4),
  NET_PROFIT        NUMBER(12,4),
  TOTAL_FEES        NUMBER(12,4),
  CONSTRAINT PK_FINANCIAL_METRICS PRIMARY KEY (METRIC_DATE)
);
```

### system_alerts

Owner: L5. Written by alerts.py. Read by human/front-end.

```sql
CREATE TABLE ADMIN.SYSTEM_ALERTS (
  ALERT_ID          VARCHAR2(36)   NOT NULL,
  ALERT_TS          TIMESTAMP(6)   DEFAULT SYSTIMESTAMP,
  ALERT_TYPE        VARCHAR2(50),
  STATION_ID        VARCHAR2(10),
  SOURCE_ID         VARCHAR2(20),
  SEVERITY_SCORE    NUMBER(8,4),
  DETAILS_JSON      CLOB,
  IS_RESOLVED       NUMBER(1)      DEFAULT 0,
  RESOLVED_TS       TIMESTAMP(6),
  RESOLVED_BY       VARCHAR2(100),
  CONSTRAINT PK_SYSTEM_ALERTS PRIMARY KEY (ALERT_ID)
);
```

### pipeline_day_health

Owner: L5. One row per observation day.

```sql
CREATE TABLE ADMIN.PIPELINE_DAY_HEALTH (
  TARGET_DATE        DATE           NOT NULL,
  RUN_TS             TIMESTAMP(6),
  STATIONS_ACTIVE    NUMBER(3,0),
  STATIONS_FORECASTED NUMBER(3,0),
  STATIONS_OBSERVED  NUMBER(3,0),
  STATIONS_SCORED    NUMBER(3,0),
  MODELS_ACTIVE      NUMBER(3,0),
  MODELS_INGESTED    NUMBER(3,0),
  IS_HEALTHY         NUMBER(1),
  FAILURE_REASONS_JSON CLOB,
  CONSTRAINT PK_PDH PRIMARY KEY (TARGET_DATE)
);
```

### ibe_signal_log

Owner: L5. Append-only. Written by execution/ibe/engine.py. Read by L5.

```sql
CREATE TABLE ADMIN.IBE_SIGNAL_LOG (
  TICKER            VARCHAR2(100),
  PIPELINE_RUN_ID   VARCHAR2(36),
  KCV_NORM          NUMBER(8,4),
  KCV_MOD           NUMBER(6,4),
  MPDS_K            NUMBER(8,6),
  MPDS_MOD          NUMBER(6,4),
  HMAS              NUMBER(6,4),
  HMAS_MOD          NUMBER(6,4),
  FCT               NUMBER(8,4),
  FCT_MOD           NUMBER(6,4),
  SCAS              NUMBER(8,4),
  SCAS_MOD          NUMBER(6,4),
  COMPOSITE         NUMBER(6,4),
  VETO_TRIGGERED    NUMBER(1),
  VETO_REASON       VARCHAR2(200),
  RECORDED_AT       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP
);
```

## Migration Summary from Current Schema

| Current Table | v10 Action | Reason |
|---------------|-----------|--------|
| DASHBOARD_STATS | **Restructured** | Add TARGET_TYPE, LEAD_BRACKET (replaces LEAD_DAYS), SKEWNESS_SAMPLE, SKEWNESS_UNBIASED columns. Remove KIND (replaced by TARGET_TYPE). |
| FORECASTS_DAILY | **Restructured** | Add SOURCE_ID column. Add partitioning by TARGET_DATE. |
| FORECAST_ERRORS | **Restructured** | Add SOURCE_ID, TARGET_TYPE, LEAD_BRACKET, LEAD_HOURS, F_RAW, F_ADJUSTED columns. Remove FORECAST_RUN_ID/OBSERVATION_RUN_ID (replaced by RUN_ID + SOURCE_ID). |
| FORECAST_EXTRAS_HOURLY | **Renamed → FORECASTS_HOURLY** | Add SOURCE_ID, WEATHERCODE columns. Add partitioning. |
| FORECAST_RUNS | **Restructured** | Add STATION_ID, INIT_TIME, RAW_PAYLOAD_JSON. |
| LOCATIONS | **Renamed → STATIONS** | Add WFO_ID, IS_RELIABLE, RELIABILITY_NOTE, FLAGGED_AT, FORECAST_URL, GRID_URL, HOURLY_URL. |
| OBSERVATIONS | **Restructured** | Add AMENDED, AMENDED_AT, ORIGINAL_HIGH_F, ORIGINAL_LOW_F for amendment tracking. |
| SHADOW_BOOK_AND_BRIER_SCORES | **Split** | → SHADOW_BOOK (L3, pricing) + BRIER_SCORES (L5, evaluation). Fixes the layer boundary violation. |
| *(new)* KALMAN_STATES | **Created** | Per (station, target_type) Kalman state with state_version. |
| *(new)* KALMAN_HISTORY | **Created** | Append-only Kalman step log. |
| *(new)* ENSEMBLE_STATE | **Created** | Per-run ensemble output. |
| *(new)* BSS_MATRIX | **Created** | 20×5×2 skill matrix with hysteresis flags. |
| *(new)* MODEL_WEIGHTS | **Created** | Per-run model weights. |
| *(new)* SHADOW_BOOK_HISTORY | **Created** | Append-only P(win) log for MPDS signal. |
| *(new)* REGIME_FLAGS | **Created** | Bimodal detection log. |
| *(new)* BEST_BETS | **Created** | Materialized gate output. |
| *(new)* POSITIONS | **Created** | Open and historical positions. |
| *(new)* ORDER_LOG | **Created** | Raw Kalshi API submission log. |
| *(new)* MARKET_ORDERBOOK_SNAPSHOTS | **Created** | Pre-execution book snapshots. |
| *(new)* METAR_OBSERVATIONS | **Created** | Live METAR feed. |
| *(new)* METAR_DAILY_MAX | **Created** | Running intraday max/min. |
| *(new)* AFD_TEXT | **Created** | NWS AFD raw text. |
| *(new)* AFD_SIGNALS | **Created** | Parsed AFD confidence signals. |
| *(new)* PARAMS | **Created** | All tunable parameters. |
| *(new)* SOURCES | **Created** | Model source definitions. |
| *(new)* PIPELINE_RUNS | **Created** | Pipeline heartbeat. |
| *(new)* CALIBRATION_HISTORY | **Created** | Weekly CAL records. |
| *(new)* FINANCIAL_METRICS | **Created** | Append-only financial log. |
| *(new)* SYSTEM_ALERTS | **Created** | All human-review triggers. |
| *(new)* PIPELINE_DAY_HEALTH | **Created** | Per-day completeness summary. |
| *(new)* IBE_SIGNAL_LOG | **Created** | Full IBE signal history. |

---

# Section 3: Configuration and Parameters

Every tunable constant in the system is stored in the `PARAMS` database table. Python source files contain bootstrap defaults only — used when the database is unreachable or during initial schema population. At runtime, `config.load_params()` reads the PARAMS table and overrides all bootstrap values.

## 3.1 Parameter Access Pattern

```
def load_params(db_conn) -> dict[str, float | int | str]:
    """
    Read all rows from PARAMS table.
    Returns dict keyed by PARAM_KEY.
    Falls back to BOOTSTRAP_DEFAULTS on connection failure.
    """
```

Every function that consumes a tunable parameter receives it via the `params` dict — never by importing a module-level constant. This ensures the database is the single source of truth and that parameter changes take effect without redeployment.

## 3.2 Complete Parameter Table

### Ensemble Aggregation (L2)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `ensemble.w_m_min_factor` | 0.05 | [0.01, 0.20] | w_m = max(w_m_min_factor / M, BSS_m) | [CALIBRATION REQUIRED] |
| `ensemble.entropy_lambda` | 0.10 | [0.01, 0.50] | L_entropy = -λ Σ w_m ln(w_m) added to BSS objective | [CALIBRATION REQUIRED] |
| `ensemble.staleness_tau` | 3.0 | [1.0, 7.0] | w_m_stale = w_m × exp(-age_hours / (τ × 24)) | Fixed |
| `ensemble.k_spread` | 0.50 | [0.10, 1.00] | σ_eff = √(σ²_{h,s} + k_spread × S²_{t,k}) | [CALIBRATION REQUIRED] |
| `ensemble.min_models` | 3 | [2, 9] | Skip ensemble if fewer than min_models report | Fixed |
| `ensemble.ewm_span` | 90 | [30, 180] | Q_base computed with EWM(span=ewm_span) instead of uniform 180-day | [CALIBRATION REQUIRED] |

### Kalman Filter (L2)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `kalman.R_default` | 0.25 | [0.05, 1.00] | R_default (°F²) — base measurement noise | [CALIBRATION REQUIRED] |
| `kalman.beta` | 2.0 | [0.5, 5.0] | R_k = R_default × (1 + β × max(0, \|F_top - F̄\| / S - 1)) | [CALIBRATION REQUIRED] |
| `kalman.gamma` | 0.10 | [0.01, 0.50] | Q_k = Q_base + γ × Σ v_j (asymmetric innovation penalty) | [CALIBRATION REQUIRED] |
| `kalman.lambda_asym` | 1.5 | [1.0, 3.0] | v_j = λ × d²_j when d_j < 0 (cold bias amplification) | Fixed |
| `kalman.Q_window_days` | 180 | [90, 365] | Rolling window for Q_base computation | Fixed |
| `kalman.gap_inflate_factor` | 2.0 | [1.5, 5.0] | U_k^- = U_{k-1} + gap_inflate_factor × Q_k when state_version gap detected | [CALIBRATION REQUIRED] |
| `kalman.B_init` | 0.0 | [-5.0, 5.0] | Initial bias estimate (°F) | Fixed |
| `kalman.U_init` | 4.0 | [1.0, 10.0] | Initial uncertainty (°F²) | Fixed |

### Sigma and Skewness (L2)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `sigma.m_prior` | 20 | [5, 50] | Bayesian shrinkage: σ_adj = (N×σ_obs + m_prior×σ_global) / (N + m_prior) | [CALIBRATION REQUIRED] |
| `sigma.min_samples` | 10 | [5, 30] | Minimum N before per-station σ replaces global σ | Fixed |
| `sigma.rmse_window_days` | 90 | [30, 180] | Rolling window for RMSE computation | Fixed |
| `skewness.significance_factor` | 2.0 | [1.5, 3.0] | Fallback to normal if \|G1\| < factor × √(6/N) | Fixed |

### Pricing — Skew-Normal (L3)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `pricing.alpha_cap` | 10.0 | [5.0, 20.0] | Clip α_s to [-alpha_cap, +alpha_cap] to prevent numerical instability | Fixed |
| `pricing.p_min_floor` | 0.001 | [0.0001, 0.01] | P(win) floored at p_min_floor to prevent log(0) in Kelly | Fixed |
| `pricing.bimodal_iqr_threshold` | 1.35 | [1.10, 1.60] | Trigger bimodal detection when IQR/S > threshold | [CALIBRATION REQUIRED] |
| `pricing.bimodal_centroid_min` | 1.0 | [0.5, 2.0] | Confirm bimodal only if centroid distance > factor × S_{t,k} | Fixed |

### METAR Truncation (L3)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `metar.truncation_enabled` | 1 | {0, 1} | Master switch for intraday truncation | Fixed |
| `metar.lead_hours_cutoff` | 6.0 | [2.0, 12.0] | Only truncate when lead_hours ≤ cutoff (same-day markets) | Fixed |
| `metar.staleness_minutes` | 120 | [30, 240] | Ignore METAR obs older than this | Fixed |

### Execution — Conviction Gates (L4)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `gate.ev_min_fraction` | 0.025 | [0.01, 0.10] | Minimum EV_net as fraction of bankroll: skip if EV_net < ev_min_fraction × b | [CALIBRATION REQUIRED] |
| `gate.epsilon_edge_base` | 0.03 | [0.01, 0.05] | Base edge buffer: adaptive ε = max(ε_base, 1.96 × √(0.25/N_bets)) | [CALIBRATION REQUIRED] |
| `gate.spread_max` | 4.0 | [2.0, 8.0] | Reject if S_{t,k} > spread_max (°F) — models too dispersed | [CALIBRATION REQUIRED] |
| `gate.bss_enter` | 0.07 | [0.05, 0.15] | Skill Gate entry threshold (hysteresis) | [CALIBRATION REQUIRED] |
| `gate.bss_exit` | 0.03 | [0.01, 0.07] | Skill Gate exit threshold (hysteresis) | [CALIBRATION REQUIRED] |
| `gate.lead_ceiling_hours` | 72.0 | [24.0, 120.0] | Maximum lead hours — reject bets beyond this | Fixed |

### Execution — Kelly Sizing (L4)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `kelly.fraction_cap` | 0.10 | [0.02, 0.25] | Maximum single-bet Kelly fraction of bankroll | Fixed |
| `kelly.min_bet_fraction` | 0.025 | [0.01, 0.05] | Minimum bet as fraction of bankroll; skip if f_final × b < min_bet_fraction × b | Fixed |
| `kelly.jitter_pct` | 0.03 | [0.00, 0.10] | Uniform jitter ±jitter_pct applied to f_final to reduce pattern detection | Fixed |
| `kelly.phi_bss_cap` | 0.25 | [0.15, 0.50] | BSS value at which Φ(BSS) = 1.0 (continuous formula) | [CALIBRATION REQUIRED] |
| `kelly.phi_min` | 0.10 | [0.05, 0.25] | Minimum Φ value above Skill Gate threshold | Fixed |

### Execution — Position Limits (L4)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `position.max_single_fraction` | 0.10 | [0.05, 0.25] | Maximum single position as fraction of bankroll | Fixed |
| `position.max_station_fraction` | 0.25 | [0.10, 0.50] | Maximum total exposure per station as fraction of bankroll | Fixed |
| `position.max_correlated_fraction` | 0.40 | [0.20, 0.60] | Maximum total exposure across correlated stations (same WFO) | Fixed |
| `position.max_total_fraction` | 0.80 | [0.50, 1.00] | Maximum total portfolio exposure as fraction of bankroll | Fixed |
| `position.max_station_day_fraction` | 0.10 | [0.05, 0.20] | Maximum new exposure per station per calendar day as fraction of bankroll | Fixed |

### Execution — VWAP and Order (L4)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `vwap.depth_levels` | 5 | [3, 10] | Order book depth levels to scan for VWAP | Fixed |
| `vwap.staleness_delta` | 0.05 | [0.02, 0.10] | Alert if \|c_VWAP - c_best\| > delta (book is stale) | Fixed |
| `vwap.tranche_threshold` | 50 | [20, 100] | Orders > this many contracts split into tranches | Fixed |
| `vwap.tranche_size` | 25 | [10, 50] | Contracts per tranche | Fixed |
| `vwap.tranche_delay_sec` | 5 | [2, 15] | Seconds between tranche submissions | Fixed |
| `order.maker_timeout_sec` | 300 | [60, 600] | Cancel unfilled maker order after this many seconds | Fixed |
| `order.retry_max` | 3 | [1, 5] | Maximum API submission retries | Fixed |
| `order.retry_backoff_sec` | 2.0 | [1.0, 10.0] | Exponential backoff base for retries | Fixed |

### Execution — IBE Signals (L4)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `ibe.kcv_lookback_days` | 7 | [3, 14] | KCV = \|B_k - B_{k-lookback}\| / lookback | Fixed |
| `ibe.kcv_norm_window` | 90 | [30, 180] | Trailing window for KCV normalization | Fixed |
| `ibe.kcv_veto_threshold` | 4.0 | [3.0, 6.0] | Veto if KCV_normalized > threshold | [CALIBRATION REQUIRED] |
| `ibe.mpds_veto_threshold` | 0.12 | [0.08, 0.20] | Veto if \|MPDS\| > threshold | [CALIBRATION REQUIRED] |
| `ibe.mpds_positive_scale` | 5.0 | [3.0, 8.0] | MPDS modifier scale for positive divergence | [CALIBRATION REQUIRED] |
| `ibe.mpds_negative_scale` | 8.0 | [5.0, 12.0] | MPDS modifier scale for negative divergence | [CALIBRATION REQUIRED] |
| `ibe.hmas_consensus_f` | 1.0 | [0.5, 2.0] | HMAS consensus threshold in °F | Fixed |
| `ibe.fct_veto_threshold` | 1.5 | [1.0, 2.5] | Veto if FCT > threshold (rapid divergence) | [CALIBRATION REQUIRED] |
| `ibe.scas_scale` | 0.15 | [0.05, 0.30] | SCAS modifier = max(0.6, 1 - scale × SCAS) | [CALIBRATION REQUIRED] |
| `ibe.composite_weights` | [0.25, 0.35, 0.15, 0.15, 0.10] | each ∈ [0.05, 0.50], sum=1.0 | Exponents: KCV^w1 × MPDS^w2 × HMAS^w3 × FCT^w4 × SCAS^w5 | [CALIBRATION REQUIRED] |
| `ibe.composite_clip_low` | 0.25 | [0.10, 0.50] | f_conviction ≥ clip_low × f_final | Fixed |
| `ibe.composite_clip_high` | 1.50 | [1.00, 2.00] | f_conviction ≤ clip_high × f_final | Fixed |

### Execution — Fees (L4)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `fee.taker_rate` | 0.07 | [0.0, 0.15] | fee = ⌈F_rate × C × P × (1-P)⌉ | Fixed (Kalshi-set) |
| `fee.maker_rate` | 0.0175 | [0.0, 0.05] | Same formula, maker rate | Fixed (Kalshi-set) |
| `fee.maker_fill_prob_h_half` | 24.0 | [6.0, 48.0] | P_fill(h) = 1 - e^(-h_hours / h_half) | [CALIBRATION REQUIRED] |

### Execution — Market Convergence (L4)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `market.gamma_threshold` | 0.75 | [0.50, 1.00] | Γ = P_market(top) / max(P_market(other)); scale if Γ < threshold | [CALIBRATION REQUIRED] |
| `market.ev_rho` | 0.50 | [0.10, 1.00] | EV_threshold(h) = min_ev × (1 + ρ × h_hours/24) | [CALIBRATION REQUIRED] |

### Execution — Drawdown (L4)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `drawdown.mdd_safe` | 0.10 | [0.05, 0.20] | D_scale begins shrinking at MDD = mdd_safe | Fixed |
| `drawdown.mdd_halt` | 0.20 | [0.10, 0.40] | D_scale = 0 (halt all betting) at MDD = mdd_halt | Fixed |

### Evaluation (L5)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `eval.bss_window_days` | 90 | [30, 180] | Rolling window for BSS matrix computation | Fixed |
| `eval.calibration_buckets` | 10 | [5, 20] | Number of probability buckets for CAL metric | Fixed |
| `eval.sharpe_min_bets` | 20 | [10, 50] | Minimum bets before Sharpe ratio is meaningful | Fixed |
| `eval.adverse_selection_window` | 90 | [30, 180] | Days for maker adverse selection delta test | Fixed |
| `eval.pattern_check_interval_days` | 7 | [1, 14] | BSS pattern classifier frequency | Fixed |

### Pipeline (Orchestration)

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `pipeline.morning_utc_hour` | 12 | [6, 18] | Morning run scheduled UTC hour | Fixed |
| `pipeline.night_utc_hour` | 6 | [0, 12] | Night run scheduled UTC hour | Fixed |
| `pipeline.market_open_utc_hour` | 14 | [12, 16] | Market-open run scheduled UTC hour | Fixed |
| `pipeline.amendment_lookback_days` | 3 | [1, 7] | Re-fetch CLI observations for this many past days | Fixed |
| `pipeline.health_heartbeat_sec` | 300 | [60, 600] | Health check interval | Fixed |
| `pipeline.max_workers` | 10 | [4, 20] | ThreadPoolExecutor worker count | Fixed |
| `pipeline.forecast_days` | 4 | [3, 7] | Days of forecast to collect per run | Fixed |

### Lead Time Brackets

| Parameter Key | Default | Valid Range | Owning Formula | Calibration |
|---------------|---------|-------------|----------------|-------------|
| `lead.h1_max` | 12.0 | [6.0, 18.0] | h1: [0, h1_max) hours | Fixed |
| `lead.h2_max` | 24.0 | [18.0, 30.0] | h2: [h1_max, h2_max) hours | Fixed |
| `lead.h3_max` | 48.0 | [30.0, 54.0] | h3: [h2_max, h3_max) hours | Fixed |
| `lead.h4_max` | 72.0 | [54.0, 78.0] | h4: [h3_max, h4_max) hours | Fixed |
| `lead.h5_max` | 120.0 | [78.0, 168.0] | h5: [h4_max, h5_max) hours | Fixed |

## 3.3 Bootstrap Defaults File

The file `config/params_bootstrap.py` contains a single dict `BOOTSTRAP_DEFAULTS` mirroring every row above. This dict is used:
1. To populate the PARAMS table on first schema initialization.
2. As runtime fallback if the database is unreachable.

The bootstrap file is **generated** from the PARAMS table, never hand-edited. A CLI command `python -m kalshicast.config.export_params` dumps the current PARAMS table to `params_bootstrap.py`.

## 3.4 Parameter Modification Protocol

1. **During development:** Edit the PARAMS table directly via SQL or the admin CLI.
2. **During calibration:** The weekly calibration job (Section 8) may update [CALIBRATION REQUIRED] parameters. Every update is logged to `CALIBRATION_HISTORY` with the old value, new value, and the metric that triggered the change.
3. **In production:** Parameters are read once per pipeline run at the start of `morning.py`, `night.py`, or `market_open.py`. Mid-run parameter changes do not take effect until the next run.

---

# Section 4: Data Collection Layer (L1)

**Owner:** `kalshicast/collection/`
**Writes to:** FORECAST_RUNS, FORECASTS_DAILY, FORECASTS_HOURLY, METAR_OBSERVATIONS, METAR_DAILY_MAX, AFD_TEXT, AFD_SIGNALS
**Reads from:** STATIONS, SOURCES, PARAMS

The collection layer fetches raw forecast data from external APIs and writes it to the database. It has **no knowledge** of ensemble weights, Kalman filters, pricing, or betting. Its sole contract is to produce `ForecastBundle` objects and persist them.

## 4.1 ForecastBundle Contract

Every collector returns a `ForecastBundle`:

```
ForecastBundle:
    source_id:    str               # e.g., "NWS", "OME_GFS"
    station_id:   str               # e.g., "KNYC"
    issued_at:    datetime (UTC)    # truncated to the hour
    daily: list[DailyRow]
    hourly: list[HourlyRow] | None  # None if source provides no hourly data

DailyRow:
    target_date:      date          # local calendar date
    high_f:           float | None  # °F
    low_f:            float | None  # °F
    lead_hours_high:  float         # continuous hours from issued_at to projected high
    lead_hours_low:   float         # continuous hours from issued_at to projected low

HourlyRow:
    valid_time:       datetime (UTC)
    temperature_f:    float | None
    dewpoint_f:       float | None
    humidity_pct:     float | None
    wind_speed_mph:   float | None
    wind_dir_deg:     float | None
    cloud_cover_pct:  float | None
    precip_prob_pct:  float | None
```

**Validation rules enforced by the collector harness before database write:**
- `high_f` ∈ [-60, 140] or None
- `low_f` ∈ [-80, 120] or None
- If both present: `high_f ≥ low_f`
- `lead_hours_high` ∈ [0, 168]
- `len(daily)` ∈ [1, `params.pipeline.forecast_days`]
- `issued_at` is timezone-aware UTC

## 4.2 Common Collector Infrastructure

**File:** `kalshicast/collection/collector_harness.py`

All collectors share:
1. **Global issued_at lock:** A single `issued_at` timestamp, truncated to the hour, is locked at pipeline start and shared across all collectors. This ensures all forecasts from the same run share the same temporal reference.
2. **Concurrency control:** Per-source semaphores limit concurrent requests to prevent rate limiting.
3. **Retry with exponential backoff:** On HTTP 429 or 5xx, retry up to `params.order.retry_max` times with `backoff = retry_backoff_sec × 2^attempt`.
4. **Staleness definition:** A collector is stale if its last successful run_id for a station is older than `2 × update_cycle` hours.
5. **run_id pre-caching:** One `run_id` per source is generated before dispatch, eliminating N×20 redundant FORECAST_RUNS upserts.

**Lead time computation** (shared utility `kalshicast/collection/lead_time.py`):

```
compute_lead_hours(station_tz, issued_at, target_date, kind, hourly_rows=None):
    if hourly_rows is not None:
        local_rows = filter to rows matching target_date in station_tz
        if kind == "high":
            target_hour = hour of max(temperature_f) among local_rows
        else:
            target_hour = hour of min(temperature_f) among local_rows
        target_utc = localize(target_date, target_hour, station_tz).to_utc()
    else:
        # Static fallback anchors
        if kind == "high":
            target_local_hour = 15  # 3:00 PM local
        else:
            target_local_hour = 7   # 7:00 AM local
        target_utc = localize(target_date, target_local_hour, station_tz).to_utc()
    return (target_utc - issued_at).total_seconds() / 3600.0
```

## 4.3 Collector Specifications

### 4.3.1 NWS — National Weather Service

| Property | Value |
|----------|-------|
| **Source ID** | `NWS` |
| **File** | `kalshicast/collection/collectors/collect_nws.py` |
| **Concurrency** | 4 |
| **Update cycle** | 12 hours |
| **Auth** | None (public API) |
| **User-Agent** | Required: `KalshiCast/1.0 (contact@example.com)` |

**Endpoints:**
1. `GET https://api.weather.gov/points/{lat},{lon}` → discovers `forecast`, `forecastGridData`, `forecastHourly` URLs
2. `GET {forecastURL}` → daily periods with `isDaytime`, `temperature` (°F), `name` (e.g., "Monday")
3. `GET {forecastGridDataURL}` → hourly grid: `temperature`, `dewpoint`, `windSpeed`, `windDirection`, `skyCover`, uses ISO 8601 duration intervals (`validTime: "2026-03-22T06:00:00+00:00/PT1H"`)
4. `GET {forecastHourlyURL}` → hourly `precipitationProbability`

**ForecastBundle population:**
- `daily[].high_f`: Maximum temperature from daytime periods
- `daily[].low_f`: Minimum temperature from nighttime periods
- `hourly[].temperature_f`: From gridData, expanded per-hour from duration intervals
- `hourly[].dewpoint_f`: gridData `dewpoint` (°C → °F: `T_f = T_c × 9/5 + 32`)
- `hourly[].wind_speed_mph`: gridData `windSpeed` (km/h → mph: `v_mph = v_kmh / 1.609344`)
- `hourly[].wind_dir_deg`: gridData `windDirection`
- `hourly[].cloud_cover_pct`: gridData `skyCover`
- `hourly[].precip_prob_pct`: From forecastHourly

**Failure handling:** If `/points` returns 404 (coordinates not in NWS coverage), mark station as NWS-unavailable. If `/forecast` returns 500/503, retry; after max retries, log warning and skip — station gets no NWS data for this run.

### 4.3.2 OME_BASE — Open-Meteo Base Model

| Property | Value |
|----------|-------|
| **Source ID** | `OME_BASE` |
| **File** | `kalshicast/collection/collectors/collect_ome.py` |
| **Concurrency** | 2 (shared across all OME_* sources via semaphore, max 4 in-flight) |
| **Update cycle** | 6 hours |
| **Auth** | None (public API, rate-limited) |

**Endpoint:** `GET https://api.open-meteo.com/v1/forecast`

**Query parameters:**
- `latitude={lat}&longitude={lon}`
- `timezone=UTC&temperature_unit=fahrenheit&wind_speed_unit=mph`
- `start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}`
- `daily=temperature_2m_max,temperature_2m_min`
- `hourly=temperature_2m,dew_point_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,cloud_cover,precipitation_probability`
- `models=best` (Open-Meteo auto-selects optimal model per location)

**ForecastBundle population:**
- `daily[].high_f`: `daily.temperature_2m_max`
- `daily[].low_f`: `daily.temperature_2m_min`
- `hourly[]`: Direct mapping from API response arrays (`hourly.time[]`, `hourly.temperature_2m[]`, etc.)

**Failure handling:** Retry on 429 (rate limit) with backoff. On persistent failure, skip — other OME_* sources may still succeed.

### 4.3.3 OME_GFS — Open-Meteo GFS Ensemble

| Property | Value |
|----------|-------|
| **Source ID** | `OME_GFS` |
| **File** | `kalshicast/collection/collectors/collect_ome_model.py` |
| **Concurrency** | Shared OME semaphore (4 max in-flight) |
| **Update cycle** | 6 hours |
| **Auth** | None |

**Endpoint:** Same as OME_BASE with `models=gfs_seamless`

**Note:** Open-Meteo suffixes variable names with the model name (e.g., `temperature_2m_gfs_seamless`). The collector uses `_find_ome_key()` to dynamically discover the correct key.

### 4.3.4 OME_EC — Open-Meteo ECMWF

| Property | Value |
|----------|-------|
| **Source ID** | `OME_EC` |
| **File** | `kalshicast/collection/collectors/collect_ome_model.py` |
| **Concurrency** | Shared OME semaphore |
| **Update cycle** | 12 hours |
| **Auth** | None |

**Endpoint:** Same as OME_BASE with `models=ecmwf_ifs025`

### 4.3.5 OME_ICON — Open-Meteo ICON

| Property | Value |
|----------|-------|
| **Source ID** | `OME_ICON` |
| **File** | `kalshicast/collection/collectors/collect_ome_model.py` |
| **Concurrency** | Shared OME semaphore |
| **Update cycle** | 6 hours |
| **Auth** | None |

**Endpoint:** Same as OME_BASE with `models=icon_seamless`

### 4.3.6 OME_GEM — Open-Meteo GEM

| Property | Value |
|----------|-------|
| **Source ID** | `OME_GEM` |
| **File** | `kalshicast/collection/collectors/collect_ome_model.py` |
| **Concurrency** | Shared OME semaphore |
| **Update cycle** | 12 hours |
| **Auth** | None |

**Endpoint:** Same as OME_BASE with `models=gem_seamless`

### 4.3.7 WAPI — WeatherAPI

| Property | Value |
|----------|-------|
| **Source ID** | `WAPI` |
| **File** | `kalshicast/collection/collectors/collect_wapi.py` |
| **Concurrency** | 2 |
| **Update cycle** | 6 hours |
| **Auth** | API key via query parameter `key={WEATHERAPI_KEY}` (env var: `WEATHERAPI_KEY`) |

**Endpoint:** `GET https://api.weatherapi.com/v1/forecast.json`

**Query parameters:**
- `key={WEATHERAPI_KEY}&q={lat},{lon}&days={forecast_days}&aqi=no&alerts=no`

**ForecastBundle population:**
- `daily[].high_f`: `forecastday[].day.maxtemp_f`
- `daily[].low_f`: `forecastday[].day.mintemp_f`
- `hourly[].temperature_f`: `hour[].temp_f`
- `hourly[].dewpoint_f`: `hour[].dewpoint_f`
- `hourly[].humidity_pct`: `hour[].humidity`
- `hourly[].wind_speed_mph`: `hour[].wind_mph`
- `hourly[].wind_dir_deg`: `hour[].wind_degree`
- `hourly[].cloud_cover_pct`: `hour[].cloud`
- `hourly[].precip_prob_pct`: `hour[].chance_of_rain`

**Known limitation:** WAPI returns ~56–59 of 96 expected hourly slots depending on plan tier. Missing slots are written as NULL.

**Failure handling:** On 401 (invalid key) or 403 (quota exhausted), halt retries and log error. On 429, retry with backoff.

### 4.3.8 VCR — Visual Crossing

| Property | Value |
|----------|-------|
| **Source ID** | `VCR` |
| **File** | `kalshicast/collection/collectors/collect_vcr.py` |
| **Concurrency** | 2 |
| **Update cycle** | 12 hours |
| **Auth** | API key via query parameter `key={VISUALCROSSING_KEY}` (env var: `VISUALCROSSING_KEY`) |

**Endpoint:** `GET https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat},{lon}/{start_date}/{end_date}`

**Query parameters:**
- `unitGroup=us&key={VISUALCROSSING_KEY}&contentType=json&include=days,hours`

**Note:** Requests one extra day beyond `forecast_days` to handle UTC edge clipping. Uses `datetimeEpoch` for hourly timestamp conversion.

**ForecastBundle population:**
- `daily[].high_f`: `days[].tempmax`
- `daily[].low_f`: `days[].tempmin`
- `hourly[].temperature_f`: `hours[].temp`
- `hourly[].dewpoint_f`: `hours[].dew`
- `hourly[].humidity_pct`: `hours[].humidity`
- `hourly[].wind_speed_mph`: `hours[].windspeed`
- `hourly[].wind_dir_deg`: `hours[].winddir`
- `hourly[].cloud_cover_pct`: `hours[].cloudcover`
- `hourly[].precip_prob_pct`: `hours[].precipprob`

**Failure handling:** On 401/429, same as WAPI pattern.

### 4.3.9 TOM — Tomorrow.io

| Property | Value |
|----------|-------|
| **Source ID** | `TOM` |
| **File** | `kalshicast/collection/collectors/collect_tom.py` |
| **Concurrency** | 1 (most aggressive rate limiting among all sources) |
| **Update cycle** | 6 hours |
| **Auth** | API key via query parameter `apikey={TOMORROW_API_KEY}` (env var: `TOMORROW_API_KEY`) |

**Endpoint:** `POST https://api.tomorrow.io/v4/timelines?apikey={TOMORROW_API_KEY}`

**Request body:**
```json
{
    "location": [lat, lon],
    "fields": ["temperatureMax", "temperatureMin", "temperature", "humidity",
               "windSpeed", "windDirection", "cloudCover",
               "precipitationProbability", "dewPoint"],
    "timesteps": ["1d", "1h"],
    "units": "imperial",
    "startTime": "2026-03-22T00:00:00Z",
    "endTime": "2026-03-26T00:00:00Z",
    "timezone": "UTC"
}
```

**Response structure:** `data.timelines[]` with `timestep: "1d"|"1h"`, each containing `intervals[].startTime` and `intervals[].values`.

**ForecastBundle population:**
- `daily[].high_f`: `values.temperatureMax` (from 1d timeline)
- `daily[].low_f`: `values.temperatureMin` (from 1d timeline)
- `hourly[].temperature_f`: `values.temperature` (from 1h timeline)
- `hourly[].dewpoint_f`: `values.dewPoint`
- `hourly[].humidity_pct`: `values.humidity`
- `hourly[].wind_speed_mph`: `values.windSpeed`
- `hourly[].wind_dir_deg`: `values.windDirection`
- `hourly[].cloud_cover_pct`: `values.cloudCover`
- `hourly[].precip_prob_pct`: `values.precipitationProbability`

**Failure handling:** Frequent 429 responses observed. Serialized execution (concurrency=1) with aggressive backoff (10–20s). After max retries, skip station for this source.

## 4.4 METAR Intraday Observation Collector

**This is a new build. It does not exist in the current codebase.**

| Property | Value |
|----------|-------|
| **Source ID** | `METAR` (not a forecast model — does not produce ForecastBundle) |
| **File** | `kalshicast/collection/collectors/collect_metar.py` |
| **Update cycle** | 30 minutes during market hours (09:00–21:00 UTC) |
| **Auth** | None (public API) |

**Purpose:** Fetch live METAR (Meteorological Aerodrome Report) observations for intraday temperature truncation in L3. METAR reports the current actual temperature at each airport station.

**Endpoint:** `GET https://aviationweather.gov/api/data/metar?ids={station_id}&format=json&hours=2`

**Alternative endpoint:** `GET https://api.weather.gov/stations/{station_id}/observations/latest`

**Output contract** (not ForecastBundle — this is an observation, not a forecast):

```
METARObservation:
    station_id:     str           # ICAO code, e.g., "KNYC"
    observed_at:    datetime(UTC) # METAR observation timestamp
    temperature_f:  float         # Current temperature in °F
    dewpoint_f:     float | None
    wind_speed_kt:  float | None
    wind_dir_deg:   float | None
    raw_metar:      str           # Raw METAR string for audit
```

**Database writes:**
1. `METAR_OBSERVATIONS`: Append every observation (append-only log).
2. `METAR_DAILY_MAX`: Upsert running max and min for today's date:
   - `MAX_TEMP_F = max(MAX_TEMP_F, new observation temperature_f)`
   - `MIN_TEMP_F = min(MIN_TEMP_F, new observation temperature_f)`
   - `OBS_COUNT` incremented
   - `LAST_OBS_AT` updated

**Staleness:** A METAR observation is stale if `observed_at` is older than `params.metar.staleness_minutes` from now.

**Failure handling:** METAR is an enhancement, not a requirement. If the API is unavailable, log a warning. L3 pricing proceeds without truncation (full distribution).

**Integration point:** L3 reads `METAR_DAILY_MAX.MAX_TEMP_F` (for high markets) or `METAR_DAILY_MAX.MIN_TEMP_F` (for low markets) as `T_obs_max` for distribution truncation. This is a database-mediated handoff — L1 writes, L3 reads. No direct function call.

## 4.5 AFD Text Signal Collector

**This is a new build. It does not exist in the current codebase.**

| Property | Value |
|----------|-------|
| **Source ID** | `AFD` (not a forecast model — does not produce ForecastBundle) |
| **File** | `kalshicast/collection/collectors/collect_afd.py` |
| **Update cycle** | 6 hours (AFDs are issued 2–4 times per day per WFO) |
| **Auth** | None (public API) |

**Purpose:** Fetch NWS Area Forecast Discussion text and extract qualitative confidence signals. AFDs contain human-written prose from forecasters describing their confidence in the forecast, expected model performance, and notable weather patterns.

**Endpoint:** `GET https://api.weather.gov/products/types/AFD/locations/{wfo_id}`

This returns a list of recent AFD products. Fetch the most recent:
`GET https://api.weather.gov/products/{product_id}`

**WFO mapping:** Each station maps to a Weather Forecast Office (WFO) via the `STATIONS.WFO_ID` column. Multiple stations may share a WFO, so one AFD fetch covers all stations in that WFO's area.

| WFO_ID | Stations |
|--------|----------|
| OKX | KNYC |
| MFL | KMIA |
| LIX | KMSY |
| PHI | KPHL |
| LOT | KMDW |
| LOX | KLAX |
| EWX | KAUS, KSAT |
| BOU | KDEN |
| SEW | KSEA |
| VEF | KLAS |
| MTR | KSFO |
| LWX | KDCA |
| BOX | KBOS |
| FFC | KATL |
| PSR | KPHX |
| FWD | KDFW |
| MPX | KMSP |
| HGX | KHOU |
| OUN | KOKC |

**NLP extraction pipeline:**

```
parse_afd(raw_text: str) -> AFDSignal:
    1. Split text into sections (SYNOPSIS, SHORT TERM, LONG TERM, etc.)
    2. Focus on SHORT TERM section (0-48 hour forecast discussion)
    3. Keyword scan for confidence indicators:

       HIGH_CONFIDENCE_KEYWORDS = [
           "high confidence", "confident", "well-defined",
           "strong agreement", "models agree", "ensemble agreement",
           "high probability", "likely", "expected"
       ]
       LOW_CONFIDENCE_KEYWORDS = [
           "low confidence", "uncertain", "tricky",
           "models disagree", "spread", "diverge",
           "difficult", "challenge", "unclear",
           "could go either way", "depends on"
       ]
       EXTREME_KEYWORDS = [
           "record", "near-record", "dangerously",
           "well above normal", "well below normal",
           "heat advisory", "freeze warning", "frost advisory"
       ]

    4. Compute confidence_score:
       hits_high = count of HIGH_CONFIDENCE_KEYWORDS found
       hits_low  = count of LOW_CONFIDENCE_KEYWORDS found
       confidence_score = (hits_high - hits_low) / max(hits_high + hits_low, 1)
       # Range: [-1.0, +1.0]

    5. Map to sigma modifier:
       if confidence_score > 0.3:   sigma_mod = 0.90  # tighten distribution
       elif confidence_score < -0.3: sigma_mod = 1.15  # widen distribution
       else:                         sigma_mod = 1.00  # neutral

    6. Flag extreme events:
       extreme_flag = any(kw in text.lower() for kw in EXTREME_KEYWORDS)
```

**Output contract:**

```
AFDSignal:
    wfo_id:            str
    fetched_at:        datetime(UTC)
    confidence_score:  float    # [-1.0, +1.0]
    sigma_mod:         float    # [0.85, 1.20]
    extreme_flag:      bool
    keyword_hits:      str      # JSON array of matched keywords for audit
    raw_length:        int      # Character count of raw AFD text
```

**Database writes:**
1. `AFD_TEXT`: Raw AFD text (append-only, for audit and future NLP improvement).
2. `AFD_SIGNALS`: Parsed signal row per WFO per fetch.

**Integration point:** L2 reads `AFD_SIGNALS.SIGMA_MOD` for the station's WFO. If available and fresh (within 12 hours), it multiplies into σ_eff:
```
σ_eff_final = σ_eff × sigma_mod
```

**Failure handling:** AFD is an enhancement. If unavailable, `sigma_mod = 1.0` (no effect). Log warning.

## 4.6 CLI Observation Collector

**File:** `kalshicast/collection/collectors/collect_cli.py` (refactored from existing `cli_observations.py`)

The CLI (Climate) product is the official NWS daily temperature observation. It is the **authoritative observation source** used for error computation and Kalman updates.

**Endpoint:** `GET https://api.weather.gov/products/types/CLI/locations/{cli_site}`

Each station has a `cli_site` mapping (e.g., KNYC → NYC, KMIA → MIA). The collector:
1. Fetches the product list for the target date.
2. Parses the raw text using regex to extract MAXIMUM and MINIMUM temperature.
3. Handles letter-suffixed values (e.g., "35R" = record) by stripping the suffix.
4. Cross-validates against NWS station observations (`/stations/{id}/observations`) if available.
5. Writes to `OBSERVATIONS` table.

**Amendment detection:** CLI products can be amended up to 3 days after initial issuance. The pipeline re-fetches CLI for the past `params.pipeline.amendment_lookback_days` days on every night run. If the observed value changes, the `AMENDED` flag is set, `ORIGINAL_HIGH_F` / `ORIGINAL_LOW_F` preserve the pre-amendment values, and a retroactive Kalman correction is triggered (Section 5).

## 4.7 Database Write Patterns

All L1 database writes use Oracle `MERGE` (upsert) statements with `/*+ NO_PARALLEL(tgt) */` hints for free-tier compatibility.

**Batching:**
- `FORECASTS_DAILY`: 500 rows per batch
- `FORECASTS_HOURLY`: 200 rows per batch
- `METAR_OBSERVATIONS`: Single-row INSERT (low volume)
- Single COMMIT per source per station group

**run_id generation:** `SYS_GUID()` formatted as lowercase dashed UUID. One run_id per source per pipeline run (not per station).

---

# Section 5: Processing Layer (L2)

**Owner:** `kalshicast/processing/`
**Writes to:** ENSEMBLE_STATE, MODEL_WEIGHTS, KALMAN_STATES, KALMAN_HISTORY, FORECAST_ERRORS, DASHBOARD_STATS, REGIME_FLAGS
**Reads from:** FORECASTS_DAILY, FORECASTS_HOURLY, OBSERVATIONS, BSS_MATRIX, SOURCES, STATIONS, PARAMS, AFD_SIGNALS

The processing layer transforms raw forecasts into the three quantities consumed by L3 (Pricing):
1. **μ** — bias-corrected location estimate (Kalman filter output)
2. **σ_eff** — effective scale (RMSE + spread + AFD modifier)
3. **G1_s** — unbiased sample skewness

It also computes ensemble weights, error statistics, and the lead-time classification used by all downstream layers.

## 5.1 Lead-Time Classification

**Function:** `classify_lead_hours(h_float: float) -> str`

Maps a continuous float (hours from issued_at to projected high/low) to one of five discrete brackets:

```
def classify_lead_hours(h_float: float, params: dict) -> str:
    if h_float < 0:
        raise ValueError(f"Negative lead time: {h_float}")
    if h_float < params["lead.h1_max"]:      # default 12.0
        return "h1"
    elif h_float < params["lead.h2_max"]:     # default 24.0
        return "h2"
    elif h_float < params["lead.h3_max"]:     # default 48.0
        return "h3"
    elif h_float < params["lead.h4_max"]:     # default 72.0
        return "h4"
    elif h_float < params["lead.h5_max"]:     # default 120.0
        return "h5"
    else:
        return "h5"  # clamp to h5 for ultra-long-range
```

**Default brackets:**
| Bracket | Range (hours) | Interpretation |
|---------|---------------|----------------|
| h1 | [0, 12) | Same-day |
| h2 | [12, 24) | Next-day |
| h3 | [24, 48) | 2-day |
| h4 | [48, 72) | 3-day |
| h5 | [72, 120) | Extended range |

The bracket string is stored in `FORECAST_ERRORS.LEAD_BRACKET` and is the row dimension of the BSS skill matrix.

## 5.2 Error Computation

**File:** `kalshicast/processing/errors.py`

For each (source, station, target_date, target_type) tuple where both a forecast and an observation exist:

```
E_{m,s,t,k} = F_{m,s,t,k} - O_{s,t,k}
```

Where:
- F_{m,s,t,k} = forecast from model m, station s, target date t, type k ∈ {high, low}
- O_{s,t,k} = observed value from CLI product
- E > 0 means model predicted too hot; E < 0 means too cold

**Database write:** One row per error in `FORECAST_ERRORS`:
- `RUN_ID`, `SOURCE_ID`, `STATION_ID`, `TARGET_DATE`, `TARGET_TYPE`
- `LEAD_BRACKET` = classify_lead_hours(LEAD_HOURS)
- `LEAD_HOURS` = continuous float (preserved for analysis)
- `F_RAW` = raw forecast value
- `F_ADJUSTED` = Kalman-corrected forecast (filled after Kalman step)
- `ERROR_RAW` = F_RAW - O
- `ERROR_ADJUSTED` = F_ADJUSTED - O

## 5.3 RMSE and Bayesian Shrinkage (σ computation)

**File:** `kalshicast/processing/sigma.py`

### 5.3.1 Per-Model RMSE

For model m, station s, lead bracket h, target type k, over the trailing `params.sigma.rmse_window_days`:

```
σ_{m,s,h,k} = √[(1/N) × Σᵢ (E_adjusted_{m,s,i,k})²]
```

Where E_adjusted uses the Kalman-corrected errors (not raw errors). N is the count of errors in the window for that (m, s, h, k) tuple.

### 5.3.2 Global RMSE

Pooled across all stations for a given (model, lead_bracket, target_type):

```
σ_global_{m,h,k} = √[(1/N_global) × Σ_s Σᵢ (E_adjusted_{m,s,i,k})²]
```

**Outlier exclusion:** Before pooling, exclude stations where σ_{m,s,h,k} > 2 × their own 90-day rolling mean σ. This prevents a single malfunctioning station from inflating the global estimate that other stations shrink toward.

**Aggregation method:** Use the rolling median across station-level σ values rather than the mean. The median is robust to the remaining outliers after exclusion.

### 5.3.3 Bayesian Shrinkage

When per-station sample size is small, shrink toward the global estimate:

```
σ_{m,s,h,k,adj} = (N × σ_{m,s,h,k} + m_prior × σ_global_{m,h,k}) / (N + m_prior)
```

Where `m_prior` = `params.sigma.m_prior` (default 20).

**Effect:** With N=0 observations, σ equals the global estimate. As N grows, the station-specific estimate dominates. At N=20 (equal to m_prior), the estimate is a 50/50 blend.

### 5.3.4 Aggregate σ for Pricing

The σ used in L3 pricing is not per-model — it is the ensemble-level RMSE. Computed from the Kalman-corrected top-model errors:

```
σ_{h,s,k} = √[(1/N) × Σᵢ (F_top_{s,i,k} + B_k - O_{s,i,k})²]
```

Where F_top is the forecast from the top model (see §5.5) and B_k is the current Kalman bias estimate.

With Bayesian shrinkage:

```
σ_{h,s,k,adj} = (N × σ_{h,s,k} + m_prior × σ_global_{h,k}) / (N + m_prior)
```

### 5.3.5 Zero-Variance Fallback

If σ_{h,s,k} = 0 (all errors are exactly zero — can happen with N < 3):

```
σ_{h,s,k} = σ_global_{h,k}
```

If σ_global_{h,k} is also zero (impossible in practice but defensive): σ = 2.0 °F.

## 5.4 Skewness Computation

**File:** `kalshicast/processing/skewness.py`

### 5.4.1 Zero-Variance Fallback

**Before any skewness computation:**

```
IF sample_variance = 0 THEN G1_s = 0.0; RETURN
```

This catches the degenerate case where all errors are identical (including N < 3). The distribution falls back to a symmetric normal.

### 5.4.2 Biased Sample Skewness (g_s)

Computed over the **top model's** Kalman-corrected errors for (station s, lead_bracket h, target_type k), matching the same error distribution used for σ (§5.3.4):

```
g_s = [(1/N) × Σᵢ (Eᵢ - Ē)³] / [(1/N) × Σᵢ (Eᵢ - Ē)²]^(3/2)
```

Where Eᵢ = (F_top_{s,i,k} + B_k - O_{s,i,k}) and Ē is their sample mean, over the trailing `params.sigma.rmse_window_days`.

### 5.4.3 Unbiased Sample Skewness (G1_s)

The operative skewness value used in all downstream formulas:

```
G1_s = [√(N(N-1)) / (N-2)] × g_s
```

**Minimum N:** Requires N ≥ 3 (otherwise division by zero in N-2). For N < 3, G1_s = 0.0.

### 5.4.4 Significance Test

If the skewness is not statistically significant, fall back to normal distribution:

```
IF |G1_s| < params.skewness.significance_factor × √(6/N) THEN:
    G1_s = 0.0   # Forces α = 0 in skew-normal → pure normal
```

Default `significance_factor` = 2.0, so the threshold is 2 × √(6/N). At N=30 this is ≈ 0.894; at N=90 it is ≈ 0.516.

## 5.5 Ensemble Aggregation

**File:** `kalshicast/processing/ensemble.py`

### 5.5.1 Decoupled Location-Scale Architecture

v10 retains the v9 decoupled architecture:
- **Location (μ input):** The single best-performing model (top model) feeds the Kalman filter. Not the ensemble mean.
- **Scale (σ input):** All models contribute via ensemble spread.

**Rationale:** The ensemble mean dilutes the signal from the best model. The Kalman filter is designed to track the bias of a single model. Mixing models at the location level defeats this purpose.

### 5.5.2 Top-Model Selection

For each (station s, lead_bracket h, target_type k):

```
top_model_{s,h,k} = argmax_m [BSS_{m,s,h,k}]
```

Where BSS_{m,s,h,k} is the per-model Brier Skill Score from the BSS matrix (Section 8). Ties are broken by most recent error magnitude (lower wins).

The top model's raw forecast becomes the Kalman filter input:

```
F_top_{s,t,k} = F_{top_model,s,t,k}
```

### 5.5.3 BSS-Weighted Ensemble Mean (for Spread Only)

The ensemble mean is computed for the spread calculation (not for location):

```
F̄_{t,k} = (1/M_k) × Σ_m F_{m,t,k}
```

Where M_k is the count of active models that reported for this (station, target_date, target_type). Requires M_k ≥ `params.ensemble.min_models` (default 3).

### 5.5.4 Weight Computation with Entropy Regularization

Model weights determine the weighted spread (§5.5.6) and are stored for audit. The weight for model m is:

**Without entropy regularization (v9 baseline):**
```
w_m = max(w_m_min, BSS_{m,s,h,k})
```
Where `w_m_min = params.ensemble.w_m_min_factor / M` (default 0.05/M).

**With entropy regularization (v10):**

Weights are computed by maximizing the regularized objective:

```
Maximize: Σ_m w_m × BSS_m + λ × H(w)
Subject to: Σ_m w_m = 1, w_m ≥ w_m_min ∀m

Where H(w) = -Σ_m w_m × ln(w_m) is the Shannon entropy of the weight vector.
```

`λ` = `params.ensemble.entropy_lambda` (default 0.10).

**Practical implementation:** Since M ≤ 9 and the objective is concave, solve via scipy.optimize.minimize with method='SLSQP':

```
from scipy.optimize import minimize

def compute_weights(bss_scores: list[float], lambda_ent: float, w_min: float) -> list[float]:
    M = len(bss_scores)
    def neg_objective(w):
        bss_term = -sum(w[i] * bss_scores[i] for i in range(M))
        entropy_term = lambda_ent * sum(w[i] * log(w[i]) for i in range(M) if w[i] > 0)
        return bss_term + entropy_term  # minimize negative = maximize positive

    constraints = [{'type': 'eq', 'fun': lambda w: sum(w) - 1.0}]
    bounds = [(w_min, 1.0)] * M
    w0 = [1.0 / M] * M
    result = minimize(neg_objective, w0, method='SLSQP', bounds=bounds, constraints=constraints)
    return result.x.tolist()
```

**Effect of λ:** At λ=0, this reduces to the v9 behavior (highest-BSS model gets maximum weight). As λ increases, weights become more uniform. At λ→∞, all weights converge to 1/M. The default λ=0.10 provides moderate regularization.

**Fallback:** If the optimizer fails to converge, fall back to uniform weights: w_m = 1/M.

### 5.5.5 Staleness Decay

If a model has not reported in the current run (its forecast is stale), decay its weight:

```
w_m_stale = w_m × exp(-age_hours / (τ × 24))
```

Where `τ` = `params.ensemble.staleness_tau` (default 3.0 days) and `age_hours` is the time since the model's last successful forecast for this station.

After decay, renormalize: `w_m = w_m_stale / Σ w_m_stale`.

### 5.5.6 Ensemble Spread

**Unweighted spread** (used in Spread Gate):

```
S_{t,k} = √[(1/(M_k - 1)) × Σ_m (F_{m,t,k} - F̄_{t,k})²]
```

Bessel correction (M_k - 1) is mandatory. If M_k = 1, S_{t,k} = 0 and the Spread Gate rejects the bet.

**BSS-weighted spread** (used in σ_eff):

```
S_weighted_{t,k} = √[Σ_m w_m × (F_{m,t,k} - F̄_weighted_{t,k})²]
```

Where F̄_weighted_{t,k} = Σ_m w_m × F_{m,t,k}.

### 5.5.7 Effective Standard Deviation (σ_eff)

```
σ_eff = √(σ²_{h,s,k,adj} + k_spread × S²_weighted_{t,k})
```

Where `k_spread` = `params.ensemble.k_spread` (default 0.50).

**AFD modifier** (if available):

```
σ_eff_final = σ_eff × sigma_mod
```

Where `sigma_mod` is read from `AFD_SIGNALS` for the station's WFO. If no fresh AFD signal exists, `sigma_mod = 1.0`.

### 5.5.8 Exponentially Weighted Q_base

The Kalman process noise Q_base (§5.6) uses an exponentially weighted moving variance instead of a uniform rolling window:

```
Q_base = EWM_variance(ΔB series, span=params.ensemble.ewm_span)
```

Where `ΔB_i = B_i - B_{i-1}` is the daily change in Kalman bias. Default `ewm_span` = 90 days.

**Rationale:** Uniform 180-day windows include stale seasonal data. EWM with span=90 half-weights data from ~60 days ago, adapting faster to regime changes.

## 5.6 Kalman Filter

**File:** `kalshicast/processing/kalman.py`

A 1-dimensional Kalman filter operates per (station, target_type) pair — 40 independent filters total (20 stations × 2 types). The filter tracks the systematic bias of the top model's forecasts.

### 5.6.1 State Definition

```
State vector:  B_k   (scalar — estimated forecast bias in °F)
Uncertainty:   U_k   (scalar — estimate variance in °F²)
Version:       state_version_k  (integer — monotonically increasing)
```

**Initialization:** B_0 = `params.kalman.B_init` (default 0.0), U_0 = `params.kalman.U_init` (default 4.0), state_version = 1.

### 5.6.2 Predict Step

```
B_k⁻ = B_{k-1}           # Bias persists (random walk model)
U_k⁻ = U_{k-1} + Q_k     # Uncertainty grows by process noise
```

### 5.6.3 Measurement Noise (R_k)

Dynamic R_k inflates when the top model disagrees with the ensemble:

```
R_k = R_default × (1 + β × max(0, |F_top_{s,t,k} - F̄_{t,k}| / S_{t,k} - 1))
```

Where:
- `R_default` = `params.kalman.R_default` (default 0.25 °F²)
- `β` = `params.kalman.beta` (default 2.0)
- `F_top` is the top model forecast, `F̄` is the unweighted ensemble mean, `S` is the unweighted spread

**Interpretation:** When the top model is within 1 standard deviation of the ensemble mean, R_k = R_default. When it deviates, R_k increases, making the filter more conservative (trusts the observation less).

**Edge case:** If S_{t,k} = 0 (single model or all models agree exactly), use R_k = R_default.

### 5.6.4 Process Noise (Q_k)

```
Q_k = Q_base + γ × Σ_{j=1}^{J} v_j
```

Where:
- `Q_base` = EWM variance of ΔB series (§5.5.8)
- `γ` = `params.kalman.gamma` (default 0.10)
- J = min(5, number of completed observation-forecast pairs in the last 7 days)
- v_j is asymmetric:
  ```
  v_j = d_j²                         if d_j ≥ 0  (warm bias)
  v_j = λ_asym × d_j²               if d_j < 0  (cold bias)
  ```
- `d_j = ε_j - B_{k-1}` (innovation residual after bias removal)
- `λ_asym` = `params.kalman.lambda_asym` (default 1.5)

**Rationale for asymmetry:** Cold bias (forecast too low) is more operationally dangerous for temperature markets — it causes the system to underprice high bins. The asymmetric penalty makes the filter respond faster to cold bias.

### 5.6.5 Update Step

**Innovation:**
```
ε_{k} = O_{s,t,k} - F_top_{s,t,k}
```

Where O is the observed temperature (from CLI product) and F_top is the top model's raw forecast for that (station, date, type).

**Kalman Gain:**
```
K_k = U_k⁻ / (U_k⁻ + R_k)
```

**Bias Update:**
```
B_k = B_k⁻ + K_k × (ε_k - B_k⁻)
```

**Uncertainty Update:**
```
U_k = (1 - K_k) × U_k⁻
```

**Version Increment:**
```
state_version_k = state_version_{k-1} + 1
```

### 5.6.6 Bias Application

The corrected forecast (location estimate for L3):

```
μ = F_top_{s,t,k} + B_k
```

This μ, together with σ_eff_final and G1_s, forms the complete input triple for the Skew-Normal parameterization in Section 6.

### 5.6.7 State Version Gap Detection and Recovery

The Oracle free tier has transient connection failures. If a Kalman update is missed (e.g., the night pipeline fails), the next successful update must detect and compensate for the gap.

**Detection:**
```
expected_version = state_version_{k-1} + 1
actual_gap = expected_version_from_date_sequence - state_version_{k-1}
gap_detected = actual_gap > 1
```

In practice: the pipeline tracks the last `TARGET_DATE` for which a Kalman update was performed. If the current target date is more than 1 calendar day after the last update date, a gap exists.

**Recovery (Option B from v9_thinking — adopted):**

When a gap is detected, inflate the uncertainty to reflect the missing updates:

```
U_k⁻ = U_{k-1} + gap_inflate_factor × Q_k × gap_days
```

Where:
- `gap_inflate_factor` = `params.kalman.gap_inflate_factor` (default 2.0)
- `gap_days` = number of missed calendar days

**Effect:** The inflated uncertainty makes K_k larger, so the filter trusts the next observation more heavily. This is the correct behavior — after a gap, the bias estimate is stale and should be overwritten more aggressively by new data.

**Logging:** Every gap detection is logged to `SYSTEM_ALERTS` with severity `WARNING`.

### 5.6.8 Amendment Retroactive Correction

When a CLI observation is amended (§4.6), the Kalman filter must retroactively correct:

1. Read `KALMAN_HISTORY` for the affected (station, target_type, target_date).
2. Recompute ε_k using the amended observation.
3. Recompute B_k and U_k from that step forward (replay subsequent steps).
4. Update `KALMAN_STATES` with the corrected current state.
5. Log the amendment correction to `KALMAN_HISTORY` with `IS_AMENDMENT = 1`.

**Scope limit:** Retroactive correction is limited to `params.pipeline.amendment_lookback_days` (default 3 days). Amendments older than this are logged but do not trigger Kalman replay.

### 5.6.9 Database Persistence

**KALMAN_STATES** (one row per station × target_type — 40 rows total):
- Upserted after every update step.
- Contains: `STATION_ID`, `TARGET_TYPE`, `B_K` (bias estimate), `U_K` (uncertainty), `Q_BASE`, `STATE_VERSION`, `LAST_OBSERVATION_DATE`, `LAST_UPDATED_UTC`.

**KALMAN_HISTORY** (append-only log):
- One row per update step.
- Contains: `STATION_ID`, `TARGET_TYPE`, `PIPELINE_RUN_ID`, `B_K`, `U_K`, `Q_K`, `R_K`, `K_K` (Kalman gain), `EPSILON_K` (innovation), `STATE_VERSION`, `CREATED_AT`.

## 5.7 Bimodal Regime Detection

**File:** `kalshicast/processing/regime.py`

**Status: ARCHITECTURE DEFINED — NOT LIVE.** The bimodal detector is specified but not activated until 90 days of forecast spread data exist for validation.

**Trigger condition:**

For a given (station, target_date, target_type) with M_k active model forecasts:

```
IQR = Q3 - Q1 of {F_{1,t,k}, F_{2,t,k}, ..., F_{M,t,k}}
IF IQR / S_{t,k} > params.pricing.bimodal_iqr_threshold THEN:
    Run bimodal detection
```

Default `bimodal_iqr_threshold` = 1.35.

**Detection method:**

1. Run 1D K-means (k=2) on the M_k forecast values.
2. Compute centroid distance: `d_centroid = |c₁ - c₂|`
3. If `d_centroid > params.pricing.bimodal_centroid_min × S_{t,k}`:
   - Confirm bimodal regime.
   - Write to `REGIME_FLAGS` table: `STATION_ID`, `TARGET_DATE`, `TARGET_TYPE`, `IS_BIMODAL=1`, `CENTROID_1`, `CENTROID_2`, `CLUSTER_SIZES`.
4. Else: single-mode distribution; `IS_BIMODAL=0`.

**L3 impact (when activated):** If bimodal, L3 uses a mixture-of-normals instead of a single skew-normal. See Section 6.

## 5.8 Dashboard Statistics

**File:** `kalshicast/processing/dashboard.py`

After error computation, rolling dashboard statistics are updated for each (station, source, target_type, lead_bracket) over multiple windows:

**Windows:** [2, 3, 7, 14, 30, 90] days.

**Metrics per window:**
- `N`: count of errors
- `BIAS`: mean(E)
- `MAE`: mean(|E|)
- `RMSE`: √(mean(E²))
- `P10`, `P50`, `P90`: percentiles of E
- `PCT_WITHIN_0_5F`: fraction of |E| ≤ 0.5
- `PCT_WITHIN_1F`: fraction of |E| ≤ 1.0
- `PCT_OVER`: fraction of E > 0
- `PCT_UNDER`: fraction of E < 0
- `SKEWNESS_SAMPLE`: g_s (biased)
- `SKEWNESS_UNBIASED`: G1_s (unbiased)

**Database write:** MERGE into `DASHBOARD_STATS`. This data powers the monitoring dashboard but is not consumed by any pricing or execution formula.

---

# Section 6: Pricing Layer (L3)

**Owner:** `kalshicast/pricing/`
**Writes to:** SHADOW_BOOK, SHADOW_BOOK_HISTORY
**Reads from:** KALMAN_STATES, ENSEMBLE_STATE, DASHBOARD_STATS, METAR_DAILY_MAX, REGIME_FLAGS, PARAMS

The pricing layer converts the (μ, σ_eff, G1_s) triple from L2 into a probability for every active Kalshi temperature bin. Its output — the Shadow Book — is the system's probabilistic view of where the temperature will land.

## 6.1 Skew-Normal Parameterization

**File:** `kalshicast/pricing/shadow_book.py`

The system models the temperature distribution as a Skew-Normal using `scipy.stats.skewnorm`. The Skew-Normal PDF is:

```
f(x) = (2/ω) × φ((x-ξ)/ω) × Φ(α × (x-ξ)/ω)
```

Where φ is the standard normal PDF, Φ is the standard normal CDF, and (ξ, ω, α) are the location, scale, and shape parameters respectively.

**scipy call convention:**
```python
from scipy.stats import skewnorm
p = skewnorm.cdf(x, a=α_s, loc=ξ_s, scale=ω_s)
```

### 6.1.1 Four-Step Conversion: (μ, σ_eff, G1_s) → (ξ, ω, α)

**Input:** μ (Kalman-corrected location, °F), σ_eff (effective standard deviation, °F), G1_s (unbiased sample skewness, dimensionless).

**Step 1 — Compute δ from G1_s:**

```
K = (|G1_s| × 2 / (4 - π))^(2/3)
δ² = (K × π/2) / (1 + K)
δ_s = sign(G1_s) × √(δ²)
```

**Numerical guard:** The skew-normal can only represent skewness values in the range (-0.9953, +0.9953). If |G1_s| ≥ 0.9953, clamp to ±0.9952. This prevents δ² ≥ 1.

**Step 2 — Compute α (shape parameter):**

```
α_s = δ_s / √(1 - δ_s²)
```

**Numerical guard:** Clip α_s to [-`params.pricing.alpha_cap`, +`params.pricing.alpha_cap`] (default ±10.0). Extreme α values cause numerical instability in scipy.

**Step 3 — Compute ω (scale parameter):**

```
ω_s = σ_eff / √(1 - 2δ_s²/π)
```

**Guard:** ω_s must be positive. If the denominator is ≤ 0 (cannot happen if δ² < 1, but defensive), use ω_s = σ_eff.

**Step 4 — Compute ξ (location parameter):**

```
ξ_s = μ - ω_s × δ_s × √(2/π)
```

**Verification:** After conversion, the skew-normal distribution with parameters (ξ_s, ω_s, α_s) has:
- Mean ≈ μ
- Std dev ≈ σ_eff
- Skewness ≈ G1_s

These are approximate equalities because the conversion formulas use moment-matching, which is exact only for the first three moments.

### 6.1.2 Normal Distribution Fallback

When G1_s = 0 (either by measurement or by significance test fallback):
- α_s = 0
- δ_s = 0
- ω_s = σ_eff
- ξ_s = μ

In this case, `skewnorm.cdf(x, a=0, loc=μ, scale=σ_eff)` is exactly `norm.cdf(x, loc=μ, scale=σ_eff)`.

## 6.2 Bin Boundary Convention

Kalshi temperature contracts use integer-labeled bins. The system must map each ticker to a half-open interval in °F.

### 6.2.1 Ticker-to-Boundary Conversion

A Kalshi temperature ticker encodes a station, date, target type, and temperature range. The bin label "[81 to 82]" resolves to:

```
bin_lower = label_low - 0.5  = 80.5 °F
bin_upper = label_high + 0.5 = 82.5 °F
```

**General rule:**
```
bin_lower = ticker_lower_bound - 0.5
bin_upper = ticker_upper_bound + 0.5
```

**Half-open interval:** The bin covers `[bin_lower, bin_upper)` — inclusive lower, exclusive upper.

### 6.2.2 Tail Bins

- **Lowest bin** (e.g., "≤60"): `bin_lower = -∞, bin_upper = 60.5`
- **Highest bin** (e.g., "≥90"): `bin_lower = 89.5, bin_upper = +∞`

In the CDF:
- Lowest bin: `P(win) = skewnorm.cdf(bin_upper, a=α_s, loc=ξ_s, scale=ω_s)`
- Highest bin: `P(win) = 1.0 - skewnorm.cdf(bin_lower, a=α_s, loc=ξ_s, scale=ω_s)`

### 6.2.3 Bin Width

Kalshi daily temperature bins are typically 2°F wide. The system does not hardcode this — it reads the actual boundaries from the ticker. The bin width may change if Kalshi adjusts its market structure.

## 6.3 P(win) Computation

For an interior bin with boundaries [a, b):

```
P(win) = skewnorm.cdf(b, a=α_s, loc=ξ_s, scale=ω_s) - skewnorm.cdf(a, a=α_s, loc=ξ_s, scale=ω_s)
```

**Floor:** P(win) is floored at `params.pricing.p_min_floor` (default 0.001) to prevent log(0) in the Kelly criterion.

**Normalization:** After computing P(win) for all bins in a market, verify that they sum to approximately 1.0:

```
total = Σ_bins P(win)
IF |total - 1.0| > 0.01 THEN:
    Log WARNING: "P(win) sum = {total}, expected ~1.0"
    Normalize: P(win)_i = P(win)_i / total
```

Small deviations (< 0.01) are expected due to floating-point precision and the p_min_floor. Larger deviations indicate a parameterization error.

## 6.4 METAR Truncation

**File:** `kalshicast/pricing/truncation.py`

When live intraday observations are available from METAR, the distribution can be truncated to reflect known information. If the temperature has already reached 85°F today, the high temperature is at least 85°F — bins below 85°F should get P(win) = 0.

### 6.4.1 Trigger Condition

METAR truncation applies when ALL of the following are true:
1. `params.metar.truncation_enabled` = 1
2. The market is for today's date (same calendar day in station's local timezone)
3. Lead time ≤ `params.metar.lead_hours_cutoff` (default 6.0 hours)
4. A fresh METAR observation exists: `METAR_DAILY_MAX.LAST_OBS_AT` is within `params.metar.staleness_minutes` of now

### 6.4.2 Truncation for High Temperature Markets

Read `T_obs_max` = `METAR_DAILY_MAX.MAX_TEMP_F` for the station and today's date. The daily high must be ≥ T_obs_max.

For each bin [a, b):

```
a_truncated = max(a, T_obs_max)
```

**Case 1:** `a_truncated ≥ b` → P(win) = 0 (this bin is impossible — the high is already above the entire bin).

**Case 2:** `a_truncated < b` and `a_truncated > a` → Truncated probability:

```
P(win) = [skewnorm.cdf(b, α_s, ξ_s, ω_s) - skewnorm.cdf(a_truncated, α_s, ξ_s, ω_s)] / skewnorm.sf(T_obs_max, α_s, ξ_s, ω_s)
```

Where `sf` is the survival function (1 - cdf). The denominator renormalizes the distribution conditional on T ≥ T_obs_max.

**Case 3:** `a_truncated == a` (observation below this bin) → No truncation effect on this specific bin, but the renormalization denominator still applies:

```
P(win) = [skewnorm.cdf(b, α_s, ξ_s, ω_s) - skewnorm.cdf(a, α_s, ξ_s, ω_s)] / skewnorm.sf(T_obs_max, α_s, ξ_s, ω_s)
```

### 6.4.3 Truncation for Low Temperature Markets

For low temperature markets, the relevant METAR value is `T_obs_min` = `METAR_DAILY_MAX.MIN_TEMP_F`. The daily low must be ≤ T_obs_min (any temperature observed so far is an upper bound on the minimum).

For each bin [a, b):

```
b_truncated = min(b, T_obs_min)
```

**Case 1:** `b_truncated ≤ a` → P(win) = 0.

**Case 2:** `b_truncated < b` and `b_truncated > a` → Truncated:

```
P(win) = [skewnorm.cdf(b_truncated, α_s, ξ_s, ω_s) - skewnorm.cdf(a, α_s, ξ_s, ω_s)] / skewnorm.cdf(T_obs_min, α_s, ξ_s, ω_s)
```

**Case 3:** `b_truncated == b` → Renormalization only:

```
P(win) = [skewnorm.cdf(b, α_s, ξ_s, ω_s) - skewnorm.cdf(a, α_s, ξ_s, ω_s)] / skewnorm.cdf(T_obs_min, α_s, ξ_s, ω_s)
```

### 6.4.4 Post-Truncation Normalization

After truncation, re-floor all P(win) at `p_min_floor` and re-verify that the sum across bins ≈ 1.0. Renormalize if necessary.

## 6.5 Bimodal Regime Pricing

**Status: ARCHITECTURE DEFINED — NOT LIVE.** Activated only when `REGIME_FLAGS.IS_BIMODAL = 1` for the current (station, target_date, target_type).

When the bimodal detector (§5.7) confirms a bimodal distribution:

### 6.5.1 Mixture-of-Normals Model

Replace the single skew-normal with a two-component Gaussian mixture:

```
f(x) = π₁ × N(x; μ₁, σ₁²) + π₂ × N(x; μ₂, σ₂²)
```

Where:
- μ₁, μ₂ = cluster centroids from K-means (stored in REGIME_FLAGS)
- σ₁ = σ₂ = σ_eff (same scale for both components — simplification)
- π₁ = n₁/M_k, π₂ = n₂/M_k (cluster sizes as mixture weights)

### 6.5.2 P(win) Under Bimodal

```
P(win) = π₁ × [Φ((b-μ₁)/σ₁) - Φ((a-μ₁)/σ₁)] + π₂ × [Φ((b-μ₂)/σ₂) - Φ((a-μ₂)/σ₂)]
```

Where Φ is the standard normal CDF.

**Skewness is ignored** in bimodal mode — the mixture already captures the asymmetry through separate centroids.

## 6.6 Shadow Book Output

For each (station, target_date, target_type, bin), the pricing layer writes one row to `SHADOW_BOOK`:

| Column | Value |
|--------|-------|
| `STATION_ID` | e.g., "KNYC" |
| `TARGET_DATE` | e.g., 2026-03-23 |
| `TARGET_TYPE` | "high" or "low" |
| `BIN_LOWER` | e.g., 80.5 |
| `BIN_UPPER` | e.g., 82.5 |
| `TICKER` | Kalshi ticker string |
| `MU` | μ (Kalman-corrected) |
| `SIGMA_EFF` | σ_eff_final |
| `G1` | G1_s |
| `XI` | ξ_s |
| `OMEGA` | ω_s |
| `ALPHA` | α_s |
| `P_YES` | P(win) for this bin |
| `IS_TRUNCATED` | 1 if METAR truncation was applied |
| `IS_BIMODAL` | 1 if bimodal mixture was used |
| `LEAD_HOURS` | Continuous float lead time |
| `LEAD_BRACKET` | Classified bracket (h1–h5) |
| `PRICED_AT` | SYSTIMESTAMP |

**SHADOW_BOOK_HISTORY:** Every Shadow Book update is also appended to the history table (append-only). This powers the MPDS signal in the IBE (Section 7) which needs to track how P(win) changes over time.

---

# Section 7: Execution Layer (L4)

**Owner:** `kalshicast/execution/`
**Writes to:** BEST_BETS, POSITIONS, ORDER_LOG, MARKET_ORDERBOOK_SNAPSHOTS, IBE_SIGNAL_LOG
**Reads from:** SHADOW_BOOK, BSS_MATRIX, KALMAN_STATES, ENSEMBLE_STATE, SHADOW_BOOK_HISTORY, METAR_DAILY_MAX, FINANCIAL_METRICS, PARAMS

The execution layer decides **whether** to bet (Conviction Gates), **how much** to bet (Kelly sizing), **at what price** (VWAP), and **submits the order** to Kalshi. It also computes the five IBE signals that modulate bet sizing based on real-time system confidence.

## 7.1 Conviction Gates

**File:** `kalshicast/execution/gates.py`

Every candidate bet must pass all five gates. A gate failure is an immediate rejection — no bet is placed. Gates are evaluated in order; evaluation short-circuits on first failure.

### Gate 1: Minimum EV (Edge Existence)

For a YES buy at market price c (in dollars, e.g., 0.40) with model probability p:

```
fee_per_contract_cents = ⌈fee_rate × c × (1 - c) × 100⌉
EV_net_per_contract_cents = (p - c) × 100 - fee_per_contract_cents
```

**Gate condition (two parts, both must pass):**

```
Part A: EV_net_per_contract_cents > 0
         (the bet is profitable after fees)

Part B: EV_net_per_contract_cents × contracts / 100 ≥ params.gate.ev_min_fraction × b
         (the total dollar EV exceeds the minimum threshold)
```

Where `b` is the current bankroll. Default `ev_min_fraction` = 0.025, so at $10 bankroll, the minimum total EV per bet is $0.25.

**On failure:** Reject. Reason: "EV_net below minimum threshold."

### Gate 2: Edge Buffer (Statistical Significance)

```
ε_edge = max(params.gate.epsilon_edge_base, 1.96 × √(0.25 / N_bets))
```

Where N_bets is the total number of bets placed to date (across all stations and types).

**Adaptive behavior:**
- At N_bets = 0: ε_edge = ∞ (no bets until at least one Shadow Book cycle completes — use epsilon_edge_base)
- At N_bets = 10: ε_edge = max(0.03, 1.96 × √(0.025)) = max(0.03, 0.310) = 0.310
- At N_bets = 100: ε_edge = max(0.03, 1.96 × √(0.0025)) = max(0.03, 0.098) = 0.098
- At N_bets = 1000: ε_edge = max(0.03, 1.96 × √(0.00025)) = max(0.03, 0.031) = 0.031
- At N_bets ≥ 1068: ε_edge = 0.03 (base dominates)

**Gate condition:**
```
PASS if (p - c) ≥ ε_edge
```

**On failure:** Reject. Reason: "Edge {p-c:.4f} below buffer {ε_edge:.4f}."

### Gate 3: Spread Gate (Model Agreement)

```
PASS if S_{t,k} ≤ params.gate.spread_max
```

Where S_{t,k} is the **unweighted** ensemble spread (°F). Default `spread_max` = 4.0°F.

**Rationale:** When models disagree by more than 4°F, the system's probability estimate is unreliable regardless of what the math produces.

**On failure:** Reject. Reason: "Spread {S:.1f}°F exceeds limit {spread_max}°F."

### Gate 4: Skill Gate (BSS Threshold with Hysteresis)

The BSS for the relevant (station, lead_bracket, target_type) cell in the skill matrix:

```
bss = BSS_MATRIX[station_id, lead_bracket, target_type]
```

**Hysteresis logic:**
- If the cell was **not** previously qualified: PASS if `bss ≥ params.gate.bss_enter` (default 0.07)
- If the cell **was** previously qualified: PASS if `bss ≥ params.gate.bss_exit` (default 0.03)

The qualification state is stored in `BSS_MATRIX.IS_QUALIFIED` (1 or 0).

**On failure:** Reject. Reason: "BSS {bss:.3f} below skill threshold."

### Gate 5: Lead Time Ceiling

```
PASS if lead_hours ≤ params.gate.lead_ceiling_hours
```

Default `lead_ceiling_hours` = 72.0 (3 days).

**On failure:** Reject. Reason: "Lead time {lead_hours:.1f}h exceeds ceiling {ceiling}h."

## 7.2 IBE Signals

**File:** `kalshicast/execution/ibe.py`

The Intelligent Betting Engine computes five signals that assess the system's real-time confidence. Each signal produces a raw value and a modifier ∈ (0, 2). Signals operate in two tiers: **veto** (can kill the bet entirely) and **scaling** (modulates bet size).

### 7.2.1 KCV — Kalman Convergence Velocity

**Purpose:** Detect when the Kalman filter's bias estimate is changing rapidly (unstable).

**Computation:**
```
KCV_k = |B_k - B_{k-lookback}| / lookback
```

Where `lookback` = `params.ibe.kcv_lookback_days` (default 7).

**Normalization:**
```
KCV_normalized = KCV_k / mean(KCV, trailing params.ibe.kcv_norm_window runs)
```

Default `kcv_norm_window` = 90.

**Modifier:**
```
KCV_mod = max(0.5, 1 - 0.25 × (KCV_normalized - 1))
```

Range: [0.5, 1.25]. Values > 1 when KCV_normalized < 1 (filter is more stable than average).

**Veto condition:** `KCV_normalized > params.ibe.kcv_veto_threshold` (default 4.0). If triggered, the bet is killed entirely.

### 7.2.2 MPDS — Model-Price Divergence Signal

**Purpose:** Detect when the Shadow Book probability is moving in the opposite direction from the market price.

**Computation:**
```
delta_p = p_current - p_previous   (Shadow Book P(win) change since last pricing)
delta_c = c_current - c_previous   (Market price change since last snapshot)
MPDS_k = delta_c - delta_p
```

A positive MPDS means the market is moving toward us faster than our model — potential adverse selection. A negative MPDS means the market is moving away from us.

**Modifier (positive MPDS — market converging):**
```
MPDS_mod = max(0.5, 1 - |MPDS_k| × params.ibe.mpds_positive_scale)
```

Default `mpds_positive_scale` = 5.0.

**Modifier (negative MPDS — market diverging):**
```
MPDS_mod = max(0.3, 1 - |MPDS_k| × params.ibe.mpds_negative_scale)
```

Default `mpds_negative_scale` = 8.0 (more aggressive penalty for divergence).

**Veto condition:** `|MPDS_k| > params.ibe.mpds_veto_threshold` (default 0.12). Large divergence in either direction triggers veto.

### 7.2.3 HMAS — Historical Model Agreement Score

**Purpose:** Measure consensus among the best-performing models.

**Computation:**
```
top_models = models where BSS_{m,s,h,k} > params.gate.bss_exit
in_consensus = [m for m in top_models if |F_{m,t,k} - F̄_top| ≤ params.ibe.hmas_consensus_f]
HMAS = len(in_consensus) / len(top_models)
```

Where F̄_top = mean of top_models' forecasts. Default `hmas_consensus_f` = 1.0°F.

If `len(top_models)` = 0, HMAS = 0 and the bet should have already been rejected by the Skill Gate.

**Modifier:**
```
HMAS_mod = 0.7 + 0.6 × HMAS
```

Range: [0.7, 1.3]. Maximum when all top models agree within 1°F.

**No veto condition.** HMAS is scaling-only.

### 7.2.4 FCT — Forecast Convergence Tracker

**Purpose:** Detect whether models are converging (gaining agreement over time) or diverging (losing agreement).

**Computation:**
```
S_delta = S_{t,k} - S_{t-3,k}    (spread change over 3 runs)
FCT = S_delta / σ_{h,s,k}         (normalized by historical RMSE)
```

**Modifier (FCT < 0 — converging):**
```
FCT_mod = min(1.4, 1 - FCT × 0.4)
```

**Modifier (FCT > 0 — diverging):**
```
FCT_mod = max(0.5, 1 - FCT × 0.6)
```

**Veto condition:** `FCT > params.ibe.fct_veto_threshold` (default 1.5). Rapid divergence triggers veto.

### 7.2.5 SCAS — Seasonal Climatological Anomaly Score

**Purpose:** Detect when the current Kalman bias departs significantly from the historical seasonal bias.

**Computation:**
```
B_seasonal_{s,doy,k} = rolling 15-day mean of historical Kalman biases for station s,
                        day-of-year doy, target type k (from KALMAN_HISTORY)

SCAS = |B_seasonal_{s,doy,k} - B_k| / σ_{h,s,k}
```

**Modifier:**
```
SCAS_mod = max(0.6, 1 - params.ibe.scas_scale × SCAS)
```

Default `scas_scale` = 0.15.

**No veto condition.** SCAS is scaling-only.

### 7.2.6 IBE Combination

**Veto tier (evaluated first):**
Any single veto kills the bet:
```
IF KCV_normalized > kcv_veto_threshold: VETO. Reason: "KCV veto"
IF |MPDS_k| > mpds_veto_threshold:      VETO. Reason: "MPDS veto"
IF FCT > fct_veto_threshold:            VETO. Reason: "FCT veto"
```

**Scaling tier:**
If no veto, compute the composite modifier:

```
COMPOSITE = KCV_mod^w₁ × MPDS_mod^w₂ × HMAS_mod^w₃ × FCT_mod^w₄ × SCAS_mod^w₅
```

Where [w₁, w₂, w₃, w₄, w₅] = `params.ibe.composite_weights` (default [0.25, 0.35, 0.15, 0.15, 0.10]).

**Clipping:**
```
COMPOSITE_clipped = clip(COMPOSITE, params.ibe.composite_clip_low, params.ibe.composite_clip_high)
```

Default clip range: [0.25, 1.50].

**Database write:** Every IBE evaluation is logged to `IBE_SIGNAL_LOG` with all raw values, modifiers, composite, and veto status.

## 7.3 Kelly Sizing

**File:** `kalshicast/execution/kelly.py`

### 7.3.1 Background: Mutually Exclusive Outcomes

Temperature bins within a single market (e.g., "KNYC high temperature on March 23") are mutually exclusive — exactly one bin will settle YES. The standard binary Kelly criterion (`f* = (p-c)/(1-c)`) does not apply because betting on multiple bins simultaneously creates correlated exposures.

The correct framework is the Smirnov (1973) procedure for optimal Kelly sizing across mutually exclusive outcomes.

### 7.3.2 Smirnov Three-Step Procedure

**Input:** For a market with B bins, each bin i has:
- p_i = Shadow Book P(win) (our probability estimate)
- c_i = market price (YES price in [0,1])

**Step 1 — Sort and filter:**
```
Compute e_i = p_i / c_i for each bin (edge ratio).
Sort bins by e_i in descending order.
Filter to bins where e_i > 1.0 (positive expected value only).
```

**Step 2 — Determine the optimal bet set S:**
```
Initialize S = empty set
For each bin i in sorted order:
    reserve_rate = (1 - Σ_{j∈S} p_j) / (1 - Σ_{j∈S} c_j)
    IF e_i > reserve_rate THEN:
        Add i to S
    ELSE:
        STOP (no more bins are profitable to add)
```

The reserve rate represents the implied odds of "holding cash" (not betting on any bin). As more bins are added to S, the reserve rate changes because the mutually exclusive constraint means capital allocated to one bin cannot be allocated to another.

**Step 3 — Compute optimal fractions:**
```
For each bin i in S:
    f_i* = p_i - c_i × (1 - Σ_{j∈S} p_j) / (1 - Σ_{j∈S} c_j)
```

The sum `Σ f_i*` ≤ 1.0 is guaranteed by construction.

### 7.3.3 Worked Example: 3-Bin Market

Consider a simplified market for KNYC high temperature with 3 bins:

| Bin | Label | p_i (model) | c_i (market) | e_i = p_i/c_i |
|-----|-------|-------------|-------------|----------------|
| A | ≤78°F | 0.25 | 0.30 | 0.833 |
| B | 79-80°F | 0.50 | 0.40 | 1.250 |
| C | ≥81°F | 0.25 | 0.30 | 0.833 |

**Step 1:** Sort by e_i descending: B (1.250), A (0.833), C (0.833).
Filter to e_i > 1.0: Only bin B qualifies initially.

**Step 2:** Determine S.
- Start: S = {}
- Consider B: reserve_rate = (1 - 0) / (1 - 0) = 1.0. Is e_B = 1.250 > 1.0? YES. Add B to S.
- S = {B}. Σp_S = 0.50, Σc_S = 0.40.
- Consider A: reserve_rate = (1 - 0.50) / (1 - 0.40) = 0.50/0.60 = 0.833. Is e_A = 0.833 > 0.833? NO (equal, not strictly greater). STOP.
- Final S = {B}.

**Step 3:** Compute f_B*.
```
f_B* = p_B - c_B × (1 - Σ_S p) / (1 - Σ_S c)
     = 0.50 - 0.40 × (1 - 0.50) / (1 - 0.40)
     = 0.50 - 0.40 × 0.833
     = 0.50 - 0.333
     = 0.167
```

**Result:** Bet 16.7% of bankroll on bin B (YES at 40¢). Do not bet on A or C.

**Verification:** At $10 bankroll, bet $1.67 on B.
- If B wins (prob 0.50): gain $1.67 × (1/0.40 - 1) = $1.67 × 1.50 = $2.505
- If B loses (prob 0.50): lose $1.67
- E[gain] = 0.50 × $2.505 - 0.50 × $1.67 = $1.253 - $0.835 = $0.418 (before fees)

### 7.3.4 Worked Example Extension: Two Bins in S

Modify the market so bin C is also attractive:

| Bin | p_i | c_i | e_i |
|-----|-----|-----|-----|
| A | 0.20 | 0.30 | 0.667 |
| B | 0.50 | 0.35 | 1.429 |
| C | 0.30 | 0.20 | 1.500 |

**Step 1:** Sort: C (1.500), B (1.429), A (0.667).

**Step 2:**
- S = {}
- Consider C: reserve = 1.0/1.0 = 1.0. e_C = 1.500 > 1.0. Add C. S = {C}. Σp = 0.30, Σc = 0.20.
- Consider B: reserve = (1-0.30)/(1-0.20) = 0.70/0.80 = 0.875. e_B = 1.429 > 0.875. Add B. S = {B,C}. Σp = 0.80, Σc = 0.55.
- Consider A: reserve = (1-0.80)/(1-0.55) = 0.20/0.45 = 0.444. e_A = 0.667 > 0.444. Add A. S = {A,B,C}. Σp = 1.00, Σc = 0.85.
- No more bins. Done.

**Step 3:**
```
reserve_final = (1 - 1.00) / (1 - 0.85) = 0.0 / 0.15 = 0.0

f_A* = 0.20 - 0.30 × 0.0 = 0.200
f_B* = 0.50 - 0.35 × 0.0 = 0.500
f_C* = 0.30 - 0.20 × 0.0 = 0.300
```

Σf* = 1.00. The system would bet the entire bankroll (split across three bins). In practice this is aggressive; the Kelly fraction cap and IBE scaling will reduce these.

**Note:** When Σp = 1.0 exactly, the reserve rate goes to 0 and all edge is captured. In real markets, Σp ≈ 1.0 and Σc > 1.0 (market vig), so the reserve rate is typically > 0.

### 7.3.5 Full Sizing Chain

After the raw Kelly fractions f_i* are computed:

```
1. Kelly cap:        f_capped = min(f_i*, params.kelly.fraction_cap)         # default 0.10
2. Φ scaling:        f_phi = f_capped × Φ(BSS_{s,h,k})
3. IBE scaling:      f_ibe = f_phi × COMPOSITE_clipped
4. Market filter:    IF Γ < gamma_threshold: f_market = f_ibe × (Γ / gamma_threshold)
                     ELSE: f_market = f_ibe
5. Position cap:     f_pos = min(f_market, remaining_capacity per position limits)
6. Drawdown scale:   f_dd = f_pos × D_scale
7. Jitter:           f_final = f_dd × (1 + Uniform(-jitter_pct, +jitter_pct))
8. Minimum check:    IF f_final × b < params.kelly.min_bet_fraction × b: SKIP
9. Round to cents:   contracts = floor(f_final × b / c_i)  # whole contracts only
                     IF contracts < 1: SKIP
```

### 7.3.6 Φ(BSS) — Continuous Skill Scaling

Replaces the step-function tiers from v9:

```
Φ(BSS) = min(1.0, BSS / params.kelly.phi_bss_cap)
Φ(BSS) = max(params.kelly.phi_min, Φ(BSS))   # floor at 0.10
```

Default `phi_bss_cap` = 0.25, so:
- BSS = 0.07 (just above Skill Gate): Φ = max(0.10, 0.07/0.25) = max(0.10, 0.28) = 0.28
- BSS = 0.125: Φ = 0.50
- BSS = 0.25: Φ = 1.00
- BSS = 0.40: Φ = 1.00 (capped)

### 7.3.7 Market Convergence Filter

```
Γ_{t,k} = P_market(top_bin) / max_{j≠top} P_market(bin_j)
```

Where P_market is derived from market prices: P_market(bin) = c_i (the YES price).

If `Γ < params.market.gamma_threshold` (default 0.75):
```
f_market = f_ibe × (Γ / gamma_threshold)
```

This scales down bets when the market strongly favors one bin (concentrated liquidity).

### 7.3.8 Drawdown Scale (D_scale)

```
D_scale = max(0, 1 - max(0, (MDD_current - MDD_safe) / (MDD_halt - MDD_safe)))
```

Where:
- `MDD_current` = current maximum drawdown (from FINANCIAL_METRICS)
- `MDD_safe` = `params.drawdown.mdd_safe` (default 0.10)
- `MDD_halt` = `params.drawdown.mdd_halt` (default 0.20)

**Behavior:**
- MDD < 10%: D_scale = 1.0 (full sizing)
- MDD = 15%: D_scale = 0.50 (half sizing)
- MDD ≥ 20%: D_scale = 0.0 (halt all betting)

### 7.3.9 Position Limits

All expressed as fractions of current bankroll `b`:

| Limit | Parameter | Default | Scope |
|-------|-----------|---------|-------|
| Single position | `position.max_single_fraction` | 0.10 | One bet on one bin |
| Per-station per-day | `position.max_station_day_fraction` | 0.10 | New exposure per station per calendar day |
| Per-station total | `position.max_station_fraction` | 0.25 | All open positions at one station |
| Correlated stations | `position.max_correlated_fraction` | 0.40 | All open positions across stations sharing a WFO |
| Portfolio total | `position.max_total_fraction` | 0.80 | All open positions across all stations |

`remaining_capacity` in the sizing chain is the minimum of all applicable limits minus current exposure.

## 7.4 VWAP and Order Execution

**File:** `kalshicast/execution/order.py`

### 7.4.1 Order Book Scan

Before submitting an order, fetch the current order book:

```
GET /markets/{ticker}/orderbook?depth={params.vwap.depth_levels}
```

The response contains `yes` bids at various price levels.

### 7.4.2 VWAP Computation

For a YES buy order of `contracts` contracts:

```
c_VWAP = Σ(price_level × quantity_at_level) / Σ(quantity_at_level)
```

Summing from the best ask (lowest YES price) upward until `contracts` are filled.

If the order book has insufficient depth to fill `contracts`, the VWAP is undefined — use the best ask and flag a liquidity warning.

### 7.4.3 Staleness Test

Compare VWAP to the best bid/ask:

```
c_best = best YES ask price
delta = |c_VWAP - c_best|

IF delta > params.vwap.staleness_delta THEN:
    Log ALERT: "Order book stale — VWAP delta {delta:.3f} > threshold"
    IF delta > 2 × staleness_delta:
        ABORT order submission. Write to SYSTEM_ALERTS.
    ELSE:
        Proceed with caution. Use c_VWAP (not c_best) for execution.
```

Default `staleness_delta` = 0.05.

### 7.4.4 Tranche Execution

For orders exceeding `params.vwap.tranche_threshold` contracts (default 50):

```
n_tranches = ceil(contracts / params.vwap.tranche_size)  # default 25 per tranche

For each tranche i = 1..n_tranches:
    1. Re-fetch order book
    2. Recompute c_VWAP for remaining contracts
    3. Submit tranche order (tranche_size contracts, or remainder)
    4. Wait params.vwap.tranche_delay_sec seconds (default 5)
    5. Check fill status of previous tranche
    6. If previous tranche not filled after maker_timeout_sec, cancel and abort remaining tranches
```

**Rationale:** Large orders move the market. Tranche execution reduces market impact and detects adverse conditions between tranches.

### 7.4.5 Maker vs. Taker Decision

```
lead_hours = current lead time to settlement
P_fill = 1 - exp(-lead_hours / params.fee.maker_fill_prob_h_half)

IF P_fill > 0.80 AND EV_net_maker > EV_net_taker THEN:
    Place as MAKER (limit order at our price)
ELSE:
    Place as TAKER (limit order at best ask)
```

**EV comparison:**
```
EV_net_taker = (p - c_ask) × 100 - ⌈0.07 × c_ask × (1-c_ask) × 100⌉
EV_net_maker = P_fill × [(p - c_bid) × 100 - ⌈0.0175 × c_bid × (1-c_bid) × 100⌉]
```

Where c_bid is our posted price (between best bid and best ask).

## 7.5 Kalshi API Integration

**File:** `kalshicast/execution/kalshi_api.py`

### 7.5.1 Authentication

All requests require three headers:
```
KALSHI-ACCESS-KEY: {api_key_id}
KALSHI-ACCESS-SIGNATURE: {rsa_pss_signature}
KALSHI-ACCESS-TIMESTAMP: {unix_ms}
```

The signature is computed as RSA-PSS over the message: `{timestamp}{method}{path}{body}`.

**Env vars:** `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`.

### 7.5.2 Base URL

```
Production: https://trading-api.kalshi.com/trade-api/v2
```

### 7.5.3 Order Submission

**Endpoint:** `POST /portfolio/orders`

**Request body:**
```json
{
    "ticker": "KXHIGHNYC-26MAR23-B82",
    "side": "yes",
    "action": "buy",
    "count": 10,
    "yes_price": 40,
    "time_in_force": "good_till_canceled",
    "client_order_id": "kc-{run_id}-{station}-{bin}",
    "buy_max_cost": 450
}
```

**Key fields:**
- `yes_price`: Integer cents (1–99)
- `count`: Whole contracts
- `buy_max_cost`: Safety cap in cents = `count × yes_price × 1.10` (10% slippage buffer)
- `time_in_force`: "good_till_canceled" for maker, "fill_or_kill" for taker
- `client_order_id`: Idempotency key preventing duplicate submissions on retry

**Response 201:**
```json
{
    "order": {
        "order_id": "uuid",
        "status": "resting" | "executed" | "canceled",
        "yes_price": 40,
        "no_price": 60,
        "created_time": "2026-03-22T14:30:00Z",
        "action": "buy",
        "side": "yes",
        "count": 10,
        "remaining_count": 10
    }
}
```

### 7.5.4 Position Query

**Endpoint:** `GET /portfolio/positions?ticker={ticker}`

Returns current position (contract count, average entry price).

### 7.5.5 Balance Query

**Endpoint:** `GET /portfolio/balance`

Returns `balance` (available cash in cents) and `portfolio_value` (total including positions).

The system reads balance at the start of every market_open run to set the current bankroll `b`:
```
b = balance_cents / 100.0
```

### 7.5.6 Order Book Fetch

**Endpoint:** `GET /markets/{ticker}/orderbook?depth={depth}`

Returns `orderbook_fp` with `yes` bids and `no` bids at each price level.

### 7.5.7 Retry Logic

```
for attempt in range(params.order.retry_max):
    try:
        response = submit_order(payload)
        if response.status == 201:
            return response.order
        if response.status == 409:  # duplicate client_order_id
            return get_order(client_order_id)  # idempotent
        if response.status in (429, 500, 502, 503):
            sleep(params.order.retry_backoff_sec × 2^attempt)
            continue
        raise APIError(response)
    except ConnectionError:
        sleep(params.order.retry_backoff_sec × 2^attempt)
        continue
raise MaxRetriesExceeded()
```

### 7.5.8 Database Logging

Every API interaction is logged to `ORDER_LOG`:
- `ORDER_ID`, `CLIENT_ORDER_ID`, `RUN_ID`, `TICKER`, `SIDE`, `ACTION`
- `PRICE_CENTS`, `CONTRACTS`, `STATUS`, `FILL_COUNT`, `FILL_PRICE_AVG`
- `REQUEST_PAYLOAD` (JSON), `RESPONSE_PAYLOAD` (JSON)
- `SUBMITTED_AT`, `FILLED_AT`, `CANCELED_AT`

Pre-execution order book snapshots are logged to `MARKET_ORDERBOOK_SNAPSHOTS`:
- `TICKER`, `SNAPSHOT_AT`, `DEPTH`, `BOOK_JSON` (full order book as JSON)

## 7.6 Fee Formula

### 7.6.1 Taker Fee

```
fee_cents = ⌈F_taker × C × P × (1 - P) × 100⌉
```

Where:
- F_taker = `params.fee.taker_rate` (default 0.07)
- C = number of contracts
- P = contract price in dollars (e.g., 0.40)
- ⌈...⌉ = ceiling to next cent

**Example:** 10 contracts at $0.50:
```
fee_cents = ⌈0.07 × 10 × 0.50 × 0.50 × 100⌉ = ⌈17.5⌉ = 18 cents = $0.18
```

### 7.6.2 Maker Fee

```
fee_cents = ⌈F_maker × C × P × (1 - P) × 100⌉
```

Where F_maker = `params.fee.maker_rate` (default 0.0175).

### 7.6.3 Conservative Partial-Fill Assumption

When an order is partially filled, fees are computed **per fill**, not on the aggregate order. This means rounding applies to each partial fill independently.

**Example:** An order for 100 contracts partially fills as 3 fills of 33, 33, and 34 contracts:
```
Fee per fill (33 contracts at $0.50): ⌈0.07 × 33 × 0.25 × 100⌉ = ⌈57.75⌉ = 58 cents
Fee per fill (34 contracts at $0.50): ⌈0.07 × 34 × 0.25 × 100⌉ = ⌈59.50⌉ = 60 cents
Total: 58 + 58 + 60 = 176 cents

vs. aggregate: ⌈0.07 × 100 × 0.25 × 100⌉ = 175 cents
```

The system always models the worst case (per-fill rounding) to avoid underestimating fee drag.

### 7.6.4 Net EV After Fees

```
EV_net = (p - c) - ⌈F_rate × c × (1-c) × 100⌉ / 100
```

A bet is only placed if EV_net > 0 **and** the total dollar EV exceeds the minimum bet floor.

## 7.7 Best Bets Output

After all gates, IBE signals, and Kelly sizing, the surviving bets are written to `BEST_BETS`:

| Column | Description |
|--------|-------------|
| `RUN_ID` | Pipeline run identifier |
| `STATION_ID` | Station |
| `TARGET_DATE` | Settlement date |
| `TARGET_TYPE` | "high" or "low" |
| `TICKER` | Kalshi ticker |
| `BIN_LOWER` | Lower bound (°F) |
| `BIN_UPPER` | Upper bound (°F) |
| `P_MODEL` | Shadow Book P(win) |
| `P_MARKET` | Market price (c) |
| `EDGE` | p - c |
| `EV_NET` | After fees |
| `F_KELLY_RAW` | Raw Smirnov f_i* |
| `F_FINAL` | After full sizing chain |
| `CONTRACTS` | Whole contracts to buy |
| `COST_CENTS` | contracts × price × 100 |
| `FEE_CENTS` | Estimated fee |
| `IBE_COMPOSITE` | IBE composite modifier |
| `PHI_BSS` | Φ(BSS) value |
| `D_SCALE` | Drawdown scaling factor |
| `MAKER_OR_TAKER` | "maker" or "taker" |
| `GATE_PASS` | 1 (always — only passing bets are written) |
| `CREATED_AT` | SYSTIMESTAMP |

The `BEST_BETS` table is the materialized output of the decision engine. It is both the instruction set for order submission and the audit trail for post-hoc evaluation.

---

# Section 8: Evaluation Layer (L5)

**Owner:** `kalshicast/evaluation/`
**Writes to:** BRIER_SCORES, BSS_MATRIX, CALIBRATION_HISTORY, FINANCIAL_METRICS, SYSTEM_ALERTS
**Reads from:** SHADOW_BOOK, OBSERVATIONS, BEST_BETS, POSITIONS, ORDER_LOG, PARAMS

The evaluation layer measures system performance across two dimensions: **probabilistic accuracy** (are the Shadow Book probabilities well-calibrated?) and **financial performance** (is the system making money?). It also maintains the BSS skill matrix that drives ensemble weights, skill gates, and Kelly scaling.

## 8.1 Brier Score and BSS

### 8.1.1 Brier Score (BS)

For a single bin prediction:

```
BS = (P_yes - A)²
```

Where:
- P_yes = Shadow Book P(win) at the time of pricing
- A = actual outcome: 1 if the observed temperature fell in this bin, 0 otherwise

For a set of N predictions:

```
BS_mean = (1/N) × Σᵢ (P_yes_i - Aᵢ)²
```

Range: [0, 1]. Lower is better. A perfect forecaster has BS = 0.

### 8.1.2 Baseline 1: Climatological

The climatological baseline assigns equal probability to all bins:

```
BS_clim = (1/N) × Σᵢ (1/B - Aᵢ)²
```

Where B is the number of bins in the market (typically ~15–20 for a temperature market).

### 8.1.3 Baseline 2: Market Price

The market baseline uses the Kalshi market price as the probability:

```
BS_market = (1/N) × Σᵢ (c_i - Aᵢ)²
```

This measures whether our model outperforms the market's implied probabilities.

### 8.1.4 Brier Skill Score

Against climatology:
```
BSS_clim = 1 - BS_model / BS_clim
```

Against market:
```
BSS_market = 1 - BS_model / BS_market
```

Range: (-∞, 1]. Positive means the model outperforms the baseline. BSS = 0 means equal performance. BSS < 0 means the model is worse than the baseline.

### 8.1.5 Per-Model BSS

For each model m, compute BS using that model's raw (non-ensemble) forecast probability:

```
BS_m = (1/N) × Σᵢ (P_yes_m_i - Aᵢ)²
BSS_m = 1 - BS_m / BS_clim
```

These per-model BSS values feed into:
- Ensemble weight computation (Section 5.5.4)
- Top-model selection (Section 5.5.2)
- The BSS skill matrix (Section 8.1.6)

### 8.1.6 BSS Skill Matrix

The 100-cell matrix: 20 stations × 5 lead brackets, computed separately for high and low (total 200 cells, stored as 200 rows in `BSS_MATRIX`).

Each cell contains:
```
BSS_{s,h,k} = ensemble BSS for station s, lead bracket h, target type k
              over the trailing params.eval.bss_window_days (default 90) days
```

**Database table `BSS_MATRIX`:**
- `STATION_ID`, `LEAD_BRACKET`, `TARGET_TYPE`
- `BSS_CLIM` (BSS vs. climatology)
- `BSS_MARKET` (BSS vs. market — NULL if no market prices available)
- `N_SAMPLES` (count of graded predictions)
- `IS_QUALIFIED` (1 if above skill gate, 0 if below)
- `ENTERED_AT` (timestamp when qualification was gained)
- `EXITED_AT` (timestamp when qualification was lost, NULL if qualified)
- `UPDATED_AT`

**Refresh frequency:** Updated nightly after observations are ingested and Brier scores are computed.

### 8.1.7 Lead-Time Ceiling

The maximum profitable lead time for a station:

```
h*_s = max h where BSS_{s,h,k} ≥ params.gate.bss_exit
```

Bets beyond h*_s are rejected by the Lead Time Ceiling gate (Section 7.1, Gate 5).

## 8.2 Calibration Metrics

### 8.2.1 Probability Calibration (CAL)

Divide all predictions into B buckets by predicted probability (B = `params.eval.calibration_buckets`, default 10):

```
Bucket b: predictions where P_yes ∈ [(b-1)/B, b/B)
f̄_b = mean predicted probability in bucket b
ō_b = actual frequency of outcome=1 in bucket b

CAL = (1/B) × Σ_b |f̄_b - ō_b|
```

Range: [0, 0.5]. Lower is better. A perfectly calibrated system has CAL = 0.

**Interpretation:**
- CAL < 0.03: Excellent calibration
- CAL 0.03–0.06: Good
- CAL 0.06–0.10: Acceptable
- CAL > 0.10: Recalibration needed

### 8.2.2 Market Calibration (Market CAL)

Same computation but comparing market prices to outcomes instead of model predictions:

```
Market CAL = (1/B) × Σ_b |c̄_b - ō_b|
```

If Market CAL < CAL, the market is better calibrated than our model — a warning signal.

## 8.3 Financial Metrics

**File:** `kalshicast/evaluation/financial.py`

All financial metrics are computed from settled positions in `POSITIONS` and `ORDER_LOG`.

### 8.3.1 Profit/Loss Per Bet

```
π_i = (settlement_value - cost - fee) per contract × contracts
```

Where settlement_value = 100¢ if the bet won, 0¢ if it lost.

### 8.3.2 Hit Rate (SR — Success Ratio)

```
SR = (count of bets where π_i > 0) / (total settled bets)
```

### 8.3.3 Dollar Sharpe Ratio (SR_$)

```
SR_$ = π̄ / σ_π
```

Where π̄ = mean profit per bet and σ_π = standard deviation of profits. Computed over betting days only (days with no bets are excluded). Risk-free rate is excluded (at $10 bankroll scale, it is negligible).

**Interpretive benchmarks:**
- SR_$ > 1.0: Functional edge — system is profitable
- SR_$ > 2.0: Real edge — consistent profitability
- SR_$ > 3.0: Exceptional — rare at any scale

### 8.3.4 Maximum Drawdown (MDD)

```
MDD = max over all t [(Peak_t - Trough_t) / Peak_t]
```

Where Peak_t is the running maximum portfolio value up to time t, and Trough_t is the minimum portfolio value after Peak_t.

MDD feeds the D_scale computation in Section 7.3.8.

### 8.3.5 Fee Drag Ratio (FDR)

```
FDR = Total Fees Paid / Gross Profit
```

Where Gross Profit = Σ max(0, π_i). FDR > 0.50 means fees consume more than half of gross winnings — a critical problem at small bankroll scale.

### 8.3.6 Edge Utilization Rate (EUR)

```
EUR = Contracts Actually Bet / Contracts Passing EV Gate
```

Measures what fraction of identified opportunities were actually traded. Low EUR (< 0.30) suggests gates or IBE are too conservative. High EUR (> 0.90) suggests insufficient filtering.

### 8.3.7 Database Persistence

`FINANCIAL_METRICS` (append-only, one row per day):
- `METRIC_DATE`, `BANKROLL`, `PORTFOLIO_VALUE`, `DAILY_PNL`
- `CUMULATIVE_PNL`, `MDD`, `SR_DOLLAR`, `SR_HIT`, `FDR`, `EUR`
- `N_BETS_TOTAL`, `N_BETS_WON`, `N_BETS_LOST`
- `TOTAL_FEES`, `GROSS_PROFIT`, `NET_PROFIT`
- `SHARPE_ROLLING_30`, `CAL`, `MARKET_CAL`

## 8.4 Adaptive ε_edge

The edge buffer (Section 7.1, Gate 2) tightens automatically as the bet record grows:

```
ε_edge = max(params.gate.epsilon_edge_base, 1.96 × √(0.25 / N_bets))
```

**Update mechanism:** N_bets is read from `FINANCIAL_METRICS.N_BETS_TOTAL` at the start of each market_open run. The computed ε_edge is used for all gate evaluations in that run.

**Rationale:** At small sample sizes, the uncertainty around our probability estimates is wide. Requiring a large edge buffer prevents premature betting. As the sample grows, the buffer narrows to the base level (0.03), allowing thinner edges to be traded.

## 8.5 Maker Adverse Selection Test

**File:** `kalshicast/evaluation/adverse_selection.py`

Over a trailing window of `params.eval.adverse_selection_window` days (default 90):

```
fill_quality_maker = mean(π_i for maker fills)
fill_quality_taker = mean(π_i for taker fills)

delta_fill = fill_quality_maker - fill_quality_taker
```

**Interpretation:**
- `delta_fill > 0`: Maker fills are more profitable — good, continue using maker orders.
- `delta_fill < 0`: Maker fills are less profitable — adverse selection detected. The market is filling our maker orders only when they are mispriced.
- `delta_fill < -0.02`: ALERT. System should reduce maker order frequency. Write to `SYSTEM_ALERTS` with severity `WARNING`.
- `delta_fill < -0.05`: CRITICAL ALERT. Disable maker orders entirely until manual review. Write to `SYSTEM_ALERTS` with severity `CRITICAL`.

## 8.6 BSS Pattern Classifier

**File:** `kalshicast/evaluation/pattern_classifier.py`

**Replaces weekly manual BSS matrix review.** Runs every `params.eval.pattern_check_interval_days` (default 7).

Scans the BSS matrix for four degradation patterns:

### 8.6.1 Row Degradation

A single station performs poorly across all lead brackets:

```
FOR each station s:
    IF mean(BSS_{s,h,k} for all h) < params.gate.bss_exit:
        ALERT: "Station {s} degraded across all brackets. Mean BSS = {value}."
        Severity: WARNING
        Suggested action: Check station reliability, consider IS_RELIABLE flag.
```

### 8.6.2 Column Degradation

A single lead bracket performs poorly across all stations:

```
FOR each lead bracket h:
    IF mean(BSS_{s,h,k} for all s) < params.gate.bss_exit:
        ALERT: "Bracket {h} degraded across all stations. Mean BSS = {value}."
        Severity: WARNING
        Suggested action: Lead time ceiling may need tightening.
```

### 8.6.3 Diagonal Degradation

Performance degrades monotonically with lead time (expected, but rate matters):

```
FOR each station s:
    bss_by_bracket = [BSS_{s,h1,k}, BSS_{s,h2,k}, ..., BSS_{s,h5,k}]
    IF all(bss_by_bracket[i] > bss_by_bracket[i+1] for i in range(4)):
        slope = linear_regression_slope(bss_by_bracket)
        IF slope < -0.05 per bracket:
            ALERT: "Station {s} steep diagonal degradation. Slope = {slope}."
            Severity: INFO
```

### 8.6.4 Weight Convergence

A single model dominates the ensemble weights:

```
FOR each (station, lead_bracket, target_type):
    weights = MODEL_WEIGHTS for this cell
    max_weight = max(weights)
    IF max_weight > 0.60:
        ALERT: "Model {model} dominates at {station}/{bracket}. Weight = {max_weight}."
        Severity: INFO
        Suggested action: Verify entropy regularization lambda is sufficient.
```

### 8.6.5 Alert Database

All pattern classifier alerts are written to `SYSTEM_ALERTS`:
- `ALERT_TYPE` (row_degradation, column_degradation, diagonal_degradation, weight_convergence)
- `SEVERITY` (INFO, WARNING, CRITICAL)
- `STATION_ID` (if applicable)
- `LEAD_BRACKET` (if applicable)
- `MESSAGE`
- `SUGGESTED_ACTION`
- `CREATED_AT`
- `ACKNOWLEDGED` (default 0 — set to 1 by human review)

## 8.7 Weekly Calibration Job

**File:** `kalshicast/evaluation/calibration.py`

Runs weekly (or on-demand via CLI). Evaluates whether [CALIBRATION REQUIRED] parameters should be adjusted.

### 8.7.1 Procedure

For each [CALIBRATION REQUIRED] parameter:

1. **Compute current performance** using the most recent `params.eval.bss_window_days` of data.
2. **Compute hypothetical performance** under candidate parameter values (grid search over valid range, 5 points).
3. **Apply BIC guard:**
   ```
   BIC = N × ln(RSS/N) + k × ln(N)
   ```
   Accept the new value only if it lowers BIC relative to the current value.
4. **Log to CALIBRATION_HISTORY:** old value, new value, BIC_old, BIC_new, metric_trigger.
5. **Update PARAMS table** if accepted.

### 8.7.2 Walk-Forward Validation Gate

Before any calibration change takes effect in production:

```
IF calibration change would alter a pricing or sizing parameter:
    Run walk-forward backtest (Section 10.4) over the last 90 days
    Accept ONLY IF:
        - median delta_SR_$ > 0
        - No BSS dominant cell drops below 0.05
        - MDD does not increase by > 5 percentage points
        - CAL does not increase by > 0.03
```

This prevents calibration from accidentally degrading live performance.

## 8.8 Complete Alert Conditions

| Alert | Severity | Condition | Action |
|-------|----------|-----------|--------|
| METAR collector down | WARNING | No METAR obs for > 2 × staleness_minutes | Pricing continues without truncation |
| AFD collector down | INFO | No AFD signal for > 24 hours | sigma_mod defaults to 1.0 |
| Order book stale | WARNING | VWAP delta > staleness_delta | Log and proceed with caution |
| Order book very stale | CRITICAL | VWAP delta > 2 × staleness_delta | Abort order submission |
| Maker adverse selection | WARNING | delta_fill < -0.02 | Reduce maker frequency |
| Maker adverse selection severe | CRITICAL | delta_fill < -0.05 | Disable maker orders |
| Row degradation | WARNING | Station mean BSS < bss_exit | Check station reliability |
| Column degradation | WARNING | Bracket mean BSS < bss_exit | Tighten lead ceiling |
| Weight convergence | INFO | max weight > 0.60 | Check entropy lambda |
| MDD approaching halt | WARNING | MDD > mdd_safe | D_scale is reducing sizing |
| MDD halt triggered | CRITICAL | MDD ≥ mdd_halt | All betting halted |
| Kalman gap detected | WARNING | state_version gap > 1 | Uncertainty inflated |
| P(win) sum anomaly | WARNING | |Σ P(win) - 1.0| > 0.01 | Check parameterization |
| Pipeline run missed | CRITICAL | No pipeline_run for > 2 × expected interval | Check scheduler |
| Oracle connection failure | CRITICAL | DB connection fails 3 consecutive attempts | Pipeline halts, retry on next scheduled run |
| Fee drag excessive | WARNING | FDR > 0.50 | Review minimum EV thresholds |
| Calibration drift | INFO | Parameter changed by > 20% in single week | Verify walk-forward results |
| CLI amendment detected | INFO | Observation value changed on re-fetch | Kalman retroactive correction triggered |

---

# Section 9: Pipeline Orchestration

**Owner:** `kalshicast/pipeline/`
**Files:** `morning.py`, `night.py`, `market_open.py`, `health.py`

The system runs three scheduled pipelines per day plus a health monitor. Each pipeline is a strict sequence of function calls with layer annotations, database writes, and failure recovery.

## 9.1 Night Pipeline

**Schedule:** `params.pipeline.night_utc_hour` (default 06:00 UTC)
**GitHub Actions trigger:** `cron: '0 6 * * *'`
**Purpose:** Ingest yesterday's observations, compute errors, update Kalman filters, refresh BSS matrix.

### 9.1.1 Execution Sequence

```
night.py run_id = generate_run_id()

Step  Layer  Function                           DB Writes
───── ───── ──────────────────────────────────── ─────────────────────────
 1    --    init_db()                            PIPELINE_RUNS (insert)
 2    --    load_params(db_conn)                 --
 3    L1    fetch_cli_observations(target_date)  OBSERVATIONS
 4    L1    check_amendments(lookback_days=3)    OBSERVATIONS (amend flag)
 5    L2    build_forecast_errors(target_date)   FORECAST_ERRORS
 6    L2    update_kalman_filters(target_date)   KALMAN_STATES, KALMAN_HISTORY
 7    L2    retroactive_kalman_correction()      KALMAN_STATES, KALMAN_HISTORY
            (only if amendments detected in step 4)
 8    L2    update_dashboard_stats(windows)      DASHBOARD_STATS
 9    L5    grade_brier_scores(target_date)      BRIER_SCORES
10    L5    refresh_bss_matrix()                 BSS_MATRIX, MODEL_WEIGHTS
11    L5    compute_financial_metrics()           FINANCIAL_METRICS
12    L5    run_pattern_classifier()              SYSTEM_ALERTS
            (only if day_of_week matches interval)
13    --    update_pipeline_run(status="OK")      PIPELINE_RUNS (update)
```

### 9.1.2 Target Date Computation

```python
from datetime import datetime, timedelta
import pytz

eastern = pytz.timezone("America/New_York")
now_eastern = datetime.now(eastern)
target_date = (now_eastern - timedelta(days=1)).date()
```

The target date is **yesterday in US Eastern time**. This ensures observations are final (CLI products are typically issued by midnight local time).

### 9.1.3 Failure Handling

| Step | On Failure | Recovery |
|------|-----------|----------|
| 1 (init_db) | ABORT pipeline | SYSTEM_ALERT CRITICAL. Retry on next schedule. |
| 3 (CLI fetch) | Continue with partial data | Log WARNING per station. Stations without obs skip steps 5–9 for that station. |
| 4 (amendments) | Continue | Non-critical. Log WARNING. |
| 5–8 (processing) | ABORT remaining steps for affected station | Log ERROR. Other stations continue. |
| 9–12 (evaluation) | Continue with partial data | Log WARNING. BSS matrix uses previous values. |

## 9.2 Morning Pipeline

**Schedule:** `params.pipeline.morning_utc_hour` (default 12:00 UTC)
**GitHub Actions trigger:** `cron: '0 12 * * *'`
**Purpose:** Collect fresh forecasts from all 9 sources for the next `forecast_days` days.

### 9.2.1 Execution Sequence

```
morning.py run_id = generate_run_id()

Step  Layer  Function                            DB Writes
───── ───── ───────────────────────────────────── ─────────────────────────
 1    --    init_db()                             PIPELINE_RUNS (insert)
 2    --    load_params(db_conn)                  --
 3    --    upsert_stations()                     STATIONS
 4    L1    load_fetchers()                       --
 5    --    lock_issued_at()                      --
 6    --    pre_cache_run_ids(9 sources)          FORECAST_RUNS
 7    L1    dispatch_collectors(20×9 tasks)       FORECASTS_DAILY, FORECASTS_HOURLY
            ThreadPoolExecutor(max_workers)
 8    --    log_collection_tally()                --
 9    --    update_pipeline_run(status="OK")      PIPELINE_RUNS (update)
```

### 9.2.2 Concurrency Model

```
ThreadPoolExecutor(max_workers=params.pipeline.max_workers)  # default 10

Per-source semaphores:
    TOM:  1
    WAPI: 2
    VCR:  2
    NWS:  4
    OME (all OME_* sources share): 4

Total in-flight: ≤10 at any time
```

Each task = one (station, source) pair. Futures are processed as they complete (not in submission order).

### 9.2.3 Failure Handling

Individual (station, source) failures do not halt the pipeline. Failed tasks are logged with the error message and traceback. The morning pipeline succeeds as long as ≥1 source per station returns data.

**Total failure threshold:** If >50% of tasks fail, the pipeline status is set to "PARTIAL" instead of "OK" and a SYSTEM_ALERT is generated.

## 9.3 Market Open Pipeline

**Schedule:** `params.pipeline.market_open_utc_hour` (default 14:00 UTC)
**GitHub Actions trigger:** `cron: '0 14 * * *'`
**Purpose:** Price the Shadow Book, run IBE, evaluate gates, size bets, submit orders.

This is the **critical pipeline** — it produces executable bet decisions and submits orders to Kalshi.

### 9.3.1 Execution Sequence

```
market_open.py run_id = generate_run_id()

Step  Layer  Function                            DB Writes
───── ───── ───────────────────────────────────── ─────────────────────────
 1    --    init_db()                             PIPELINE_RUNS (insert)
 2    --    load_params(db_conn)                  --
 3    L4    fetch_bankroll()                      -- (reads Kalshi API)
            b = balance / 100.0
 4    L1    fetch_metar_all_stations()            METAR_OBSERVATIONS, METAR_DAILY_MAX
 5    L1    fetch_afd_all_wfos()                  AFD_TEXT, AFD_SIGNALS
 6    L2    compute_ensemble_state()              ENSEMBLE_STATE, MODEL_WEIGHTS
            For each (station, target_date, target_type):
            - Top model selection
            - Weight computation (entropy regularized)
            - Spread computation
            - σ_eff computation
            - Skewness computation
 7    L3    price_shadow_book()                   SHADOW_BOOK, SHADOW_BOOK_HISTORY
            For each (station, target_date, target_type):
            - (μ, σ_eff, G1) → (ξ, ω, α) conversion
            - P(win) for every active bin
            - METAR truncation (if applicable)
            - Bimodal check (if activated)
 8    L4    fetch_market_prices()                 MARKET_ORDERBOOK_SNAPSHOTS
            For each active ticker:
            - GET /markets/{ticker}/orderbook
 9    L4    evaluate_gates_and_ibe()              BEST_BETS, IBE_SIGNAL_LOG
            For each candidate bin:
            - 5 conviction gates
            - 5 IBE signals
            - Kelly sizing (Smirnov)
            - Full sizing chain
10    L4    submit_orders()                       ORDER_LOG, POSITIONS
            For each bet in BEST_BETS:
            - VWAP computation
            - Staleness check
            - Maker/taker decision
            - Order submission (with tranche if needed)
11    L5    update_pipeline_day_health()          PIPELINE_DAY_HEALTH
12    --    update_pipeline_run(status="OK")      PIPELINE_RUNS (update)
```

### 9.3.2 Time Pressure

The market_open pipeline targets the 14:00 UTC window when Kalshi daily temperature markets are most liquid. The pipeline must complete steps 1–10 within **30 minutes** (by 14:30 UTC).

**Performance budget:**
| Step | Target Duration |
|------|----------------|
| 1–3 (init, params, bankroll) | < 10 seconds |
| 4–5 (METAR, AFD) | < 60 seconds |
| 6 (ensemble) | < 120 seconds |
| 7 (pricing) | < 60 seconds |
| 8 (market prices) | < 120 seconds |
| 9 (gates + IBE + Kelly) | < 60 seconds |
| 10 (order submission) | < 300 seconds (includes tranche delays) |
| Total | < 12 minutes |

### 9.3.3 Failure Handling

| Step | On Failure | Recovery |
|------|-----------|----------|
| 3 (bankroll) | ABORT | Cannot size bets without knowing bankroll |
| 4 (METAR) | Continue without truncation | Log WARNING |
| 5 (AFD) | Continue with sigma_mod=1.0 | Log WARNING |
| 6–7 (ensemble/pricing) | Skip affected stations | Log ERROR per station |
| 8 (market prices) | ABORT | Cannot compute edges without market prices |
| 9 (gates) | Skip affected bets | Log per bet |
| 10 (order submission) | Retry per order | Log ERROR. Do not retry same client_order_id. |

## 9.4 Health Monitor

**File:** `kalshicast/pipeline/health.py`
**Schedule:** Every `params.pipeline.health_heartbeat_sec` (default 300 seconds = 5 minutes).

### 9.4.1 Health Checks

```
1. Oracle DB ping (SELECT 1 FROM DUAL)
2. Check PIPELINE_RUNS for missed runs:
   - Expected morning run within last 18 hours
   - Expected night run within last 18 hours
   - Expected market_open run within last 18 hours
3. Check METAR freshness (last obs within staleness window)
4. Check MDD status (compare to mdd_safe and mdd_halt)
5. Check SYSTEM_ALERTS for unacknowledged CRITICAL alerts
```

### 9.4.2 Missed Run Detection

```
FOR each pipeline_type in [morning, night, market_open]:
    last_run = SELECT MAX(STARTED_AT) FROM PIPELINE_RUNS WHERE TYPE = pipeline_type
    expected_interval = 24 hours
    IF now - last_run > 2 × expected_interval:
        ALERT CRITICAL: "Missed {pipeline_type} run. Last run: {last_run}"
```

### 9.4.3 Pipeline Day Health

`PIPELINE_DAY_HEALTH` stores a per-day completeness summary:

| Column | Description |
|--------|-------------|
| `HEALTH_DATE` | Calendar date |
| `MORNING_STATUS` | OK / PARTIAL / FAILED / MISSED |
| `NIGHT_STATUS` | OK / PARTIAL / FAILED / MISSED |
| `MARKET_OPEN_STATUS` | OK / PARTIAL / FAILED / MISSED |
| `STATIONS_COLLECTED` | Count of stations with ≥1 source |
| `STATIONS_PRICED` | Count of stations with Shadow Book |
| `BETS_PLACED` | Count of orders submitted |
| `BETS_FILLED` | Count of orders filled |
| `ALERTS_GENERATED` | Count of SYSTEM_ALERTS |

## 9.5 Rollover Logic

**File:** `kalshicast/pipeline/rollover.py`

At midnight UTC, the system rolls over date-dependent state:

1. **METAR_DAILY_MAX:** Insert new rows for today's date with MAX_TEMP_F = -999, MIN_TEMP_F = 999, OBS_COUNT = 0.
2. **Shadow Book:** Previous day's Shadow Book rows are finalized (no further updates).
3. **Positions:** Check for settled positions (Kalshi settlement happens ~24h after market close). Update `POSITIONS.STATUS` from "open" to "settled" and record `SETTLEMENT_VALUE`.

## 9.6 CLI Amendment Re-Fetch

During the night pipeline (step 4), the system re-fetches CLI products for the past `amendment_lookback_days`:

```
FOR day in range(1, params.pipeline.amendment_lookback_days + 1):
    check_date = target_date - timedelta(days=day)
    new_obs = fetch_cli_observation(check_date)
    old_obs = SELECT FROM OBSERVATIONS WHERE STATION_ID = s AND VALID_DATE = check_date

    IF new_obs != old_obs:
        UPDATE OBSERVATIONS SET
            OBSERVED_HIGH = new_obs.high,
            OBSERVED_LOW = new_obs.low,
            AMENDED = 1,
            AMENDED_AT = SYSTIMESTAMP,
            ORIGINAL_HIGH_F = old_obs.high,
            ORIGINAL_LOW_F = old_obs.low
        Mark for retroactive Kalman correction (step 7)
```

## 9.7 run_id Stamping

Every pipeline run generates a UUID `run_id` at startup:

```python
import uuid
run_id = str(uuid.uuid4())
```

This `run_id` is:
- Written to `PIPELINE_RUNS` at the start of the pipeline.
- Passed to every function in the pipeline as a parameter.
- Written to every database row created during the run (where applicable).
- Used as a prefix for `client_order_id` in Kalshi orders (idempotency).

---

# Section 10: Testing Harness

**Owner:** `kalshicast/tests/`

Testing is structured by layer. Each layer has synthetic data generators, unit tests, and integration tests. A walk-forward backtesting harness spans the full pipeline.

## 10.1 Synthetic Data Generators

**File:** `kalshicast/tests/generators.py`

### 10.1.1 Forecast Generator

```
generate_synthetic_forecasts(
    n_stations: int = 5,
    n_models: int = 4,
    n_days: int = 90,
    base_temp: float = 75.0,
    noise_std: float = 3.0,
    bias_per_model: list[float] = [0.0, 1.0, -0.5, 0.3],
    seed: int = 42
) -> DataFrame
```

Produces a DataFrame of synthetic forecasts with:
- Temperatures drawn from `N(base_temp + seasonal_component + model_bias, noise_std²)`
- Seasonal component: `10 × sin(2π × day_of_year / 365)` (sinusoidal annual cycle)
- Hourly data interpolated from daily with diurnal cycle: `daily_mean + amplitude × sin(2π × (hour - 6) / 24)`
- Lead times computed from synthetic `issued_at` values
- Realistic missing data pattern (5% of model-station-day cells are NULL)

### 10.1.2 Observation Generator

```
generate_synthetic_observations(
    forecasts: DataFrame,
    observation_noise_std: float = 1.5,
    seed: int = 42
) -> DataFrame
```

Produces observations that are correlated with the "true" underlying temperature (not the forecasts). The true temperature is:
- `T_true = base_temp + seasonal + observation_noise`
- This ensures forecasts have realistic error distributions centered near zero with model-specific biases.

### 10.1.3 Market Price Generator

```
generate_synthetic_market_prices(
    shadow_book: DataFrame,
    market_noise_std: float = 0.05,
    vig: float = 0.03,
    seed: int = 42
) -> DataFrame
```

Produces market prices that are:
- Noisy versions of Shadow Book probabilities: `c = p + N(0, market_noise_std) + vig_spread`
- Vig adds symmetric spread: each price is shifted slightly away from 0.5
- Clipped to [0.01, 0.99]

### 10.1.4 METAR Generator

```
generate_synthetic_metar(
    observations: DataFrame,
    n_obs_per_day: int = 12,
    seed: int = 42
) -> DataFrame
```

Produces METAR observations that approach the final observed high/low through the day:
- Morning obs: `T_metar = T_true × 0.7 + base × 0.3`
- Afternoon obs: `T_metar = T_true × 0.95 + noise`
- Running max/min tracked cumulatively

## 10.2 Unit Tests

### 10.2.1 L1 Tests (`tests/test_collection.py`)

| Test | Assertion |
|------|-----------|
| `test_forecast_bundle_validation` | Invalid bundles (high < low, temp out of range) are rejected |
| `test_lead_time_computation` | Dynamic and fallback lead times match expected values |
| `test_lead_time_positive` | All lead times > 0 for future dates |
| `test_classify_lead_hours` | Boundary conditions: 0→h1, 11.99→h1, 12.0→h2, etc. |
| `test_metar_parsing` | Raw METAR strings parsed to correct temperature_f |
| `test_afd_keyword_scan` | Known AFD text produces expected confidence scores |
| `test_collector_retry` | Simulated 429 responses trigger retry with backoff |

### 10.2.2 L2 Tests (`tests/test_processing.py`)

| Test | Assertion |
|------|-----------|
| `test_rmse_known_errors` | RMSE([2, -2, 1, -1]) = √(10/4) = √2.5 ≈ 1.581 |
| `test_bayesian_shrinkage_limits` | N=0 → σ_adj = σ_global; N→∞ → σ_adj → σ_obs |
| `test_skewness_zero_variance` | All-identical errors → G1 = 0.0 |
| `test_skewness_known_distribution` | Known skewed sample → G1 within 10% of population skewness |
| `test_skewness_significance_fallback` | Low-N insignificant skewness → G1 = 0.0 |
| `test_entropy_weights_uniform_bss` | All BSS equal → weights ≈ 1/M |
| `test_entropy_weights_one_dominant` | One BSS >> others → weight < 1.0 (entropy prevents monopoly) |
| `test_kalman_convergence` | 100 steps with constant bias → B_k converges to true bias |
| `test_kalman_gap_recovery` | After 5-day gap, U_k inflated by gap_inflate_factor × Q × 5 |
| `test_spread_bessel_correction` | Spread of [70, 72] = √((1/1)×((70-71)²+(72-71)²)) = √2 |
| `test_sigma_eff_composition` | σ_eff² = σ² + k_spread × S² |

### 10.2.3 L3 Tests (`tests/test_pricing.py`)

| Test | Assertion |
|------|-----------|
| `test_skewnorm_normal_fallback` | G1=0 → skewnorm.cdf matches norm.cdf |
| `test_skewnorm_round_trip` | (μ, σ, G1) → (ξ, ω, α) → verify mean ≈ μ, std ≈ σ |
| `test_p_win_sums_to_one` | Σ P(win) across all bins ≈ 1.0 (within 0.001) |
| `test_p_win_floor` | Very unlikely bin → P(win) = p_min_floor, not 0 |
| `test_truncation_high` | T_obs_max = 85 → bins below 85 get P=0, rest renormalized |
| `test_truncation_low` | T_obs_min = 40 → bins above 40 get P=0, rest renormalized |
| `test_truncation_no_effect` | T_obs_max below all bins → P unchanged |
| `test_bimodal_mixture` | Two-cluster input → mixture P(win) differs from single skewnorm |
| `test_alpha_cap` | Extreme G1 → α clipped, no NaN |
| `test_bin_boundary_convention` | Ticker "[81 to 82]" → lower=80.5, upper=82.5 |

### 10.2.4 L4 Tests (`tests/test_execution.py`)

| Test | Assertion |
|------|-----------|
| `test_kelly_binary` | p=0.6, c=0.5 → f* = (0.6-0.5)/(1-0.5) = 0.20 |
| `test_kelly_smirnov_3bin` | Worked example from §7.3.3 → f_B = 0.167 |
| `test_kelly_smirnov_no_edge` | All e_i < 1 → S = empty → no bets |
| `test_kelly_cap` | f* = 0.30 → capped to kelly.fraction_cap (0.10) |
| `test_phi_continuous` | BSS=0.125 → Φ=0.50; BSS=0.25 → Φ=1.0 |
| `test_gate_ev_minimum` | EV_net below threshold → rejected |
| `test_gate_edge_buffer_adaptive` | N=10 → ε=0.310; N=1000 → ε=0.031 |
| `test_gate_spread_rejection` | S=5.0 > spread_max=4.0 → rejected |
| `test_gate_skill_hysteresis` | Enter at 0.07, do not exit until < 0.03 |
| `test_ibe_veto_kcv` | KCV_norm=5.0 > 4.0 → veto |
| `test_ibe_veto_mpds` | |MPDS|=0.15 > 0.12 → veto |
| `test_ibe_composite_range` | Composite always in [clip_low, clip_high] |
| `test_drawdown_scale` | MDD=0.15 → D_scale=0.50; MDD=0.20 → D_scale=0.0 |
| `test_fee_taker` | 10 contracts at $0.50 → fee = $0.18 |
| `test_fee_partial_fill` | 3 partial fills > aggregate rounding |
| `test_position_limits` | Exposure exceeds single cap → sized down |
| `test_vwap_staleness` | Large delta → alert generated |
| `test_tranche_execution` | 100 contracts → 4 tranches of 25 |

### 10.2.5 L5 Tests (`tests/test_evaluation.py`)

| Test | Assertion |
|------|-----------|
| `test_brier_score_perfect` | All P=A → BS = 0.0 |
| `test_brier_score_worst` | P=1, A=0 → BS = 1.0 |
| `test_bss_positive` | Model beats climatology → BSS > 0 |
| `test_bss_negative` | Model worse than climatology → BSS < 0 |
| `test_calibration_perfect` | Predicted probs match outcome frequencies → CAL ≈ 0 |
| `test_sharpe_positive` | Winning bets → SR_$ > 0 |
| `test_mdd_computation` | Known P&L sequence → MDD matches hand calculation |
| `test_pattern_row_degradation` | All-low BSS row → alert generated |
| `test_pattern_weight_convergence` | max_weight=0.70 → alert generated |
| `test_adverse_selection` | Maker fills worse → delta_fill < 0 → alert |

## 10.3 Integration Tests

**File:** `kalshicast/tests/test_integration.py`

### 10.3.1 Full Pipeline Over Synthetic Data

```
test_full_pipeline_30_days():
    1. Generate 30 days of synthetic forecasts (5 stations, 4 models)
    2. Generate corresponding observations
    3. Generate market prices
    4. Run night pipeline for each day (errors, Kalman, BSS)
    5. Run morning pipeline for each day (collection → DB write)
    6. Run market_open pipeline for each day (pricing → gates → Kelly → mock order)
    7. Assert:
       - All 30 days have PIPELINE_RUNS with status OK
       - KALMAN_STATES converged (U_k < U_init for all filters)
       - BSS_MATRIX has >0 qualified cells
       - BEST_BETS has >0 rows (some bets identified)
       - FINANCIAL_METRICS has 30 rows
       - No CRITICAL alerts generated
       - P(win) sums ≈ 1.0 for all priced markets
```

### 10.3.2 Layer Boundary Violation Test

```
test_no_upward_dependency():
    1. Parse import statements in all Python files
    2. Build dependency graph
    3. Assert:
       - No L2 file imports from L3, L4, or L5
       - No L3 file imports from L4 or L5
       - No L4 file imports from L5
       - L1 does not import from L2, L3, L4, or L5
```

### 10.3.3 Database Schema Test

```
test_schema_matches_spec():
    1. Run all CREATE TABLE statements against a test Oracle instance
    2. Query USER_TAB_COLUMNS for all tables
    3. Assert column names, types, and constraints match this specification
```

## 10.4 Walk-Forward Backtesting Harness

**File:** `kalshicast/tests/backtest.py`

### 10.4.1 Protocol

Walk-forward backtesting uses chronological train/test splits. **K-fold cross-validation is explicitly rejected** because temperature forecast errors are serially correlated — random fold assignment would leak future information into training.

```
Parameters:
    train_initial = 90 days (minimum initial training window)
    test_window = 30 days
    step_size = 30 days (train window grows by this much each iteration)

Iteration 1: Train on days [1, 90], test on days [91, 120]
Iteration 2: Train on days [1, 120], test on days [121, 150]
Iteration 3: Train on days [1, 150], test on days [151, 180]
...
```

### 10.4.2 Per-Window Metrics

For each test window, compute:

| Metric | Formula |
|--------|---------|
| `delta_BSS` | BSS_test - BSS_baseline |
| `delta_SR_$` | SR_$_test - SR_$_baseline |
| `delta_SR` | SR_test - SR_baseline |
| `delta_MDD` | MDD_test - MDD_baseline |
| `delta_CAL` | CAL_test - CAL_baseline |

Where baseline is the performance in the previous test window (or climatology for the first window).

### 10.4.3 Acceptance Criteria

A configuration change (parameter update, new signal, model addition) is accepted only if across all test windows:

```
1. median(delta_SR_$) > 0                              # Net profitable
2. No BSS dominant cell drops below 0.05               # No skill regression
3. No BSS dominant cell changes by > -30%              # No catastrophic cell loss
4. max(delta_MDD) < 0.05                               # Drawdown doesn't worsen by >5pp
5. max(delta_CAL) < 0.03                               # Calibration doesn't worsen by >0.03
```

### 10.4.4 Backtest Output

The harness produces a summary table:

```
Window  Train_Days  Test_Start  Test_End  BSS    SR_$   SR    MDD    CAL   Bets
  1     90          2026-01-01  2026-01-30 0.12  1.45  0.58  0.04  0.022   34
  2     120         2026-01-31  2026-03-01 0.14  1.62  0.61  0.06  0.019   41
  3     150         2026-03-02  2026-03-31 0.11  1.31  0.55  0.08  0.025   38
```

Plus per-station, per-bracket breakdowns.

---

# Section 11: Implementation Sequence

The build is organized into four phases. Each phase produces a runnable system with incrementally more capability. No phase depends on a future phase.

## Phase 1: Foundation (Schema + Config + Collection)

**Goal:** Database exists, configuration is loaded, data flows in.

**Files to build:**

```
kalshicast/
├── __init__.py
├── config/
│   ├── __init__.py
│   ├── params_bootstrap.py          # Bootstrap defaults for all params
│   ├── stations.py                  # 20 station definitions
│   └── sources.py                   # 9 source definitions
├── db/
│   ├── __init__.py
│   ├── connection.py                # Oracle connection pool (oracledb)
│   ├── schema.py                    # All CREATE TABLE statements
│   └── operations.py               # MERGE/INSERT helpers
├── collection/
│   ├── __init__.py
│   ├── collector_harness.py         # Shared retry, validation, semaphore
│   ├── lead_time.py                 # compute_lead_hours(), classify_lead_hours()
│   ├── collectors/
│   │   ├── __init__.py
│   │   ├── collect_nws.py
│   │   ├── collect_ome.py
│   │   ├── collect_ome_model.py
│   │   ├── collect_wapi.py
│   │   ├── collect_vcr.py
│   │   ├── collect_tom.py
│   │   └── collect_cli.py           # Refactored from cli_observations.py
│   └── sources_registry.py          # Dynamic loader
├── pipeline/
│   ├── __init__.py
│   └── morning.py                   # Morning pipeline (collection only)
```

**Database tables created:** All tables from Section 2.
**Validation:** Morning pipeline runs, collects from 9 sources, writes to FORECASTS_DAILY and FORECASTS_HOURLY. `SELECT COUNT(*) FROM FORECASTS_DAILY` returns 720 rows (20 stations × 9 sources × 4 days).

## Phase 2: Paper Trading (Processing + Pricing + Evaluation)

**Goal:** Full L1→L2→L3→L5 pipeline. Shadow Book priced, Brier scores computed, BSS matrix populated. No live orders.

**Files to build (in addition to Phase 1):**

```
kalshicast/
├── processing/
│   ├── __init__.py
│   ├── errors.py                    # Error computation
│   ├── sigma.py                     # RMSE + Bayesian shrinkage
│   ├── skewness.py                  # g_s, G1_s
│   ├── ensemble.py                  # Weights, spread, σ_eff, top model
│   ├── kalman.py                    # 1D Kalman filter (40 instances)
│   ├── regime.py                    # Bimodal detection (stub — NOT LIVE)
│   └── dashboard.py                 # Rolling dashboard stats
├── pricing/
│   ├── __init__.py
│   ├── shadow_book.py               # Skew-normal → P(win) for all bins
│   ├── truncation.py                # METAR truncation (stub until Phase 3)
│   └── bin_convention.py            # Ticker → boundary conversion
├── evaluation/
│   ├── __init__.py
│   ├── brier.py                     # BS and BSS computation
│   ├── bss_matrix.py                # 20×5×2 skill matrix refresh
│   ├── financial.py                 # SR_$, MDD, FDR, EUR (stubs — no bets yet)
│   ├── calibration.py               # CAL computation
│   └── pattern_classifier.py        # BSS matrix pattern scanner
├── pipeline/
│   ├── night.py                     # Night pipeline (obs → errors → Kalman → BSS)
│   └── morning.py                   # (updated to include ensemble state)
├── tests/
│   ├── __init__.py
│   ├── generators.py                # Synthetic data generators
│   ├── test_processing.py           # L2 unit tests
│   ├── test_pricing.py              # L3 unit tests
│   └── test_evaluation.py           # L5 unit tests
```

**Validation:**
- Night pipeline ingests observations, computes errors, updates Kalman filters.
- Morning pipeline collects + computes ensemble state.
- Shadow Book prices all active bins. P(win) sums verify ≈ 1.0.
- BSS matrix has qualified cells after 14+ days of data.
- Run the 30-day integration test with synthetic data.

**Duration estimate tag:** [CALIBRATION REQUIRED] — run Phase 2 for a minimum of 30 days before proceeding to Phase 3, to populate the BSS matrix with real data.

## Phase 3: Live Betting (Execution Layer)

**Goal:** System identifies best bets, sizes them, and submits orders to Kalshi at $10 bankroll.

**Files to build (in addition to Phases 1–2):**

```
kalshicast/
├── collection/
│   ├── collectors/
│   │   ├── collect_metar.py         # METAR intraday collector (NEW)
│   │   └── collect_afd.py           # AFD text signal collector (NEW)
├── pricing/
│   └── truncation.py                # (activated — reads METAR_DAILY_MAX)
├── execution/
│   ├── __init__.py
│   ├── gates.py                     # 5 conviction gates
│   ├── ibe.py                       # 5 IBE signals + composite
│   ├── kelly.py                     # Smirnov mutually-exclusive Kelly
│   ├── sizing.py                    # Full sizing chain (Φ, IBE, caps, D_scale, jitter)
│   ├── order.py                     # VWAP, tranche, maker/taker, submission
│   └── kalshi_api.py                # Kalshi REST API client (auth, endpoints)
├── pipeline/
│   └── market_open.py               # Market-open pipeline (pricing → gates → orders)
├── tests/
│   ├── test_execution.py            # L4 unit tests
│   ├── test_collection.py           # L1 unit tests (including METAR/AFD)
│   └── test_integration.py          # Full pipeline integration test
```

**Validation:**
- Market-open pipeline runs end-to-end in < 15 minutes.
- BEST_BETS populated with gate-passing bets.
- Orders submitted to Kalshi (start with `post_only: true` for first 3 days to verify without taking liquidity).
- ORDER_LOG records all submissions and responses.
- Position limits are respected (verify via `GET /portfolio/positions`).

## Phase 4: Monitoring and Calibration

**Goal:** Automated monitoring, adaptive calibration, backtesting infrastructure, operational maturity.

**Files to build (in addition to Phases 1–3):**

```
kalshicast/
├── evaluation/
│   ├── adverse_selection.py         # Maker adverse selection test
│   ├── calibration.py               # (upgraded — weekly auto-calibration with BIC guard)
│   └── financial.py                 # (upgraded — all financial metrics active)
├── pipeline/
│   ├── health.py                    # Health monitor (heartbeat, missed run detection)
│   └── rollover.py                  # Date rollover logic
├── tests/
│   └── backtest.py                  # Walk-forward backtesting harness
├── github_workflows/
│   ├── morning.yml                  # cron: '0 12 * * *'
│   ├── night.yml                    # cron: '0 6 * * *'
│   ├── market_open.yml              # cron: '0 14 * * *'
│   ├── health.yml                   # cron: '*/5 * * * *'
│   └── calibration.yml              # cron: '0 3 * * 0' (weekly Sunday 03:00 UTC)
```

**Validation:**
- Health monitor detects and alerts on missed runs.
- Weekly calibration job runs, evaluates parameter changes, logs to CALIBRATION_HISTORY.
- Walk-forward backtest produces summary table with acceptance criteria evaluation.
- All SYSTEM_ALERTS are generated correctly for known degradation scenarios.
- Adverse selection test detects simulated poor maker fills.

## Phase Summary

| Phase | Capability | Minimum Duration | Key Deliverable |
|-------|-----------|-----------------|-----------------|
| 1 | Data collection | 1 week | 720 daily rows per morning run |
| 2 | Paper trading | 30 days minimum | BSS matrix with qualified cells |
| 3 | Live betting at $10 | Ongoing | First live Kalshi order |
| 4 | Monitoring + calibration | Ongoing | Self-improving system |

**Critical path:** Phase 2 requires a minimum of 30 calendar days to populate the BSS matrix with enough data for meaningful skill assessment. Phase 3 should not begin until at least 3 BSS matrix cells per station show BSS > 0.07 (skill gate entry threshold). Launching Phase 3 prematurely with an under-populated BSS matrix will result in the Skill Gate rejecting all bets.

---

*End of KalshiCast v10 specification.*
