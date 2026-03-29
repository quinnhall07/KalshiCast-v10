"""All CREATE TABLE statements — authoritative schema source.

Oracle Autonomous Database (free tier). All 32 tables from v10 spec Section 2.
Tables are created empty; Phase 1 populates config + L1 tables only.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# DDL — grouped by layer
# ─────────────────────────────────────────────────────────────────────

ALL_DDL: list[str] = [
    # ── Configuration Tables ──

    """CREATE TABLE STATIONS (
        STATION_ID    VARCHAR2(10)  NOT NULL,
        CLI_SITE      VARCHAR2(10),
        NAME          VARCHAR2(100),
        CITY          VARCHAR2(50),
        STATE_CODE    VARCHAR2(2),
        TIMEZONE      VARCHAR2(40),
        LAT           NUMBER(9,6),
        LON           NUMBER(10,6),
        ELEVATION_FT  NUMBER(6,0),
        WFO_ID        VARCHAR2(4),
        IS_ACTIVE     NUMBER(1) DEFAULT 1,
        IS_RELIABLE   NUMBER(1) DEFAULT 1,
        RELIABILITY_NOTE VARCHAR2(500),
        FLAGGED_AT    TIMESTAMP(6),
        CREATED_AT    TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        CONSTRAINT PK_STATIONS PRIMARY KEY (STATION_ID)
    )""",

    """CREATE TABLE SOURCES (
        SOURCE_ID         VARCHAR2(20)  NOT NULL,
        NAME              VARCHAR2(100),
        MODULE_PATH       VARCHAR2(200),
        FUNC_NAME         VARCHAR2(100),
        PROVIDER_GROUP    VARCHAR2(10),
        PARAMS_JSON       CLOB,
        IS_ENABLED        NUMBER(1) DEFAULT 1,
        UPDATE_CYCLE_HOURS NUMBER(4,1),
        CREATED_AT        TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        CONSTRAINT PK_SOURCES PRIMARY KEY (SOURCE_ID)
    )""",

    """CREATE TABLE PARAMS (
        PARAM_KEY           VARCHAR2(100) NOT NULL,
        PARAM_VALUE         VARCHAR2(500),
        DTYPE               VARCHAR2(10),
        VALID_RANGE         VARCHAR2(100),
        DESCRIPTION         VARCHAR2(500),
        OWNING_FORMULA      VARCHAR2(200),
        IS_CALIBRATION_REQUIRED NUMBER(1) DEFAULT 0,
        LAST_CHANGED_AT     TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        CHANGED_BY          VARCHAR2(100),
        CHANGE_REASON       VARCHAR2(500),
        CONSTRAINT PK_PARAMS PRIMARY KEY (PARAM_KEY)
    )""",

    # ── L1: Raw Collection Tables ──

    """CREATE TABLE PIPELINE_RUNS (
        RUN_ID        VARCHAR2(36) NOT NULL,
        RUN_TYPE      VARCHAR2(20),
        SCHEDULED_UTC TIMESTAMP(6),
        STARTED_UTC   TIMESTAMP(6),
        COMPLETED_UTC TIMESTAMP(6),
        STATUS        VARCHAR2(20),
        M_K           NUMBER(3,0),
        STATIONS_OK   NUMBER(3,0),
        STATIONS_FAIL NUMBER(3,0),
        ROWS_DAILY    NUMBER(8,0),
        ROWS_HOURLY   NUMBER(8,0),
        ERROR_MSG     VARCHAR2(2000),
        CONSTRAINT PK_PIPELINE_RUNS PRIMARY KEY (RUN_ID)
    )""",

    """CREATE TABLE FORECAST_RUNS (
        RUN_ID     VARCHAR2(36) NOT NULL,
        SOURCE_ID  VARCHAR2(20),
        ISSUED_AT  TIMESTAMP(6),
        INIT_TIME  TIMESTAMP(6),
        CREATED_AT TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        CONSTRAINT PK_FORECAST_RUNS PRIMARY KEY (RUN_ID)
    )""",

    """CREATE TABLE FORECASTS_DAILY (
        RUN_ID          VARCHAR2(36),
        SOURCE_ID       VARCHAR2(20),
        STATION_ID      VARCHAR2(10),
        TARGET_DATE     DATE,
        HIGH_F          NUMBER(5,1),
        LOW_F           NUMBER(5,1),
        LEAD_HOURS_HIGH NUMBER(6,1),
        LEAD_HOURS_LOW  NUMBER(6,1),
        LEAD_BRACKET_HIGH VARCHAR2(2),
        LEAD_BRACKET_LOW  VARCHAR2(2),
        CREATED_AT      TIMESTAMP(6) DEFAULT SYSTIMESTAMP
    )""",

    """CREATE TABLE FORECASTS_HOURLY (
        RUN_ID          VARCHAR2(36),
        SOURCE_ID       VARCHAR2(20),
        STATION_ID      VARCHAR2(10),
        VALID_TIME_UTC  TIMESTAMP(6),
        TEMPERATURE_F   NUMBER(5,1),
        DEWPOINT_F      NUMBER(5,1),
        HUMIDITY_PCT    NUMBER(5,2),
        WIND_SPEED_MPH  NUMBER(5,1),
        WIND_DIR_DEG    NUMBER(3,0),
        CLOUD_COVER_PCT NUMBER(3,0),
        PRECIP_PROB_PCT NUMBER(3,0),
        PRECIP_TYPE_CODE NUMBER(3,0),
        CREATED_AT      TIMESTAMP(6) DEFAULT SYSTIMESTAMP
    )""",

    """CREATE TABLE OBSERVATIONS (
        STATION_ID       VARCHAR2(10) NOT NULL,
        TARGET_DATE      DATE NOT NULL,
        OBSERVED_HIGH_F  NUMBER(5,1),
        OBSERVED_LOW_F   NUMBER(5,1),
        SOURCE           VARCHAR2(10) DEFAULT 'CLI',
        INGESTED_AT      TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        AMENDED          NUMBER(1) DEFAULT 0,
        AMENDED_AT       TIMESTAMP(6),
        ORIGINAL_HIGH_F  NUMBER(5,1),
        ORIGINAL_LOW_F   NUMBER(5,1),
        FLAGGED_RAW_TEXT CLOB,
        FLAGGED_REASON   VARCHAR2(500),
        CONSTRAINT PK_OBSERVATIONS PRIMARY KEY (STATION_ID, TARGET_DATE)
    )""",

    """CREATE TABLE OBSERVATION_RUNS (
        RUN_ID        VARCHAR2(36) NOT NULL,
        RUN_ISSUED_AT TIMESTAMP(6),
        CREATED_AT    TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        CONSTRAINT PK_OBSERVATION_RUNS PRIMARY KEY (RUN_ID)
    )""",

    """CREATE TABLE METAR_OBSERVATIONS (
        STATION_ID    VARCHAR2(10),
        OBSERVED_UTC  TIMESTAMP(6),
        TEMPERATURE_F NUMBER(5,1),
        DEW_POINT_F   NUMBER(5,1),
        WIND_SPEED_KT NUMBER(5,1),
        WIND_DIR_DEG  NUMBER(3,0),
        RAW_METAR     VARCHAR2(500),
        INGESTED_AT   TIMESTAMP(6) DEFAULT SYSTIMESTAMP
    )""",

    """CREATE TABLE METAR_DAILY_MAX (
        STATION_ID      VARCHAR2(10) NOT NULL,
        LOCAL_DATE      DATE NOT NULL,
        T_OBS_MAX_F     NUMBER(5,1),
        T_OBS_MIN_F     NUMBER(5,1),
        OBS_COUNT       NUMBER(4,0) DEFAULT 0,
        LAST_OBS_AT     TIMESTAMP(6),
        LAST_UPDATED_UTC TIMESTAMP(6),
        CONSTRAINT PK_METAR_DAILY_MAX PRIMARY KEY (STATION_ID, LOCAL_DATE)
    )""",

    """CREATE TABLE AFD_TEXT (
        STATION_ID      VARCHAR2(10),
        WFO_ID          VARCHAR2(4),
        ISSUED_UTC      TIMESTAMP(6),
        DISCUSSION_TEXT CLOB,
        FETCHED_AT      TIMESTAMP(6) DEFAULT SYSTIMESTAMP
    )""",

    """CREATE TABLE AFD_SIGNALS (
        STATION_ID              VARCHAR2(10),
        ISSUED_UTC              TIMESTAMP(6),
        CONFIDENCE_FLAG         VARCHAR2(10),
        MODEL_DISAGREEMENT_FLAG NUMBER(1),
        DIRECTIONAL_NOTE        VARCHAR2(200),
        SIGMA_MULTIPLIER        NUMBER(4,2) DEFAULT 1.00
    )""",

    # ── L2: Processing / Derived State Tables ──

    """CREATE TABLE KALMAN_STATES (
        STATION_ID           VARCHAR2(10) NOT NULL,
        TARGET_TYPE          VARCHAR2(4) NOT NULL,
        B_K                  NUMBER(10,6),
        U_K                  NUMBER(10,6),
        Q_BASE               NUMBER(10,6),
        STATE_VERSION        NUMBER(10,0) DEFAULT 0,
        TOP_MODEL_ID         VARCHAR2(20),
        LAST_OBSERVATION_DATE DATE,
        LAST_UPDATED_UTC     TIMESTAMP(6),
        CONSTRAINT PK_KALMAN_STATES PRIMARY KEY (STATION_ID, TARGET_TYPE)
    )""",

    """CREATE TABLE KALMAN_HISTORY (
        STATION_ID       VARCHAR2(10),
        TARGET_TYPE      VARCHAR2(4),
        PIPELINE_RUN_ID  VARCHAR2(36),
        B_K              NUMBER(10,6),
        U_K              NUMBER(10,6),
        Q_K              NUMBER(10,6),
        R_K              NUMBER(10,6),
        K_K              NUMBER(10,6),
        EPSILON_K        NUMBER(10,6),
        STATE_VERSION    NUMBER(10,0),
        IS_AMENDMENT     NUMBER(1) DEFAULT 0,
        CREATED_AT       TIMESTAMP(6) DEFAULT SYSTIMESTAMP
    )""",

    """CREATE TABLE ENSEMBLE_STATE (
        RUN_ID          VARCHAR2(36) NOT NULL,
        STATION_ID      VARCHAR2(10) NOT NULL,
        TARGET_DATE     DATE NOT NULL,
        TARGET_TYPE     VARCHAR2(4) NOT NULL,
        F_TK_TOP        NUMBER(6,2),
        TOP_MODEL_ID    VARCHAR2(20),
        F_BAR_TK        NUMBER(6,2),
        S_TK            NUMBER(6,3),
        S_WEIGHTED_TK   NUMBER(6,3),
        SIGMA_EFF       NUMBER(6,3),
        M_K             NUMBER(3,0),
        WEIGHT_JSON     CLOB,
        STALE_MODEL_IDS VARCHAR2(200),
        CONSTRAINT PK_ENSEMBLE_STATE PRIMARY KEY (RUN_ID, STATION_ID, TARGET_DATE, TARGET_TYPE)
    )""",

    """CREATE TABLE DASHBOARD_STATS (
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
        COMPUTED_AT       TIMESTAMP(6) DEFAULT SYSTIMESTAMP
    )""",

    """CREATE TABLE BSS_MATRIX (
        STATION_ID      VARCHAR2(10) NOT NULL,
        TARGET_TYPE     VARCHAR2(4) NOT NULL,
        LEAD_BRACKET    VARCHAR2(2) NOT NULL,
        WINDOW_DAYS     NUMBER(4,0),
        BS_MODEL        NUMBER(10,6),
        BS_BASELINE_1   NUMBER(10,6),
        BS_BASELINE_2   NUMBER(10,6),
        BSS_1           NUMBER(10,6),
        BSS_2           NUMBER(10,6),
        IS_QUALIFIED    NUMBER(1) DEFAULT 0,
        ENTERED_AT      TIMESTAMP(6),
        EXITED_AT       TIMESTAMP(6),
        H_STAR_S        VARCHAR2(2),
        N_OBSERVATIONS  NUMBER(6,0),
        COMPUTED_AT     TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        CONSTRAINT PK_BSS_MATRIX PRIMARY KEY (STATION_ID, TARGET_TYPE, LEAD_BRACKET)
    )""",

    """CREATE TABLE MODEL_WEIGHTS (
        RUN_ID              VARCHAR2(36),
        STATION_ID          VARCHAR2(10),
        SOURCE_ID           VARCHAR2(20),
        LEAD_BRACKET        VARCHAR2(2),
        W_M                 NUMBER(8,6),
        BSS_M               NUMBER(8,6),
        IS_STALE            NUMBER(1),
        STALE_DECAY_FACTOR  NUMBER(6,4),
        COMPUTED_AT         TIMESTAMP(6) DEFAULT SYSTIMESTAMP
    )""",

    # ── L3: Pricing Tables ──

    """CREATE TABLE SHADOW_BOOK (
        TICKER          VARCHAR2(100) NOT NULL,
        STATION_ID      VARCHAR2(10),
        TARGET_DATE     DATE,
        TARGET_TYPE     VARCHAR2(4),
        BIN_LOWER       NUMBER(5,1),
        BIN_UPPER       NUMBER(5,1),
        MU              NUMBER(6,2),
        SIGMA_EFF       NUMBER(6,3),
        G1_S            NUMBER(8,5),
        ALPHA_S         NUMBER(10,6),
        XI_S            NUMBER(8,3),
        OMEGA_S         NUMBER(8,3),
        P_WIN           NUMBER(8,6),
        METAR_TRUNCATED NUMBER(1) DEFAULT 0,
        T_OBS_MAX       NUMBER(5,1),
        TOP_MODEL_ID    VARCHAR2(20),
        PIPELINE_RUN_ID VARCHAR2(36),
        CREATED_AT      TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        UPDATED_AT      TIMESTAMP(6),
        CONSTRAINT PK_SHADOW_BOOK PRIMARY KEY (TICKER)
    )""",

    """CREATE TABLE SHADOW_BOOK_HISTORY (
        ID              NUMBER GENERATED ALWAYS AS IDENTITY,
        TICKER          VARCHAR2(100),
        P_WIN           NUMBER(8,6),
        MU              NUMBER(6,2),
        SIGMA_EFF       NUMBER(6,3),
        PIPELINE_RUN_ID VARCHAR2(36),
        RECORDED_AT     TIMESTAMP(6) DEFAULT SYSTIMESTAMP
    )""",

    """CREATE TABLE REGIME_FLAGS (
        STATION_ID          VARCHAR2(10),
        TARGET_DATE         DATE,
        TARGET_TYPE         VARCHAR2(4),
        PIPELINE_RUN_ID     VARCHAR2(36),
        IQR_F               NUMBER(6,3),
        S_TK_F              NUMBER(6,3),
        BIMODAL_TRIGGERED   NUMBER(1),
        CENTROID_1          NUMBER(6,2),
        CENTROID_2          NUMBER(6,2),
        CLUSTER_SIZE_1      NUMBER(3,0),
        CLUSTER_SIZE_2      NUMBER(3,0),
        RECORDED_AT         TIMESTAMP(6) DEFAULT SYSTIMESTAMP
    )""",

    # ── L4: Execution Tables ──

    """CREATE TABLE BEST_BETS (
        TICKER                     VARCHAR2(100) NOT NULL,
        PIPELINE_RUN_ID            VARCHAR2(36),
        STATION_ID                 VARCHAR2(10),
        TARGET_DATE                DATE,
        TARGET_TYPE                VARCHAR2(4),
        BIN_LOWER                  NUMBER(5,1),
        BIN_UPPER                  NUMBER(5,1),
        P_WIN                      NUMBER(8,6),
        CONTRACT_PRICE             NUMBER(6,4),
        EV_NET                     NUMBER(8,6),
        EV_THRESHOLD_H             NUMBER(8,6),
        ORDER_TYPE                 VARCHAR2(10),
        C_VWAP                     NUMBER(6,4),
        C_VWAP_NET                 NUMBER(6,4),
        F_STAR                     NUMBER(8,6),
        F_OP                       NUMBER(8,6),
        F_FINAL                    NUMBER(8,6),
        IBE_COMPOSITE              NUMBER(6,4),
        IBE_VETO                   NUMBER(1) DEFAULT 0,
        D_SCALE                    NUMBER(6,4),
        GAMMA_CONVERGENCE          NUMBER(6,4),
        RANK_WITHIN_STATION_DAY    NUMBER(3,0),
        IS_SELECTED_FOR_EXECUTION  NUMBER(1),
        PIPELINE_RUN_STATUS        VARCHAR2(12),
        ALL_GATE_FLAGS_JSON        CLOB,
        CONSTRAINT PK_BEST_BETS PRIMARY KEY (TICKER)
    )""",

    """CREATE TABLE POSITIONS (
        POSITION_ID       VARCHAR2(36) NOT NULL,
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
    )""",

    """CREATE TABLE ORDER_LOG (
        ORDER_ID            VARCHAR2(36) NOT NULL,
        POSITION_ID         VARCHAR2(36),
        TICKER              VARCHAR2(100),
        CONTRACTS           NUMBER(6,0),
        LIMIT_PRICE         NUMBER(6,4),
        ORDER_TYPE          VARCHAR2(10),
        SUBMITTED_AT        TIMESTAMP(6),
        KALSHI_RESPONSE_JSON CLOB,
        STATUS              VARCHAR2(20),
        ERROR_MSG           VARCHAR2(2000),
        CONSTRAINT PK_ORDER_LOG PRIMARY KEY (ORDER_ID)
    )""",

    """CREATE TABLE MARKET_ORDERBOOK_SNAPSHOTS (
        TICKER            VARCHAR2(100),
        SNAPSHOT_UTC      TIMESTAMP(6),
        YES_BOOK_JSON     CLOB,
        NO_BOOK_JSON      CLOB,
        C_VWAP_COMPUTED   NUMBER(6,4),
        AVAILABLE_DEPTH   NUMBER(6,0)
    )""",

    # ── L5: Evaluation Tables ──

    """CREATE TABLE FORECAST_ERRORS (
        STATION_ID      VARCHAR2(10),
        SOURCE_ID       VARCHAR2(20),
        TARGET_DATE     DATE,
        TARGET_TYPE     VARCHAR2(4),
        LEAD_BRACKET    VARCHAR2(2),
        LEAD_HOURS      NUMBER(6,1),
        RUN_ID          VARCHAR2(36),
        F_RAW           NUMBER(6,2),
        F_ADJUSTED      NUMBER(6,2),
        OBSERVED        NUMBER(6,2),
        ERROR_RAW       NUMBER(8,4),
        ERROR_ADJUSTED  NUMBER(8,4)
    )""",

    """CREATE TABLE BRIER_SCORES (
        TICKER           VARCHAR2(100) NOT NULL,
        P_WIN_AT_GRADING NUMBER(8,6),
        OUTCOME          NUMBER(1),
        BRIER_SCORE      NUMBER(10,8),
        GRADED_AT        TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        CONSTRAINT PK_BRIER_SCORES PRIMARY KEY (TICKER)
    )""",

    """CREATE TABLE CALIBRATION_HISTORY (
        ID              NUMBER GENERATED ALWAYS AS IDENTITY,
        COMPUTED_AT     TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        RECORD_TYPE     VARCHAR2(20),
        STATION_ID      VARCHAR2(10),
        WINDOW_DAYS     NUMBER(4,0),
        N_PREDICTIONS   NUMBER(6,0),
        CAL_SYSTEM      NUMBER(8,6),
        CAL_MARKET      NUMBER(8,6),
        BUCKET_DATA_JSON CLOB,
        PARAM_KEY       VARCHAR2(100),
        OLD_VALUE       VARCHAR2(200),
        NEW_VALUE       VARCHAR2(200),
        BIC_OLD         NUMBER(12,4),
        BIC_NEW         NUMBER(12,4),
        METRIC_TRIGGER  VARCHAR2(200),
        CONSTRAINT PK_CALIBRATION_HISTORY PRIMARY KEY (ID)
    )""",

    """CREATE TABLE FINANCIAL_METRICS (
        METRIC_DATE       DATE NOT NULL,
        COMPUTED_AT       TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
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
    )""",

    """CREATE TABLE SYSTEM_ALERTS (
        ALERT_ID       VARCHAR2(36) NOT NULL,
        ALERT_TS       TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
        ALERT_TYPE     VARCHAR2(50),
        STATION_ID     VARCHAR2(10),
        SOURCE_ID      VARCHAR2(20),
        SEVERITY_SCORE NUMBER(8,4),
        DETAILS_JSON   CLOB,
        IS_RESOLVED    NUMBER(1) DEFAULT 0,
        RESOLVED_TS    TIMESTAMP(6),
        RESOLVED_BY    VARCHAR2(100),
        CONSTRAINT PK_SYSTEM_ALERTS PRIMARY KEY (ALERT_ID)
    )""",

    """CREATE TABLE PIPELINE_DAY_HEALTH (
        TARGET_DATE          DATE NOT NULL,
        RUN_TS               TIMESTAMP(6),
        STATIONS_ACTIVE      NUMBER(3,0),
        STATIONS_FORECASTED  NUMBER(3,0),
        STATIONS_OBSERVED    NUMBER(3,0),
        STATIONS_SCORED      NUMBER(3,0),
        MODELS_ACTIVE        NUMBER(3,0),
        MODELS_INGESTED      NUMBER(3,0),
        IS_HEALTHY           NUMBER(1),
        FAILURE_REASONS_JSON CLOB,
        CONSTRAINT PK_PDH PRIMARY KEY (TARGET_DATE)
    )""",

    """CREATE TABLE IBE_SIGNAL_LOG (
        TICKER          VARCHAR2(100),
        PIPELINE_RUN_ID VARCHAR2(36),
        KCV_NORM        NUMBER(8,4),
        KCV_MOD         NUMBER(6,4),
        MPDS_K          NUMBER(8,6),
        MPDS_MOD        NUMBER(6,4),
        HMAS            NUMBER(6,4),
        HMAS_MOD        NUMBER(6,4),
        FCT             NUMBER(8,4),
        FCT_MOD         NUMBER(6,4),
        SCAS            NUMBER(8,4),
        SCAS_MOD        NUMBER(6,4),
        COMPOSITE       NUMBER(6,4),
        VETO_TRIGGERED  NUMBER(1),
        VETO_REASON     VARCHAR2(200),
        RECORDED_AT     TIMESTAMP(6) DEFAULT SYSTIMESTAMP
    )""",
]

ALL_INDEXES: list[str] = [
    "CREATE INDEX IDX_PIPELINE_RUNS_SCHED ON PIPELINE_RUNS (SCHEDULED_UTC)",
    "CREATE UNIQUE INDEX IDX_FORECAST_RUNS_SRC ON FORECAST_RUNS (SOURCE_ID, ISSUED_AT)",
    "CREATE INDEX IDX_FCDAILY_LOOKUP ON FORECASTS_DAILY (STATION_ID, TARGET_DATE, SOURCE_ID)",
    "CREATE INDEX IDX_FCHOURLY_LOOKUP ON FORECASTS_HOURLY (STATION_ID, VALID_TIME_UTC, SOURCE_ID)",
    "CREATE INDEX IDX_METAR_LOOKUP ON METAR_OBSERVATIONS (STATION_ID, OBSERVED_UTC DESC)",
    "CREATE INDEX IDX_AFD_LOOKUP ON AFD_TEXT (STATION_ID, ISSUED_UTC DESC)",
    "CREATE INDEX IDX_SB_STATION_DATE ON SHADOW_BOOK (STATION_ID, TARGET_DATE, TARGET_TYPE)",
    "CREATE INDEX IDX_POS_OPEN ON POSITIONS (STATION_ID, TARGET_DATE, TARGET_TYPE, STATUS)",
    "CREATE INDEX IDX_FCERR_LOOKUP ON FORECAST_ERRORS (STATION_ID, SOURCE_ID, TARGET_TYPE, LEAD_BRACKET, TARGET_DATE)",
    "CREATE INDEX IDX_DSTATS_LOOKUP ON DASHBOARD_STATS (STATION_ID, SOURCE_ID, TARGET_TYPE, LEAD_BRACKET)",
    "CREATE INDEX IDX_ORDERBOOK_LOOKUP ON MARKET_ORDERBOOK_SNAPSHOTS (TICKER, SNAPSHOT_UTC DESC)",
]


def _table_name_from_ddl(ddl: str) -> str:
    """Extract table name from CREATE TABLE statement."""
    parts = ddl.strip().split()
    for i, p in enumerate(parts):
        if p.upper() == "TABLE":
            return parts[i + 1].strip("(").upper()
    return ""


def ensure_schema(conn: Any) -> list[str]:
    """Create any missing tables and indexes. Returns list of created table names."""
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM user_tables")
        existing = {row[0].upper() for row in cur}

    created = []
    for ddl in ALL_DDL:
        tname = _table_name_from_ddl(ddl)
        if tname and tname not in existing:
            with conn.cursor() as cur:
                cur.execute(ddl)
            created.append(tname)
            log.info("Created table %s", tname)

    # Indexes — skip if already exists (ORA-00955)
    for idx_sql in ALL_INDEXES:
        try:
            with conn.cursor() as cur:
                cur.execute(idx_sql)
        except Exception:
            pass  # index already exists

    conn.commit()
    return created


def seed_config_tables(conn: Any) -> None:
    """Populate STATIONS, SOURCES, and PARAMS from bootstrap config."""
    import json
    from kalshicast.config.stations import STATIONS
    from kalshicast.config.sources import SOURCES
    from kalshicast.config.params_bootstrap import PARAM_DEFS

    # Seed STATIONS
    for s in STATIONS:
        with conn.cursor() as cur:
            cur.execute("""
                MERGE INTO STATIONS tgt USING DUAL
                ON (tgt.STATION_ID = :sid)
                WHEN NOT MATCHED THEN INSERT (
                    STATION_ID, CLI_SITE, NAME, CITY, STATE_CODE,
                    TIMEZONE, LAT, LON, ELEVATION_FT, WFO_ID, IS_ACTIVE
                ) VALUES (
                    :sid, :cli, :st_name, :city, :st_state,
                    :tz, :lat, :lon, :elev, :wfo, :active
                )
            """, {
                "sid": s["station_id"], "cli": s.get("cli_site"),
                "st_name": s.get("name"), "city": s.get("city"),
                "st_state": s.get("state"), "tz": s.get("timezone"),
                "lat": s.get("lat"), "lon": s.get("lon"),
                "elev": s.get("elevation_ft"), "wfo": s.get("wfo_id"),
                "active": 1 if s.get("is_active") else 0,
            })

    # Seed SOURCES
    for src_id, spec in SOURCES.items():
        with conn.cursor() as cur:
            cur.execute("""
                MERGE INTO SOURCES tgt USING DUAL
                ON (tgt.SOURCE_ID = :sid)
                WHEN NOT MATCHED THEN INSERT (
                    SOURCE_ID, NAME, MODULE_PATH, FUNC_NAME,
                    PROVIDER_GROUP, PARAMS_JSON, IS_ENABLED, UPDATE_CYCLE_HOURS
                ) VALUES (
                    :sid, :src_name, :mod_path, :func,
                    :pg, :pj, :en, :uch
                )
            """, {
                "sid": src_id, "src_name": spec.get("name"),
                "mod_path": spec.get("module"), "func": spec.get("func"),
                "pg": spec.get("provider_group"),
                "pj": json.dumps(spec.get("params")) if spec.get("params") else None,
                "en": 1 if spec.get("enabled") else 0,
                "uch": spec.get("update_cycle_hours"),
            })

    # Seed PARAMS
    for p in PARAM_DEFS:
        with conn.cursor() as cur:
            cur.execute("""
                MERGE INTO PARAMS tgt USING DUAL
                ON (tgt.PARAM_KEY = :pk)
                WHEN NOT MATCHED THEN INSERT (
                    PARAM_KEY, PARAM_VALUE, DTYPE, DESCRIPTION
                ) VALUES (
                    :pk, :pv, :dt, :desc_val
                )
            """, {
                "pk": p.key, "pv": p.default,
                "dt": p.dtype, "desc_val": p.description,
            })

    conn.commit()
    log.info("Seeded %d stations, %d sources, %d params",
             len(STATIONS), len(SOURCES), len(PARAM_DEFS))
    
    # Seed PARAMS
    for p in PARAM_DEFS:
        with conn.cursor() as cur:
            cur.execute("""
                MERGE INTO PARAMS tgt USING DUAL
                ON (tgt.PARAM_KEY = :pk)
                WHEN NOT MATCHED THEN INSERT (
                    PARAM_KEY, PARAM_VALUE, DTYPE, DESCRIPTION
                ) VALUES (
                    :pk, :pv, :dt, :desc
                )
            """, {
                "pk": p.key, "pv": p.default,
                "dt": p.dtype, "desc": p.description,
            })

    conn.commit()
    log.info("Seeded %d stations, %d sources, %d params",
             len(STATIONS), len(SOURCES), len(PARAM_DEFS))
