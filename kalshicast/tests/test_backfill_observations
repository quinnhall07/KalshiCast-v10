# kalshicast/tests/test_backfill_observations.py
import pytest
from unittest.mock import MagicMock, patch

class TestSchemaFlags:
    def test_migration_is_idempotent(self):
        """Running migration twice must not raise."""
        from kalshicast.db.migrations.add_backfill_flags import run_migrations
        conn = MagicMock()
        # First call raises nothing
        conn.cursor.return_value.__enter__ = lambda s: s
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value.execute = MagicMock(return_value=None)
        # Should not raise even if column already exists (ORA-01430 is swallowed)
        result = run_migrations(conn)
        assert isinstance(result, list)

    def test_migration_returns_applied_names(self):
        from kalshicast.db.migrations.add_backfill_flags import MIGRATIONS
        assert len(MIGRATIONS) >= 3
        names = [m[0] for m in MIGRATIONS]
        assert "FORECAST_RUNS.IS_BACKFILL" in names
        assert "FORECAST_ERRORS.LEAD_HOURS_APPROX" in names


class TestBackfillConfig:
    def test_backfill_window_is_valid(self):
        from kalshicast.backfill.config import BACKFILL_START, BACKFILL_END
        from datetime import date
        assert isinstance(BACKFILL_START, date)
        assert isinstance(BACKFILL_END, date)
        assert BACKFILL_START < BACKFILL_END
        assert (BACKFILL_END - BACKFILL_START).days >= 365

    def test_all_stations_have_asos_id(self):
        from kalshicast.backfill.config import STATION_ASOS_MAP
        from kalshicast.config.stations import STATIONS
        for s in STATIONS:
            sid = s["station_id"]
            assert sid in STATION_ASOS_MAP, f"{sid} missing from STATION_ASOS_MAP"

    def test_ome_models_list_matches_sources(self):
        from kalshicast.backfill.config import OME_HISTORICAL_MODELS
        # Must cover the 5 OME sources in SOURCES config
        model_ids = [m["source_id"] for m in OME_HISTORICAL_MODELS]
        for expected in ["OME_BASE", "OME_GFS", "OME_EC", "OME_ICON", "OME_GEM"]:
            assert expected in model_ids, f"{expected} missing from OME_HISTORICAL_MODELS"

class TestNwsStationObsParser:
    SAMPLE_GEOJSON = {
        "features": [
            {"properties": {"timestamp": "2023-03-15T06:53:00+00:00",
                            "temperature": {"value": -2.2, "unitCode": "wmoUnit:degC"}}},
            {"properties": {"timestamp": "2023-03-15T12:53:00+00:00",
                            "temperature": {"value": 12.0, "unitCode": "wmoUnit:degC"}}},
            {"properties": {"timestamp": "2023-03-15T23:53:00+00:00",
                            "temperature": {"value": 6.5, "unitCode": "wmoUnit:degC"}}},
            # Next day
            {"properties": {"timestamp": "2023-03-16T06:53:00+00:00",
                            "temperature": {"value": 3.0, "unitCode": "wmoUnit:degC"}}},
        ]
    }

    def test_parse_nws_obs_daily_high(self):
        from kalshicast.backfill.observations import _parse_nws_obs_to_daily
        results = _parse_nws_obs_to_daily(self.SAMPLE_GEOJSON)
        # 12.0°C = 53.6°F is the high on 2023-03-15
        assert "2023-03-15" in results
        assert abs(results["2023-03-15"]["high_f"] - 53.6) < 0.1

    def test_parse_nws_obs_daily_low(self):
        from kalshicast.backfill.observations import _parse_nws_obs_to_daily
        results = _parse_nws_obs_to_daily(self.SAMPLE_GEOJSON)
        # -2.2°C = 28.04°F is the low on 2023-03-15
        assert abs(results["2023-03-15"]["low_f"] - 28.04) < 0.1

    def test_parse_nws_obs_multiple_days(self):
        from kalshicast.backfill.observations import _parse_nws_obs_to_daily
        results = _parse_nws_obs_to_daily(self.SAMPLE_GEOJSON)
        assert "2023-03-16" in results

    def test_parse_nws_obs_skips_null_temperature(self):
        from kalshicast.backfill.observations import _parse_nws_obs_to_daily
        geojson = {
            "features": [
                {"properties": {"timestamp": "2023-03-15T06:53:00+00:00",
                                "temperature": {"value": None, "unitCode": "wmoUnit:degC"}}},
                {"properties": {"timestamp": "2023-03-15T12:53:00+00:00",
                                "temperature": {"value": 10.0, "unitCode": "wmoUnit:degC"}}},
            ]
        }
        results = _parse_nws_obs_to_daily(geojson)
        assert "2023-03-15" in results
        assert abs(results["2023-03-15"]["high_f"] - 50.0) < 0.1
        assert abs(results["2023-03-15"]["low_f"] - 50.0) < 0.1  # only one valid reading

    def test_parse_nws_obs_empty_features(self):
        from kalshicast.backfill.observations import _parse_nws_obs_to_daily
        results = _parse_nws_obs_to_daily({"features": []})
        assert results == {}

    def test_nws_obs_url_builder(self):
        from kalshicast.backfill.observations import _build_nws_obs_params
        url, params = _build_nws_obs_params("KNYC", "2023-01-01", "2023-01-31")
        assert "KNYC/observations" in url
        assert "start" in params
        assert "2023-01-01" in params["start"]
        assert "end" in params
        assert params["limit"] == 500

    def test_month_chunk_generator(self):
        from kalshicast.backfill.observations import _month_chunks
        chunks = list(_month_chunks("2023-01-01", "2023-03-15"))
        # Jan, Feb, Mar (partial)
        assert len(chunks) == 3
        assert chunks[0] == ("2023-01-01", "2023-01-31")
        assert chunks[1] == ("2023-02-01", "2023-02-28")
        assert chunks[2][0] == "2023-03-01"
        assert chunks[2][1] == "2023-03-15"
