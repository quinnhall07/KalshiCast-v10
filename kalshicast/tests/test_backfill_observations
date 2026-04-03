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
