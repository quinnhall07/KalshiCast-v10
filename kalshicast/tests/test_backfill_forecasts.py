# kalshicast/tests/test_backfill_forecasts.py
import pytest
from unittest.mock import patch, MagicMock

class TestOmeHistoricalAdapter:
    MOCK_OME_RESPONSE = {
        "daily": {
            "time": ["2023-03-15", "2023-03-16"],
            "temperature_2m_max": [58.0, 62.1],
            "temperature_2m_min": [38.5, 41.0],
        },
        "hourly": {
            "time": [f"2023-03-15T{h:02d}:00" for h in range(24)] +
                    [f"2023-03-16T{h:02d}:00" for h in range(24)],
            "temperature_2m": [45.0] * 48,
            "dew_point_2m": [35.0] * 48,
            "relative_humidity_2m": [70.0] * 48,
            "wind_speed_10m": [10.0] * 48,
            "wind_direction_10m": [180.0] * 48,
            "cloud_cover": [50.0] * 48,
            "precipitation_probability": [20.0] * 48,
        },
    }

    def test_parse_ome_historical_response_daily(self):
        from kalshicast.backfill.forecasts import _parse_ome_historical_response
        daily, hourly = _parse_ome_historical_response(self.MOCK_OME_RESPONSE)
        assert len(daily) == 2
        assert daily[0]["target_date"] == "2023-03-15"
        assert abs(daily[0]["high_f"] - 58.0) < 0.01
        assert abs(daily[0]["low_f"] - 38.5) < 0.01

    def test_parse_ome_historical_response_hourly_count(self):
        from kalshicast.backfill.forecasts import _parse_ome_historical_response
        _, hourly = _parse_ome_historical_response(self.MOCK_OME_RESPONSE)
        assert len(hourly) == 48

    def test_parse_ome_historical_handles_none_temps(self):
        from kalshicast.backfill.forecasts import _parse_ome_historical_response
        resp = dict(self.MOCK_OME_RESPONSE)
        resp["daily"] = {
            "time": ["2023-03-15"],
            "temperature_2m_max": [None],
            "temperature_2m_min": [38.5],
        }
        resp["hourly"] = {"time": [], "temperature_2m": []}
        daily, _ = _parse_ome_historical_response(resp)
        # Row with None high_f should be skipped
        assert len(daily) == 0

    def test_lead_offset_issued_at_computation(self):
        from kalshicast.backfill.forecasts import _issued_at_for_offset
        from datetime import date
        target = date(2023, 3, 15)
        issued = _issued_at_for_offset(target, offset_days=2)
        assert issued == "2023-03-13T12:00:00Z"

    def test_lead_hours_computation(self):
        from kalshicast.backfill.forecasts import _approx_lead_hours
        # 2-day offset at noon UTC to target at 15:00 local = roughly 51 hours
        h = _approx_lead_hours(offset_days=2, target_local_hour=15)
        assert 47 < h < 53   # h3 bracket