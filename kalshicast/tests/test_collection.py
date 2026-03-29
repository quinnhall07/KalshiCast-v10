"""L1 Collection unit tests."""

import pytest
from kalshicast.collection.collectors.collect_metar import (
    _parse_temperature_f, _parse_dewpoint_f, _parse_wind,
)
from kalshicast.collection.collectors.collect_afd import _extract_signals


class TestMETARParsing:
    def test_standard_temp(self):
        raw = "KJFK 291456Z 18012G20KT 10SM SCT040 BKN250 22/14 A2998"
        assert abs(_parse_temperature_f(raw) - 71.6) < 0.1

    def test_negative_temp(self):
        raw = "KORD 291456Z 31015KT M02/M08 A3012"
        assert abs(_parse_temperature_f(raw) - 28.4) < 0.1

    def test_dewpoint(self):
        raw = "KJFK 291456Z 18012G20KT 10SM SCT040 BKN250 22/14 A2998"
        assert abs(_parse_dewpoint_f(raw) - 57.2) < 0.1

    def test_wind_speed_direction(self):
        raw = "KJFK 291456Z 18012G20KT 10SM SCT040 BKN250 22/14 A2998"
        ws, wd = _parse_wind(raw)
        assert ws == 12
        assert wd == 180

    def test_calm_wind(self):
        raw = "KJFK 291456Z 00000KT 10SM CLR 20/10 A3000"
        ws, wd = _parse_wind(raw)
        assert ws == 0
        assert wd == 0


class TestAFDSignals:
    def test_high_confidence(self):
        sig = _extract_signals("High confidence in the forecast with models in good agreement.")
        assert sig["confidence_flag"] == "HIGH"
        assert sig["model_disagreement_flag"] == 0
        assert sig["sigma_multiplier"] == 1.00

    def test_low_confidence_with_disagreement(self):
        sig = _extract_signals("This is a challenging forecast. Models disagree with large model spread.")
        assert sig["confidence_flag"] == "LOW"
        assert sig["model_disagreement_flag"] == 1
        assert sig["sigma_multiplier"] == 1.25

    def test_warm_bias(self):
        sig = _extract_signals("Temperatures will be warmer than normal this week.")
        assert sig["directional_note"] == "WARM_BIAS"

    def test_cold_bias(self):
        sig = _extract_signals("Expect cooler than average conditions through Friday.")
        assert sig["directional_note"] == "COOL_BIAS"

    def test_neutral(self):
        sig = _extract_signals("Partly cloudy skies expected tomorrow.")
        assert sig["confidence_flag"] == "NEUTRAL"
        assert sig["directional_note"] is None
