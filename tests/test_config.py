"""Tests for config.py — validates Settings loads and has correct defaults."""

from blackpine_signals.config import Settings


def test_settings_defaults():
    s = Settings(
        _env_file=None,
        database_url="sqlite:///:memory:",
    )
    assert s.price_poll_interval_minutes == 15
    assert s.edgar_poll_interval_seconds == 60
    assert s.polymarket_odds_threshold == 5.0
    assert s.signal_critical_threshold == 80.0
    assert len(s.edgar_targets) == 10
    assert "Anthropic" in s.edgar_targets


def test_settings_override():
    s = Settings(
        _env_file=None,
        database_url="sqlite:///:memory:",
        price_poll_interval_minutes=5,
        signal_critical_threshold=90.0,
    )
    assert s.price_poll_interval_minutes == 5
    assert s.signal_critical_threshold == 90.0
