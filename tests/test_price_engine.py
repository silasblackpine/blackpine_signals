# /home/nodeuser/blackpine_core/blackpine_signals/tests/test_price_engine.py
"""Tests for price_engine.py"""
from unittest.mock import AsyncMock, patch
import pytest
from blackpine_signals.db.models import PriceSnapshot, WatchlistTicker
from blackpine_signals.ingestion.price_engine import parse_intraday_response, PriceEngine

def test_parse_intraday_response():
    raw = {"Time Series (15min)": {"2026-04-04 13:00:00": {"1. open": "177.50", "2. high": "178.20", "3. low": "177.00", "4. close": "177.80", "5. volume": "1234567"}}}
    result = parse_intraday_response(raw)
    assert len(result) == 1
    assert result[0]["open"] == 177.50
    assert result[0]["close"] == 177.80
    assert result[0]["volume"] == 1234567

def test_parse_intraday_empty():
    assert parse_intraday_response({}) == []
    assert parse_intraday_response({"Note": "rate limited"}) == []

@pytest.mark.asyncio
async def test_price_engine_poll_ticker(db_session):
    t = WatchlistTicker(symbol="NVDA", name="NVIDIA", tier="mega_cap", sector="Technology")
    db_session.add(t)
    db_session.commit()
    mock_response = {"Time Series (15min)": {"2026-04-04 13:00:00": {"1. open": "177.50", "2. high": "178.20", "3. low": "177.00", "4. close": "177.80", "5. volume": "1234567"}}}
    engine = PriceEngine(api_key="test_key", session_factory=lambda: db_session)
    with patch.object(engine, "_fetch_intraday", new_callable=AsyncMock, return_value=mock_response):
        count = await engine.poll_ticker("NVDA")
    assert count == 1
    snap = db_session.query(PriceSnapshot).first()
    assert float(snap.close) == 177.80
