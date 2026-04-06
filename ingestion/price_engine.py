# /home/nodeuser/blackpine_core/blackpine_signals/ingestion/price_engine.py
"""
Layer 1: Price Engine — Alpha Vantage OHLCV polling.
Governing doc: /home/nodeuser/documents/2026-04-04-spec-028-black-pine-signals.md
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Any, Callable
import httpx
from sqlalchemy.orm import Session
from blackpine_signals.db.models import PriceSnapshot, WatchlistTicker

logger = logging.getLogger("bps.price_engine")
AV_BASE = "https://www.alphavantage.co/query"

def parse_intraday_response(data: dict[str, Any]) -> list[dict[str, Any]]:
    ts_key = "Time Series (15min)"
    if ts_key not in data:
        return []
    results = []
    for ts_str, ohlcv in data[ts_key].items():
        results.append({
            "timestamp": datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
            "open": float(ohlcv["1. open"]),
            "high": float(ohlcv["2. high"]),
            "low": float(ohlcv["3. low"]),
            "close": float(ohlcv["4. close"]),
            "volume": int(ohlcv["5. volume"]),
        })
    return results

class PriceEngine:
    def __init__(self, api_key: str, session_factory: Callable[[], Session]) -> None:
        self.api_key = api_key
        self.session_factory = session_factory

    async def _fetch_intraday(self, symbol: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(AV_BASE, params={
                "function": "TIME_SERIES_INTRADAY", "symbol": symbol,
                "interval": "15min", "apikey": self.api_key, "outputsize": "compact",
            })
            resp.raise_for_status()
            return resp.json()

    async def poll_ticker(self, symbol: str) -> int:
        data = await self._fetch_intraday(symbol)
        prices = parse_intraday_response(data)
        if not prices:
            logger.warning("No price data for %s", symbol)
            return 0
        session = self.session_factory()
        try:
            ticker = session.query(WatchlistTicker).filter_by(symbol=symbol, active=True).first()
            if not ticker:
                return 0
            inserted = 0
            for p in prices:
                exists = session.query(PriceSnapshot).filter_by(ticker_id=ticker.id, timestamp=p["timestamp"]).first()
                if not exists:
                    session.add(PriceSnapshot(ticker_id=ticker.id, **p))
                    inserted += 1
            session.commit()
            logger.info("Inserted %d price snapshots for %s", inserted, symbol)
            return inserted
        finally:
            session.close()

    async def poll_all(self) -> int:
        session = self.session_factory()
        try:
            symbols = [t.symbol for t in session.query(WatchlistTicker).filter_by(active=True).all()]
        finally:
            session.close()
        total = 0
        for symbol in symbols:
            try:
                total += await self.poll_ticker(symbol)
            except Exception:
                logger.exception("Failed to poll %s", symbol)
        return total
