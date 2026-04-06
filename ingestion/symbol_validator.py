"""
SPEC-028B Symbol Validator — AlphaVantage SYMBOL_SEARCH wrapper.

Governing docs:
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 5
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §5

Why this exists
---------------
The Discovery Engine's X Watchdog path extracts `$TICKER` mentions via
regex, which happily captures garbage like `$WAGMI`, `$LFG`, `$GM`, and
random 1-5 letter words. Without validation these accumulate score
toward the auto-add threshold (80) and pollute the discovery pipeline.

This module exposes a tiny `SymbolValidator` that calls AlphaVantage's
`SYMBOL_SEARCH` endpoint and returns True iff the symbol resolves to a
real equity / ETF / mutual fund with a non-zero match score.

Design notes
------------
- Direct HTTP via `httpx.AsyncClient` (matches Phase 1 Price Engine
  convention). We deliberately do NOT route through the AlphaVantage
  MCP server: MCP is for the Claude Code agent, not the backend
  service. See Phase 2 plan §4 Task 5 architectural correction.
- Per-process in-memory LRU cache on top of the HTTP call, since the
  same tickers get validated many times per day and AlphaVantage free
  tier allows only 25 calls/day. Cache TTL is 24h.
- Graceful fallback: if the API key is missing, `validate()` returns
  `None` (unknown) rather than raising, so Discovery Engine callers
  can treat missing-key as "skip validation".
- Never stores or logs the API key. Never mutates `discovery_candidates`
  rows directly — Discovery Engine owns that write path.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Callable, Optional

import httpx

logger = logging.getLogger("bps.symbol_validator")

AV_BASE = "https://www.alphavantage.co/query"
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24h
# Accept equities, ETFs, mutual funds with a non-trivial name match score.
# AlphaVantage returns matchScore as a string like "1.0000" / "0.6666".
MIN_MATCH_SCORE = 0.5
ACCEPTED_TYPES = frozenset({"Equity", "ETF", "Mutual Fund"})


class SymbolValidator:
    """Validates that a `$TICKER` mention refers to a real exchange-listed symbol.

    Usage::

        validator = SymbolValidator()
        is_real = await validator.validate("NVDA")   # True
        is_real = await validator.validate("WAGMI")  # False
        is_real = await validator.validate("PLTR")   # True

    When the AlphaVantage API key is not configured, `validate()`
    returns `None` (meaning "unknown"); callers should treat that as
    "skip validation, accept the mention".
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: float = 10.0,
        http_get: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.api_key = (
            api_key if api_key is not None else os.getenv("ALPHA_VANTAGE_API_KEY", "")
        )
        self.timeout = timeout
        # Injectable for tests: an async callable with the same signature
        # as httpx.AsyncClient().get. If None, a real httpx client is used.
        self._http_get = http_get
        # Cache: ticker -> (is_real, expires_at_epoch)
        self._cache: dict[str, tuple[bool, float]] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def validate(self, ticker: str) -> Optional[bool]:
        """Return True if `ticker` is a real tradable symbol, False if not,
        or None if we couldn't determine (missing API key, network error).

        Cached results live 24h. Never raises; all errors return None."""
        if not ticker or not isinstance(ticker, str):
            return False
        ticker = ticker.upper().strip()
        if not self.api_key:
            logger.debug("symbol_validator: no API key, skipping validation for %s", ticker)
            return None

        cached = self._cache_get(ticker)
        if cached is not None:
            return cached

        try:
            data = await self._fetch_symbol_search(ticker)
        except Exception as exc:  # noqa: BLE001
            logger.warning("symbol_validator: fetch failed for %s: %s", ticker, exc)
            return None

        is_real = self._interpret_response(data, ticker)
        async with self._lock:
            self._cache[ticker] = (is_real, time.time() + CACHE_TTL_SECONDS)
        return is_real

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _cache_get(self, ticker: str) -> Optional[bool]:
        hit = self._cache.get(ticker)
        if hit is None:
            return None
        value, expires_at = hit
        if time.time() >= expires_at:
            # Expired — drop it
            self._cache.pop(ticker, None)
            return None
        return value

    async def _fetch_symbol_search(self, ticker: str) -> dict[str, Any]:
        params = {
            "function": "SYMBOL_SEARCH",
            "keywords": ticker,
            "apikey": self.api_key,
        }
        if self._http_get is not None:
            return await self._http_get(AV_BASE, params=params, timeout=self.timeout)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(AV_BASE, params=params)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    def _interpret_response(data: dict[str, Any], ticker: str) -> bool:
        """Return True iff the response contains a bestMatches entry whose
        `1. symbol` equals the input ticker (case-insensitive), whose
        `3. type` is an accepted instrument type, and whose
        `9. matchScore` >= MIN_MATCH_SCORE."""
        matches = data.get("bestMatches") or []
        if not matches:
            return False
        for m in matches:
            symbol = (m.get("1. symbol") or "").upper().strip()
            instrument_type = (m.get("3. type") or "").strip()
            try:
                score = float(m.get("9. matchScore") or "0")
            except ValueError:
                score = 0.0
            if (
                symbol == ticker.upper().strip()
                and instrument_type in ACCEPTED_TYPES
                and score >= MIN_MATCH_SCORE
            ):
                return True
        return False
