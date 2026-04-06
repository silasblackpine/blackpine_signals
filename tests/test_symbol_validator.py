"""
Tests for SPEC-028B SymbolValidator (Task 5).

Governing docs:
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 5
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §5

Covers:
- Real ticker returns True
- Garbage ticker returns False
- Missing API key returns None (unknown)
- Empty / non-string input returns False
- Case normalization
- Response type filtering (only Equity/ETF/Mutual Fund)
- Match-score threshold
- Cache hit / miss / expiry
- Network error returns None (doesn't raise)
"""
from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock

import pytest

from blackpine_signals.ingestion.symbol_validator import (
    ACCEPTED_TYPES,
    MIN_MATCH_SCORE,
    SymbolValidator,
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _make_match(symbol: str, type_: str = "Equity", score: str = "1.0000") -> dict:
    return {
        "1. symbol": symbol,
        "2. name": f"{symbol} Corp",
        "3. type": type_,
        "4. region": "United States",
        "9. matchScore": score,
    }


def _response(*matches) -> dict[str, Any]:
    return {"bestMatches": list(matches)}


def make_fake_http(response_map: dict[str, Any]):
    """Return an async callable matching httpx.AsyncClient.get signature."""
    async def _fake(url, params=None, timeout=None):
        keyword = (params or {}).get("keywords", "")
        return response_map.get(keyword, {"bestMatches": []})
    return _fake


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


# ---------------------------------------------------------------------------
# Input handling
# ---------------------------------------------------------------------------

def test_missing_api_key_returns_none():
    v = SymbolValidator(api_key="")
    assert run(v.validate("NVDA")) is None


def test_empty_input_returns_false():
    v = SymbolValidator(api_key="demo")
    assert run(v.validate("")) is False
    assert run(v.validate(None)) is False  # type: ignore[arg-type]


def test_validate_normalizes_case_and_whitespace():
    fake = make_fake_http({"NVDA": _response(_make_match("NVDA"))})
    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate(" nvda ")) is True


# ---------------------------------------------------------------------------
# Real tickers
# ---------------------------------------------------------------------------

def test_real_equity_returns_true():
    fake = make_fake_http({
        "NVDA": _response(_make_match("NVDA", type_="Equity", score="1.0000"))
    })
    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate("NVDA")) is True


def test_real_etf_returns_true():
    fake = make_fake_http({
        "SPY": _response(_make_match("SPY", type_="ETF", score="0.9000"))
    })
    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate("SPY")) is True


def test_mutual_fund_returns_true():
    fake = make_fake_http({
        "VFIAX": _response(_make_match("VFIAX", type_="Mutual Fund", score="0.8000"))
    })
    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate("VFIAX")) is True


# ---------------------------------------------------------------------------
# Rejection cases
# ---------------------------------------------------------------------------

def test_garbage_ticker_no_matches_returns_false():
    fake = make_fake_http({"WAGMI": _response()})  # empty bestMatches
    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate("WAGMI")) is False


def test_unrelated_match_returns_false():
    """AlphaVantage can return fuzzy matches; we require exact symbol match."""
    fake = make_fake_http({
        "LFG": _response(_make_match("LFGVX", type_="Mutual Fund", score="1.0000"))
    })
    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate("LFG")) is False


def test_low_match_score_returns_false():
    fake = make_fake_http({
        "XYZ": _response(_make_match("XYZ", type_="Equity", score="0.2000"))
    })
    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate("XYZ")) is False


def test_rejected_instrument_type_returns_false():
    """Currencies, physical currencies etc should not count."""
    fake = make_fake_http({
        "BTC": _response(_make_match("BTC", type_="Physical Currency", score="1.0000"))
    })
    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate("BTC")) is False


def test_invalid_score_string_treated_as_zero():
    fake = make_fake_http({
        "OOPS": _response(_make_match("OOPS", type_="Equity", score="not-a-number"))
    })
    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate("OOPS")) is False


# ---------------------------------------------------------------------------
# Cache behavior
# ---------------------------------------------------------------------------

def test_cache_hit_avoids_second_http_call():
    call_count = {"n": 0}
    async def fake(url, params=None, timeout=None):
        call_count["n"] += 1
        return _response(_make_match("NVDA", type_="Equity", score="1.0000"))

    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate("NVDA")) is True
    assert run(v.validate("NVDA")) is True
    assert run(v.validate("NVDA")) is True
    assert call_count["n"] == 1


def test_cache_expiry_triggers_refetch(monkeypatch):
    call_count = {"n": 0}
    async def fake(url, params=None, timeout=None):
        call_count["n"] += 1
        return _response(_make_match("NVDA", type_="Equity", score="1.0000"))

    v = SymbolValidator(api_key="demo", http_get=fake)
    assert run(v.validate("NVDA")) is True
    # Force cache expiry by rewinding stored expiry
    v._cache["NVDA"] = (True, time.time() - 1)
    assert run(v.validate("NVDA")) is True
    assert call_count["n"] == 2


# ---------------------------------------------------------------------------
# Network / error handling
# ---------------------------------------------------------------------------

def test_http_exception_returns_none_not_raises():
    async def boom(url, params=None, timeout=None):
        raise RuntimeError("network down")

    v = SymbolValidator(api_key="demo", http_get=boom)
    assert run(v.validate("NVDA")) is None


def test_malformed_response_returns_false():
    async def fake(url, params=None, timeout=None):
        return {"Note": "API call frequency exceeded", "Information": "..."}

    v = SymbolValidator(api_key="demo", http_get=fake)
    # No bestMatches key → False, not None
    assert run(v.validate("NVDA")) is False
