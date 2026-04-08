"""
Tests for the XMCP Whale Watcher.

Governing plan:
  /home/nodeuser/documents/2026-04-08-bps-cadence-whale-watcher.md
"""
from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from blackpine_signals.db.models import DiscoveryCandidate
from blackpine_signals.ingestion.discovery_engine import DiscoveryEngine
from blackpine_signals.ingestion.whale_watcher import (
    WhaleWatcher,
    _classify_sentiment,
    _extract_tickers,
    _is_credits_depleted,
    _parse_posts_from_mcp,
)


# ---------------------------------------------------------------------------
# Pure-function unit tests
# ---------------------------------------------------------------------------

def test_classify_sentiment_bullish():
    assert _classify_sentiment("$NVDA breakout, set to surge tomorrow") == "bullish"


def test_classify_sentiment_bearish():
    assert _classify_sentiment("$TSLA misses earnings, will drop hard") == "bearish"


def test_classify_sentiment_neutral():
    assert _classify_sentiment("$NVDA reports tomorrow") == "neutral"


def test_extract_tickers_filters_blacklist():
    text = "Watching $NVDA, $PLTR, $IPO and $AI today"
    out = _extract_tickers(text)
    assert "NVDA" in out and "PLTR" in out
    assert "IPO" not in out and "AI" not in out


def test_extract_tickers_dedupes():
    assert _extract_tickers("$NVDA $NVDA $NVDA") == ["NVDA"]


def test_is_credits_depleted_detects_402_payload():
    payload = {
        "isError": True,
        "content": [
            {
                "type": "text",
                "text": (
                    "Error calling tool 'searchPostsRecent': "
                    "HTTP error 402: Payment Required - "
                    "{'title': 'CreditsDepleted'}"
                ),
            }
        ],
    }
    assert _is_credits_depleted(payload) is True


def test_is_credits_depleted_ignores_other_errors():
    payload = {
        "isError": True,
        "content": [{"type": "text", "text": "rate_limit_exceeded"}],
    }
    assert _is_credits_depleted(payload) is False


def test_is_credits_depleted_ignores_success():
    payload = {"isError": False, "content": [{"type": "text", "text": "[]"}]}
    assert _is_credits_depleted(payload) is False


def test_parse_posts_from_mcp_top_level_array():
    payload = {
        "isError": False,
        "content": [{"type": "text", "text": json.dumps([
            {"id": "1", "text": "hello $NVDA", "username": "alice"},
            {"id": "2", "text": "bye $TSLA", "username": "bob"},
        ])}],
    }
    posts = _parse_posts_from_mcp(payload)
    assert len(posts) == 2
    assert posts[0]["text"] == "hello $NVDA"


def test_parse_posts_from_mcp_data_envelope():
    payload = {
        "isError": False,
        "content": [{"type": "text", "text": json.dumps({
            "data": [{"id": "1", "text": "hi"}]
        })}],
    }
    posts = _parse_posts_from_mcp(payload)
    assert len(posts) == 1


def test_parse_posts_from_mcp_garbage_returns_empty():
    payload = {"isError": False, "content": [{"type": "text", "text": "not json"}]}
    assert _parse_posts_from_mcp(payload) == []


# ---------------------------------------------------------------------------
# WhaleWatcher.run() — integration with mocked httpx
# ---------------------------------------------------------------------------

def _make_watcher(db_session, accounts=None):
    return WhaleWatcher(
        gateway_url="http://test.local:8010",
        api_key="test_key",
        session_factory=lambda: db_session,
        discovery_engine_factory=lambda s: DiscoveryEngine(db_session=s),
        accounts=accounts or ["alice", "bob"],
    )


def _mock_response(payload: dict[str, Any], status_code: int = 200):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = payload
    resp.text = json.dumps(payload)
    return resp


class _MockClient:
    """Async context-manager wrapping a list of canned responses."""

    def __init__(self, responses):
        self._responses = list(responses)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, *args, **kwargs):
        return self._responses.pop(0)


def _patch_async_client(monkeypatch, client_factory):
    monkeypatch.setattr(
        "blackpine_signals.ingestion.whale_watcher.httpx.AsyncClient",
        lambda *a, **k: client_factory(),
    )


def test_whale_watcher_credits_depleted_short_circuits(db_session, monkeypatch):
    """Live 2026-04-08 fixture: gateway returns wrapped 402, run() must
    return status='credits_depleted' with empty summary and no exception."""
    payload = {
        "isError": True,
        "content": [
            {"type": "text", "text": "HTTP error 402: Payment Required CreditsDepleted"}
        ],
    }
    _patch_async_client(monkeypatch, lambda: _MockClient([_mock_response(payload)]))

    watcher = _make_watcher(db_session)
    out = asyncio.run(watcher.run())

    assert out["status"] == "credits_depleted"
    assert out["summary_text"] == ""
    assert out["posts_seen"] == 0
    assert out["error"] is None


def test_whale_watcher_happy_path(db_session, monkeypatch):
    posts = [
        {"id": "1", "text": "$NVDA breakout, going to surge", "username": "unusual_whales",
         "public_metrics": {"like_count": 1000, "retweet_count": 500}},
        {"id": "2", "text": "$PLTR rallying hard", "username": "unusual_whales",
         "public_metrics": {"like_count": 800, "retweet_count": 200}},
        {"id": "3", "text": "Quiet day", "username": "Polymarket",
         "public_metrics": {"like_count": 5, "retweet_count": 1}},
        {"id": "4", "text": "$TSLA bear flag forming", "username": "Polymarket",
         "public_metrics": {"like_count": 10, "retweet_count": 2}},
    ]
    payload = {
        "isError": False,
        "content": [{"type": "text", "text": json.dumps(posts)}],
    }
    _patch_async_client(monkeypatch, lambda: _MockClient([_mock_response(payload)]))

    watcher = _make_watcher(db_session)
    out = asyncio.run(watcher.run())

    assert out["status"] == "ok"
    assert out["posts_seen"] == 4
    assert out["notable_count"] >= 1
    assert "NVDA" in out["summary_text"] or "PLTR" in out["summary_text"]
    # At least one notable ticker should have been ingested
    assert out["tickers_ingested"] >= 1


def test_whale_watcher_no_data(db_session, monkeypatch):
    payload = {"isError": False, "content": [{"type": "text", "text": "[]"}]}
    _patch_async_client(monkeypatch, lambda: _MockClient([_mock_response(payload)]))

    watcher = _make_watcher(db_session)
    out = asyncio.run(watcher.run())
    assert out["status"] == "no_data"
    assert out["posts_seen"] == 0
    assert out["summary_text"] == ""


def test_whale_watcher_gateway_5xx_continues(db_session, monkeypatch):
    """Two batches: first 5xx, second OK. Should not raise; should report ok with batch 2 posts."""
    bad_payload = {"isError": False, "content": []}
    good_posts = [
        {"id": "1", "text": "$AVGO ripping", "username": "tom_doerr",
         "public_metrics": {"like_count": 200, "retweet_count": 50}},
    ]
    good_payload = {"isError": False, "content": [{"type": "text", "text": json.dumps(good_posts)}]}
    _patch_async_client(
        monkeypatch,
        lambda: _MockClient([
            _mock_response(bad_payload, status_code=503),
            _mock_response(good_payload),
        ]),
    )

    # Force two batches by passing more than 12 accounts
    accounts = [f"handle{i}" for i in range(14)]
    watcher = _make_watcher(db_session, accounts=accounts)
    out = asyncio.run(watcher.run())

    assert out["status"] == "ok"
    assert out["posts_seen"] == 1


def test_whale_watcher_connect_error_returns_error(db_session, monkeypatch):
    class _BoomClient:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        async def post(self, *a, **k):
            raise httpx.ConnectError("connection refused")

    _patch_async_client(monkeypatch, lambda: _BoomClient())

    watcher = _make_watcher(db_session)
    out = asyncio.run(watcher.run())
    assert out["status"] == "error"
    assert out["error"] is not None
    assert "connection" in out["error"].lower() or "http" in out["error"].lower()


def test_whale_watcher_account_batches_split():
    watcher = WhaleWatcher(
        gateway_url="http://x:8010",
        api_key="k",
        session_factory=lambda: None,
        discovery_engine_factory=lambda s: None,
        accounts=[f"a{i}" for i in range(25)],
    )
    batches = watcher._account_batches()
    assert len(batches) == 3  # 12 + 12 + 1
    assert sum(len(b) for b in batches) == 25
    assert all(len(b) <= 12 for b in batches)
