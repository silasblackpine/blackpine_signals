"""
Integration tests for SPEC-028B Task 3 — scanner-to-discovery wiring.

Governing docs:
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §2, §3
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 3

Covers the additive discovery hooks bolted onto the three Phase 1 scanners:
- X Watchdog: unknown $TICKER mentions flow to discovery
- EDGAR Scanner: AI-adjacent non-target filings flow to discovery (and S-1/F-1
  auto-seeds the IPO pipeline)
- Polymarket Engine: new markets matching AI keywords seed discovery candidates

All external APIs are mocked. In-memory SQLite via conftest db_session.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from blackpine_signals.db.models import (
    Base,
    DiscoveryCandidate,
    EdgarFiling,
    IPOPipeline,
    WatchlistTicker,
    XSignal,
)
from blackpine_signals.ingestion.discovery_engine import DiscoveryEngine
from blackpine_signals.ingestion.edgar_scanner import EdgarScanner
from blackpine_signals.ingestion.polymarket_engine import PolymarketEngine
from blackpine_signals.ingestion.x_watchdog import XWatchdog


# ---------------------------------------------------------------------------
# Shared engine + session factory fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def engine_session_factory():
    """Single shared in-memory SQLite engine with a bound session factory."""
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    Session = sessionmaker(bind=eng, expire_on_commit=False)
    yield eng, Session
    eng.dispose()


@pytest.fixture
def seeded_watchlist(engine_session_factory):
    eng, Session = engine_session_factory
    s = Session()
    s.add(WatchlistTicker(symbol="NVDA", name="NVIDIA", tier="mega_cap", sector="semis"))
    s.add(WatchlistTicker(symbol="AVGO", name="Broadcom", tier="mega_cap", sector="semis"))
    s.commit()
    s.close()
    return engine_session_factory


def make_discovery_factory(mock_post=None):
    """Build a factory that instantiates DiscoveryEngine with a stubbed webhook."""
    mock_post = mock_post or MagicMock()
    def factory(session):
        return DiscoveryEngine(
            db_session=session,
            webhook_url="",  # no alerts during integration tests
            http_post=mock_post,
        )
    return factory


# ---------------------------------------------------------------------------
# X Watchdog
# ---------------------------------------------------------------------------

def test_xwatchdog_unknown_tickers_flow_to_discovery(seeded_watchlist):
    eng, Session = seeded_watchlist

    # Two posts: one mentions known NVDA + unknown PLTR; one mentions unknown SNOW
    fake_grok_resp = {
        "choices": [
            {
                "message": {
                    "content": (
                        '[{"handle": "EricNewcomer", "text": "$NVDA and $PLTR ripping today",'
                        ' "likes": 5000, "reposts": 200, "url": "https://x.com/e/1", "posted_at": "2026-04-06T10:00:00+00:00"},'
                        '{"handle": "NirantK", "text": "Big move on $SNOW",'
                        ' "likes": 100, "reposts": 10, "url": "https://x.com/n/2", "posted_at": "2026-04-06T11:00:00+00:00"}]'
                    )
                }
            }
        ]
    }

    watchdog = XWatchdog(
        api_key="test-key",
        session_factory=Session,
        discovery_engine_factory=make_discovery_factory(),
    )

    with patch.object(XWatchdog, "_query_grok", new=AsyncMock(return_value=fake_grok_resp)):
        inserted = asyncio.run(watchdog.scan())

    assert inserted == 2

    s = Session()
    # X signals stored
    assert s.query(XSignal).count() == 2
    # Discovery candidates: PLTR (Tier1=20), SNOW (Tier2=10). NVDA is known → not in discovery.
    cands = {c.entity_name: c for c in s.query(DiscoveryCandidate).all()}
    assert "PLTR" in cands
    assert "SNOW" in cands
    assert "NVDA" not in cands
    assert cands["PLTR"].discovery_score == 20.0  # Tier 1
    assert cands["SNOW"].discovery_score == 10.0  # Tier 2
    assert cands["PLTR"].entity_type == "ticker"
    s.close()


def test_xwatchdog_without_discovery_factory_is_noop(seeded_watchlist):
    """Scanner must keep working when no discovery hook is wired."""
    eng, Session = seeded_watchlist
    fake = {
        "choices": [
            {
                "message": {
                    "content": (
                        '[{"handle": "EricNewcomer", "text": "$PLTR move",'
                        ' "likes": 1, "reposts": 1, "url": "https://x.com/e/99",'
                        ' "posted_at": "2026-04-06T10:00:00+00:00"}]'
                    )
                }
            }
        ]
    }
    watchdog = XWatchdog(api_key="k", session_factory=Session)  # no discovery factory
    with patch.object(XWatchdog, "_query_grok", new=AsyncMock(return_value=fake)):
        asyncio.run(watchdog.scan())
    s = Session()
    assert s.query(XSignal).count() == 1
    assert s.query(DiscoveryCandidate).count() == 0
    s.close()


# ---------------------------------------------------------------------------
# EDGAR Scanner
# ---------------------------------------------------------------------------

class _FakeFeed:
    def __init__(self, entries):
        self.entries = entries


def _edgar_entry(form, entity, url, summary):
    return {
        "title": f"{form} - {entity} (0001234567) (Filer)",
        "link": url,
        "summary": summary,
        "updated": "2026-04-06T12:00:00-04:00",
    }


def test_edgar_ai_adjacent_non_target_creates_discovery_and_ipo(engine_session_factory):
    eng, Session = engine_session_factory
    entries = [
        # Target S-1 — existing Phase 1 behavior, NOT a discovery
        _edgar_entry("S-1", "Anthropic PBC", "https://sec.gov/1", "Filing for IPO"),
        # Non-target AI-adjacent S-1 — SHOULD become a discovery AND seed IPO pipeline
        _edgar_entry(
            "S-1",
            "Quantum LLM Corp",
            "https://sec.gov/2",
            "Company building large language model infrastructure for enterprise",
        ),
        # Non-target NON-AI S-1 — should NOT become a discovery
        _edgar_entry("S-1", "Pizza Chain Inc", "https://sec.gov/3", "Nationwide pizza franchise"),
    ]

    scanner = EdgarScanner(
        session_factory=Session,
        discovery_engine_factory=make_discovery_factory(),
    )
    with patch(
        "blackpine_signals.ingestion.edgar_scanner.feedparser.parse",
        return_value=_FakeFeed(entries),
    ):
        inserted = asyncio.run(scanner.scan())
    assert inserted == 3

    s = Session()
    # All three filings stored
    assert s.query(EdgarFiling).count() == 3
    # Discovery candidates: only Quantum LLM Corp
    cand_names = {c.entity_name for c in s.query(DiscoveryCandidate).all()}
    assert cand_names == {"Quantum LLM Corp"}
    # IPO pipeline: Anthropic PBC (target path) + Quantum LLM Corp (discovery path)
    ipo_names = {e.company_name for e in s.query(IPOPipeline).all()}
    assert "Anthropic PBC" in ipo_names
    assert "Quantum LLM Corp" in ipo_names
    s.close()


def test_edgar_no_discovery_factory_preserves_phase1_behavior(engine_session_factory):
    eng, Session = engine_session_factory
    entries = [
        _edgar_entry(
            "S-1", "Neural Dynamics", "https://sec.gov/9",
            "Deep learning research platform"
        ),
    ]
    scanner = EdgarScanner(session_factory=Session)  # no discovery
    with patch(
        "blackpine_signals.ingestion.edgar_scanner.feedparser.parse",
        return_value=_FakeFeed(entries),
    ):
        asyncio.run(scanner.scan())
    s = Session()
    assert s.query(EdgarFiling).count() == 1
    assert s.query(DiscoveryCandidate).count() == 0  # no discovery hook
    s.close()


# ---------------------------------------------------------------------------
# Polymarket Engine
# ---------------------------------------------------------------------------

class _FakePoller:
    def __init__(self, events_by_kw):
        self.events_by_kw = events_by_kw

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def search_events(self, keyword, limit=10):
        return self.events_by_kw.get(keyword, [])


def test_polymarket_discover_new_markets_creates_candidates(engine_session_factory):
    eng, Session = engine_session_factory

    # Pre-seed a watchlist ticker so its matching event gets filtered out.
    s = Session()
    s.add(WatchlistTicker(symbol="NVDA", name="NVIDIA", tier="mega_cap", sector="semis"))
    s.commit()
    s.close()

    events_by_kw = {
        "AI": [
            {"id": "m1", "title": "Will Databricks IPO in 2026?"},
            {"id": "m2", "title": "Will NVIDIA split stock in 2026?"},  # NVIDIA — not in known set by title; still a candidate
        ],
        "OpenAI": [
            {"id": "m3", "title": "Will OpenAI reach AGI by 2027?"},
            # Duplicate: same title as above — should be deduped
            {"id": "m4", "title": "Will Databricks IPO in 2026?"},
        ],
    }
    fake_poller = _FakePoller(events_by_kw)

    pm = PolymarketEngine(
        session_factory=Session,
        discovery_engine_factory=make_discovery_factory(),
    )
    # Replace _get_poller with an async func returning our fake
    async def _get():
        return fake_poller
    pm._get_poller = _get  # type: ignore[assignment]

    created = asyncio.run(pm.discover_new_markets(keywords=["AI", "OpenAI"]))
    assert created == 3  # 3 unique titles, one dedup

    s = Session()
    cand_names = {c.entity_name for c in s.query(DiscoveryCandidate).all()}
    assert "Will Databricks IPO in 2026?" in cand_names
    assert "Will OpenAI reach AGI by 2027?" in cand_names
    assert "Will NVIDIA split stock in 2026?" in cand_names
    s.close()


def test_polymarket_discover_no_factory_returns_zero(engine_session_factory):
    eng, Session = engine_session_factory
    pm = PolymarketEngine(session_factory=Session)  # no discovery
    created = asyncio.run(pm.discover_new_markets(keywords=["AI"]))
    assert created == 0
