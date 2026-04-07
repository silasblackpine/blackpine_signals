"""
Tests for SPEC-028B Task 4 — Discovery Research Runner.

Governing docs:
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §2 Source 2, §6
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 4
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from blackpine_signals.db.models import DiscoveryCandidate, DiscoveryLog, WatchlistTicker
from blackpine_signals.ingestion.discovery_engine import DiscoveryEngine
from blackpine_signals.ingestion.discovery_research import (
    COMPANY_STOPWORDS,
    DISCOVERY_TOPICS,
    DiscoveryResearchRunner,
    TOPICS_PER_DAY,
    extract_ipo_company_mentions,
    extract_tickers_from_text,
    strip_markdown_headers,
    topics_for_day,
)


# ---------------------------------------------------------------------------
# Topic rotation
# ---------------------------------------------------------------------------

def test_topics_for_day_returns_correct_count():
    topics = topics_for_day(datetime(2026, 4, 7, tzinfo=timezone.utc))
    assert len(topics) == TOPICS_PER_DAY


def test_topics_for_day_deterministic():
    """Same date → same topics across calls."""
    d = datetime(2026, 4, 7, tzinfo=timezone.utc)
    assert topics_for_day(d) == topics_for_day(d)


def test_topics_for_day_rotation_covers_pool():
    """Two consecutive days must cover the entire 8-topic pool."""
    d1 = datetime(2026, 4, 6, tzinfo=timezone.utc)
    d2 = datetime(2026, 4, 7, tzinfo=timezone.utc)
    combined = set(topics_for_day(d1)) | set(topics_for_day(d2))
    assert combined == set(DISCOVERY_TOPICS)


def test_topics_for_day_no_overlap_consecutive_days():
    d1 = datetime(2026, 4, 6, tzinfo=timezone.utc)
    d2 = datetime(2026, 4, 7, tzinfo=timezone.utc)
    assert set(topics_for_day(d1)).isdisjoint(set(topics_for_day(d2)))


# ---------------------------------------------------------------------------
# Entity extraction
# ---------------------------------------------------------------------------

def test_extract_tickers_basic():
    text = "Bullish on $NVDA and $AVGO. Watching $PLTR too."
    assert extract_tickers_from_text(text) == {"NVDA", "AVGO", "PLTR"}


def test_extract_tickers_empty():
    assert extract_tickers_from_text("") == set()
    assert extract_tickers_from_text(None or "") == set()  # None guard


def test_extract_tickers_deduplicates():
    text = "$NVDA $NVDA $NVDA $NVDA"
    assert extract_tickers_from_text(text) == {"NVDA"}


def test_extract_ipo_companies_in_context():
    text = (
        "Anthropic PBC filed for S-1 today. CoreWeave is going public next quarter."
    )
    companies = extract_ipo_company_mentions(text)
    # Both should be detected because they're near IPO context keywords
    assert any("Anthropic" in c for c in companies)
    assert any("CoreWeave" in c for c in companies)


def test_extract_ipo_companies_rejects_stopwords():
    text = "Wall Street reacted to the IPO. The Federal Reserve weighed in."
    companies = extract_ipo_company_mentions(text)
    assert "Wall Street" not in companies
    assert "Federal Reserve" not in companies


def test_extract_ipo_companies_rejects_no_context():
    """Company names without IPO context should NOT be flagged."""
    text = "Databricks is a data platform. Stripe processes payments."
    companies = extract_ipo_company_mentions(text)
    assert len(companies) == 0


def test_company_stopwords_contains_expected():
    assert "Wall Street" in COMPANY_STOPWORDS
    assert "United States" in COMPANY_STOPWORDS
    assert "Hacker News" in COMPANY_STOPWORDS


def test_company_stopwords_includes_markdown_header_noise():
    """2026-04-07 ZHGP run regression — these polluted the DB."""
    for label in (
        "Score", "Engagement", "Relevance", "Reports", "Date",
        "Highlights", "Findings", "Summary", "Techmeme", "Bloomberg",
        "Reuters",
    ):
        assert label in COMPANY_STOPWORDS, f"{label!r} missing from COMPANY_STOPWORDS"


# ---------------------------------------------------------------------------
# strip_markdown_headers — the 2026-04-07 data-cleanliness fix
# ---------------------------------------------------------------------------

def test_strip_markdown_headers_removes_atx_headers():
    text = (
        "# Top-level\n"
        "## Subsection\n"
        "### Sub-sub\n"
        "Actual prose line.\n"
    )
    assert strip_markdown_headers(text) == "Actual prose line."


def test_strip_markdown_headers_removes_table_separator():
    text = (
        "| col1 | col2 |\n"
        "| --- | --- |\n"
        "| a | b |\n"
    )
    cleaned = strip_markdown_headers(text)
    assert "---" not in cleaned
    assert "| col1 | col2 |" in cleaned
    assert "| a | b |" in cleaned


def test_strip_markdown_headers_removes_horizontal_rule():
    assert strip_markdown_headers("prose\n---\nmore prose") == "prose\nmore prose"


def test_strip_markdown_headers_preserves_empty_string():
    assert strip_markdown_headers("") == ""
    assert strip_markdown_headers(None or "") == ""


def test_strip_markdown_headers_preserves_prose_with_hash_inside():
    """A `#` in the middle of a line is not a header — must NOT be stripped."""
    text = "The stock rose #1 today and #2 tomorrow."
    assert strip_markdown_headers(text) == text


# ---------------------------------------------------------------------------
# extract_ipo_company_mentions — regression against live-run noise
# ---------------------------------------------------------------------------

def test_extract_ipo_companies_rejects_markdown_section_labels():
    """The exact noise pattern observed in the 2026-04-07 live trigger run:
    section headers near IPO-context keywords must not be treated as companies.
    """
    text = (
        "# Research: AI infrastructure companies\n"
        "\n"
        "## Score\n"
        "## Engagement\n"
        "## Relevance\n"
        "\n"
        "Databricks filed for S-1 this week in a blockbuster deal.\n"
        "CoreWeave is going public next month.\n"
        "\n"
        "## Reports\n"
        "## Date\n"
        "## Highlights Crusoe AI\n"
    )
    companies = extract_ipo_company_mentions(text)
    # Real companies should still be caught
    assert any("Databricks" in c for c in companies)
    assert any("CoreWeave" in c for c in companies)
    # Header labels must NOT be caught
    for noise in ("Score", "Engagement", "Relevance", "Reports", "Date", "Highlights"):
        assert noise not in companies, f"{noise!r} leaked through"
    # The "Highlights Crusoe AI" multi-word match must be rejected because
    # its first token is a stop-word — even though "Crusoe" is real.
    assert "Highlights Crusoe AI" not in companies


def test_extract_ipo_companies_rejects_news_outlets():
    text = "Techmeme reports Databricks filed for IPO. Bloomberg broke the story first."
    companies = extract_ipo_company_mentions(text)
    assert "Techmeme" not in companies
    assert "Bloomberg" not in companies
    assert any("Databricks" in c for c in companies)


# ---------------------------------------------------------------------------
# DiscoveryResearchRunner — end to end
# ---------------------------------------------------------------------------

FAKE_MD_OUTPUT = """
# Research: new AI stocks 2026

## Findings

- $NVDA reported record earnings and investors are bullish
- $PLTR and $SOUN are the hottest small caps
- Databricks filed for S-1 this week in a blockbuster deal

CoreWeave is going public next month — GS and JPM are lead bankers.

Polymarket odds on a $GROQ IPO are climbing.
"""


def test_research_runner_end_to_end(db_session, monkeypatch):
    """One happy-path research cycle ingests tickers + companies into discovery."""
    # Seed a watchlist ticker so we can prove it's excluded from discovery handoff
    db_session.add(WatchlistTicker(symbol="NVDA", name="NVIDIA", tier="mega_cap", sector="semiconductors"))
    db_session.commit()

    def session_factory():
        return db_session

    def engine_factory(session):
        return DiscoveryEngine(db_session=session, webhook_url=None)

    runner = DiscoveryResearchRunner(
        session_factory=session_factory,
        discovery_engine_factory=engine_factory,
    )

    async def fake_run_last30days(topic, **kwargs):
        return FAKE_MD_OUTPUT

    monkeypatch.setattr(
        "blackpine_signals.ingestion.discovery_research.run_last30days",
        fake_run_last30days,
    )

    summary = asyncio.run(runner.run_topic("new AI stocks 2026"))

    assert summary["status"] == "ok"
    assert summary["error"] is None
    # NVDA is on the watchlist → excluded. PLTR, SOUN, GROQ are new → 3.
    assert summary["tickers_fed"] == 3
    # Databricks (S-1 filed) + CoreWeave (going public) → at least 2.
    assert summary["companies_fed"] >= 2

    # Candidates should exist in DB
    pltr = db_session.query(DiscoveryCandidate).filter_by(entity_name="PLTR").first()
    assert pltr is not None
    assert pltr.discovery_score > 0
    assert pltr.entity_type == "ticker"

    # NVDA must NOT have a candidate (it was on watchlist, filtered out)
    nvda = db_session.query(DiscoveryCandidate).filter_by(entity_name="NVDA").first()
    assert nvda is None

    # A research_completed log row should exist
    log = (
        db_session.query(DiscoveryLog)
        .filter_by(entity_name="research:new AI stocks 2026", action="research_completed")
        .first()
    )
    assert log is not None
    assert log.details["tickers_fed"] == 3


def test_research_runner_handles_subprocess_failure(db_session, monkeypatch):
    """Subprocess error → research_failed log row, no exception bubbles."""
    def session_factory():
        return db_session

    def engine_factory(session):
        return DiscoveryEngine(db_session=session, webhook_url=None)

    runner = DiscoveryResearchRunner(
        session_factory=session_factory,
        discovery_engine_factory=engine_factory,
    )

    async def failing_run(*args, **kwargs):
        raise RuntimeError("last30days exited 1: boom")

    monkeypatch.setattr(
        "blackpine_signals.ingestion.discovery_research.run_last30days",
        failing_run,
    )

    summary = asyncio.run(runner.run_topic("AI IPO filing 2026"))

    assert summary["status"] == "failed"
    assert "boom" in summary["error"]
    assert summary["tickers_fed"] == 0
    assert summary["companies_fed"] == 0

    log = (
        db_session.query(DiscoveryLog)
        .filter_by(action="research_failed")
        .first()
    )
    assert log is not None


def test_research_runner_swallows_handoff_errors(db_session, monkeypatch):
    """An engine error on one ticker should not abort the whole cycle."""
    def session_factory():
        return db_session

    bomb_counter = {"count": 0}

    class ExplodingEngine(DiscoveryEngine):
        def process_signal(self, **kwargs):
            bomb_counter["count"] += 1
            if bomb_counter["count"] == 1:
                raise RuntimeError("boom on first call")
            return super().process_signal(**kwargs)

    def engine_factory(session):
        return ExplodingEngine(db_session=session, webhook_url=None)

    runner = DiscoveryResearchRunner(
        session_factory=session_factory,
        discovery_engine_factory=engine_factory,
    )

    async def fake_run(topic, **kwargs):
        return "$NVDA $PLTR $SOUN are on fire."

    monkeypatch.setattr(
        "blackpine_signals.ingestion.discovery_research.run_last30days",
        fake_run,
    )

    summary = asyncio.run(runner.run_topic("new AI stocks 2026"))

    # One ticker exploded, the other two should have survived
    assert summary["tickers_fed"] == 2
    assert summary["status"] == "ok"
