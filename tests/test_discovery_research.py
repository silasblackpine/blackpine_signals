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
    TICKER_BLACKLIST,
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


def test_topics_for_day_sliding_window_full_coverage():
    """2026-04-08 cadence reshape: with K=3 and P=8 the sliding window
    covers the entire pool in ceil(8/3) = 3 consecutive days."""
    days = [
        datetime(2026, 4, 6, tzinfo=timezone.utc),
        datetime(2026, 4, 7, tzinfo=timezone.utc),
        datetime(2026, 4, 8, tzinfo=timezone.utc),
    ]
    combined: set[str] = set()
    for d in days:
        combined |= set(topics_for_day(d))
    assert combined == set(DISCOVERY_TOPICS)


def test_topics_for_day_no_overlap_consecutive_days():
    """Step=K=3 and pool=8 → consecutive days never share a topic."""
    d1 = datetime(2026, 4, 6, tzinfo=timezone.utc)
    d2 = datetime(2026, 4, 7, tzinfo=timezone.utc)
    assert set(topics_for_day(d1)).isdisjoint(set(topics_for_day(d2)))


def test_topics_for_day_sliding_window_wraps():
    """The window must wrap cleanly when the start index nears pool end."""
    # Find a day whose start index is 6 (so window is [6,7,0])
    from datetime import date, timedelta
    base = date(2026, 4, 6)
    found = False
    for delta in range(0, 16):
        d = datetime.combine(base + timedelta(days=delta), datetime.min.time(), tzinfo=timezone.utc)
        topics = topics_for_day(d)
        if topics == [DISCOVERY_TOPICS[6], DISCOVERY_TOPICS[7], DISCOVERY_TOPICS[0]]:
            found = True
            break
    assert found, "no wrap-around day found in 16-day window — sliding window broken"


def test_topics_for_day_resilient_to_pool_resize(monkeypatch):
    """Sliding window must not crash on a smaller pool than TOPICS_PER_DAY."""
    from blackpine_signals.ingestion import discovery_research as dr
    monkeypatch.setattr(dr, "DISCOVERY_TOPICS", ["only_one"])
    out = dr.topics_for_day(datetime(2026, 4, 8, tzinfo=timezone.utc))
    assert out == ["only_one"]


def test_run_topic_extra_context_injection(db_session, monkeypatch):
    """Whale Watcher summary must be appended to the topic argument."""
    from blackpine_signals.ingestion import discovery_research as dr

    captured: dict = {}

    async def _fake_run_last30days(topic, *, script_path=None, timeout_seconds=300.0):
        captured["topic"] = topic
        return ""  # empty markdown — no tickers/companies extracted

    monkeypatch.setattr(dr, "run_last30days", _fake_run_last30days)

    runner = dr.DiscoveryResearchRunner(
        session_factory=lambda: db_session,
        discovery_engine_factory=lambda s: DiscoveryEngine(db_session=s),
    )
    extra = "Top mentioned tickers: $NVDA (x3), $PLTR (x2)."
    asyncio.run(runner.run_topic("AI stocks 2026", extra_context=extra))

    assert "AI stocks 2026" in captured["topic"]
    assert "Whale Watcher context" in captured["topic"]
    assert extra in captured["topic"]


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
# 2026-04-08 ZHGP overnight-audit fixes
# ---------------------------------------------------------------------------

def test_ticker_blacklist_contains_critical_acronyms():
    """Acronyms that triggered the IPO false-positive must be blacklisted."""
    for sym in ("IPO", "AI", "API", "CEO", "CFO", "GPU", "LLM", "USA", "ETF"):
        assert sym in TICKER_BLACKLIST, f"{sym!r} missing from TICKER_BLACKLIST"


def test_extract_tickers_filters_blacklist():
    """The exact failure mode from 2026-04-08 audit: $IPO must NOT extract."""
    text = "$IPO season is here. $AI is hot. $CEO compensation rising. $NVDA is real."
    tickers = extract_tickers_from_text(text)
    assert "IPO" not in tickers
    assert "AI" not in tickers
    assert "CEO" not in tickers
    assert "NVDA" in tickers  # real ticker still passes


def test_extract_tickers_blacklist_does_not_break_valid_tickers():
    """Make sure all the tickers we know are real still extract."""
    text = "$NVDA $AVGO $TSM $MU $MSFT $GOOGL $META $NBIS $IREN $SOUN $PLTR $GEV $CEG"
    tickers = extract_tickers_from_text(text)
    expected = {"NVDA", "AVGO", "TSM", "MU", "MSFT", "GOOGL", "META", "NBIS", "IREN", "SOUN", "PLTR", "GEV", "CEG"}
    assert tickers == expected


def test_extract_ipo_companies_rejects_article_prefixes():
    """'The US', 'The IPO', 'An AI startup' etc. must not be company names."""
    text = (
        "The US is leading AI investment. The IPO market is heating up. "
        "An AI startup is going public. A company filed for S-1. "
        "Their CFO confirmed. Our team filed. Databricks is real."
    )
    companies = extract_ipo_company_mentions(text)
    for noise in ("The US", "The IPO", "An AI", "A company", "Their CFO", "Our team"):
        assert noise not in companies, f"{noise!r} leaked through article filter"
    assert any("Databricks" in c for c in companies)


def test_extract_ipo_companies_rejects_audit_noise_words():
    """The exact noise list pulled from the 2026-04-08 audit DB query."""
    text = (
        "Whoever files first wins. Same with the next batch. "
        "Targeting AI startups. Tech is hot. Stocks are up. "
        "Track these IPOs closely. Potentially a billion-dollar deal. "
        "Valuations matter. Startups should file early. "
        "Databricks filed for S-1 today."
    )
    companies = extract_ipo_company_mentions(text)
    for noise in (
        "Whoever", "Same", "Targeting", "Tech", "Stocks",
        "Track", "Potentially", "Valuations", "Startups",
    ):
        assert noise not in companies, f"{noise!r} leaked through stopword filter"
    assert any("Databricks" in c for c in companies)


def test_real_companies_still_pass_after_all_filters():
    """End-to-end: even with all filters in place, real names get through."""
    text = (
        "Databricks filed for S-1. CoreWeave is going public. "
        "OpenAI may file for IPO next year. SpaceX is pre-IPO. "
        "Anthropic filed Form D. Bytedance is going public."
    )
    companies = extract_ipo_company_mentions(text)
    for real in ("Databricks", "CoreWeave", "OpenAI", "SpaceX", "Anthropic", "Bytedance"):
        assert any(real in c for c in companies), f"{real!r} was wrongly rejected"


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
