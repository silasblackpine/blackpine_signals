"""
SPEC-028B Task 4 — Discovery Research Runner.

Wraps the `/last30days` skill as a subprocess to perform automated weekly→daily
deep research on rotating discovery topics, parses the markdown output for
entity mentions (tickers, companies, IPO context), and feeds survivors into
the DiscoveryEngine.

Governing docs:
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §2 Source 2, §6
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 4
  /documents/2026-04-07-spec-028c-x-watchdog-responses-api-migration.md (sibling Phase 2 fix)

Design notes:
- The skill is invoked as a subprocess in `--emit md --quick --store --days 7`
  mode. Quick mode trims cost/time while still hitting Reddit, X, HN, Polymarket,
  YouTube, web, and optionally the Meta/Instagram/TikTok sources when
  ScrapeCreators credits are available.
- Graceful degradation: if individual sources return 402 (ScrapeCreators tier
  exhausted) or any non-fatal error, the skill still writes whatever data it
  got; we log the partial and carry on. A full subprocess failure is also
  swallowed into a discovery_log row with action=research_failed.
- Topic rotation: 8 topics in the pool, 4 per day, cycling through the pool
  over 2 days. Topic index is derived from the ISO week+day so the rotation
  is deterministic and not dependent on persistent state.
- Entity extraction uses the SPEC-028B §6 regex set (TICKER_PATTERN,
  COMPANY_PATTERN, VALUATION_PATTERN, IPO_CONTEXT). A ticker is always
  fed to discovery; a company name is only fed when co-located with
  IPO_CONTEXT within a 200-char window.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from sqlalchemy.orm import Session

from blackpine_signals.db.models import DiscoveryLog, WatchlistTicker
from blackpine_signals.ingestion.discovery_engine import (
    DiscoveryEngine,
    LAST30DAYS_MENTION_SCORE,
)

logger = logging.getLogger("bps.discovery_research")

# ---------------------------------------------------------------------------
# Topic rotation (SPEC-028B §2 Source 2, daily cadence per assumptions.md 2026-04-06)
# ---------------------------------------------------------------------------

DISCOVERY_TOPICS: list[str] = [
    "new AI stocks 2026",
    "AI IPO filing 2026",
    "AI companies funding round 2026",
    "AI startups to watch",
    "AI infrastructure companies",
    "best performing AI stocks this month",
    "AI company acquisitions 2026",
    "AI SPAC merger 2026",
]

# 2026-04-08: cadence reshape — 3 slots/day at 06:00/13:00/20:00 ET, Mon-Sun.
# Governing plan: /home/nodeuser/documents/2026-04-08-bps-cadence-whale-watcher.md
TOPICS_PER_DAY: int = 3


def topics_for_day(day: Optional[datetime] = None) -> list[str]:
    """Return today's research topics via a sliding window over the full pool.

    For any pool length P and any TOPICS_PER_DAY = K (K capped at P), day N
    selects indices [N*K mod P, (N*K+1) mod P, ..., (N*K+K-1) mod P]. The
    window wraps cleanly so the system is resilient to any pool size.

    With P=8, K=3 the rotation covers the full pool in ceil(8/3) = 3 days:
        day 0 → [0,1,2]
        day 1 → [3,4,5]
        day 2 → [6,7,0]
        day 3 → [1,2,3]
        ...
    """
    pool_len = len(DISCOVERY_TOPICS)
    if pool_len == 0:
        return []
    k = min(TOPICS_PER_DAY, pool_len)
    ref = (day or datetime.now(timezone.utc)).date()
    start = (ref.toordinal() * k) % pool_len
    return [DISCOVERY_TOPICS[(start + i) % pool_len] for i in range(k)]


# ---------------------------------------------------------------------------
# Entity extraction (SPEC-028B §6)
# ---------------------------------------------------------------------------

TICKER_PATTERN = re.compile(r"\$([A-Z]{1,5})\b")

# Common all-caps acronyms that get $-prefixed in tweets/articles but are
# NOT real ticker symbols. Identified 2026-04-08 from the first overnight
# autonomous run, where `$IPO` accumulated to 40pt and tripped the
# PROPOSE threshold. Hard-coded per CLAUDE.md Hard Rule #5.
TICKER_BLACKLIST: frozenset[str] = frozenset({
    # Acronyms that frequently appear with a $ in finance prose
    "IPO", "AI", "API", "USA", "USD", "EUR", "GBP", "JPY", "CNY",
    "CEO", "CFO", "CTO", "COO", "CMO", "VP", "EVP", "SVP",
    "GPU", "CPU", "TPU", "LLM", "GPT", "RAG", "ML", "DL", "NLP",
    "ETF", "ETN", "REIT", "SPAC", "IPO", "M", "B", "T",
    "Q", "Y", "M2", "M1", "PE", "EV", "NAV", "EPS", "PNL",
    "FTC", "SEC", "DOJ", "FDA", "FTC", "IRS", "DOE", "DOD",
    "NYSE", "NASDAQ", "DOW", "SP", "SPX", "DJI", "VIX",
    # Tech vendor / model abbreviations that aren't tradeable
    "AWS", "GCP", "MS", "FB", "IG", "TT", "YT", "X", "TG",
    "OS", "DB", "OS", "VM", "SDK", "API", "SQL", "NOSQL",
})
# Matches "Palantir", "Palantir Technologies", "CoreWeave", "SpaceX", "xAI Labs".
# Leading uppercase + any letters, followed by up to 3 additional capitalized words.
# The token pattern [A-Z][a-zA-Z]+ handles both PascalCase and CamelCase identifiers.
COMPANY_PATTERN = re.compile(r"\b([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3})\b")
VALUATION_PATTERN = re.compile(r"\$[\d.]+\s*[BMT](?:illion)?\b", re.IGNORECASE)
IPO_CONTEXT = re.compile(
    r"\b(?:IPO|S-1|F-1|going public|pre[- ]IPO|filed for|Form D)\b",
    re.IGNORECASE,
)

# Stop-words for the COMPANY_PATTERN — frequent proper-noun matches that
# are almost never real company names worth tracking. Grouped by source:
#
#   - Geography / institutions
#   - Generic technology phrases that get title-cased
#   - News / media outlets (re-mentions, not primary targets)
#   - Days / months
#   - Markdown section headers and summary labels produced by /last30days
#     (these are the noise source fixed 2026-04-07 after the first live run)
COMPANY_STOPWORDS: frozenset[str] = frozenset({
    # Geography / institutions
    "United States", "United Kingdom", "New York", "Wall Street", "Silicon Valley",
    "Federal Reserve",
    # Generic tech phrases
    "Artificial Intelligence", "Machine Learning", "Large Language",
    "Deep Learning", "Foundation Model", "Neural Network",
    # Lab + news outlets
    "Black Pine", "Hacker News", "Reuters", "Bloomberg", "Techmeme",
    "The New York Times", "The Wall Street Journal", "CNBC", "Financial Times",
    "Business Insider", "The Information", "The Verge", "Wired", "Forbes",
    "Axios", "Semafor",
    "Reddit", "Twitter", "YouTube", "TikTok", "Instagram", "LinkedIn",
    # Days / months
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "May", "June", "July", "August",
    "September", "October", "November", "December",
    # Markdown section labels / metric headers from /last30days output
    # (identified 2026-04-07 in the first live trigger_research_now.py run)
    "Score", "Scores", "Engagement", "Engagements", "Relevance",
    "Reports", "Report", "Date", "Dates", "Highlights", "Findings",
    "Summary", "Overview", "Metrics", "Statistics", "Analysis",
    "Recommendation", "Recommendations", "Conclusion", "Conclusions",
    "Introduction", "Background", "Context", "Methodology", "Notes",
    "Sources", "Citations", "References", "Appendix", "Discussion",
    "Key Takeaways", "Top Posts", "Top Comments", "Top Results",
    "Research", "Insight", "Insights", "Findings Summary",
    "Post", "Posts", "Thread", "Threads", "Comment", "Comments",
    "Likes", "Reposts", "Shares", "Views", "Mentions",
    # Common-noun false positives identified 2026-04-08 from the first
    # overnight autonomous run (1st-night ZHGP audit)
    "Whoever", "Same", "Targeting", "Tech", "Stocks", "Track",
    "Potentially", "Valuations", "Startups", "StockMarket", "Spac",
    "Whoever", "VentureCapital", "Open Ai",
})


def strip_markdown_headers(text: str) -> str:
    """Remove lines that are markdown headers (`#`, `##`, `###`, …).

    /last30days produces a lot of section structure — "## Score", "## Engagement",
    "## Relevance", etc. Those headers polluted the first live ZHGP run because
    COMPANY_PATTERN would match the header label as a "company name" whenever
    the 200-char co-location window happened to contain an IPO keyword.
    Stripping header lines eliminates an entire class of false positives
    without touching the real prose content of the document.

    Also strips markdown table header rows (lines of only `|`, `-`, `:` and
    whitespace) and horizontal rules (`---`, `***`, `___`).
    """
    if not text:
        return text
    cleaned_lines: list[str] = []
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        # Table separator like `| --- | --- |`
        if stripped and set(stripped).issubset(set("|-: \t")):
            continue
        # Horizontal rule
        if stripped in ("---", "***", "___"):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def extract_tickers_from_text(text: str) -> set[str]:
    """Return unique $TICKERs found in a text blob.

    Filters out TICKER_BLACKLIST entries (common all-caps acronyms like
    $IPO, $AI, $CEO that appear with `$` in finance prose but are not
    real tradeable symbols). Identified 2026-04-08 after the first
    overnight ZHGP run promoted `IPO` to PROPOSED.
    """
    if not text:
        return set()
    return {
        m.group(1)
        for m in TICKER_PATTERN.finditer(text)
        if m.group(1) not in TICKER_BLACKLIST
    }


def extract_ipo_company_mentions(text: str, window: int = 200) -> set[str]:
    """Return company names that appear within `window` chars of IPO_CONTEXT.

    This is a conservative filter — we only treat a company name as a
    discovery candidate when it is co-located with IPO-context language
    (S-1, pre-IPO, filed for, etc.). Otherwise any capitalized pair of
    words would flood the discovery pipeline.

    As of 2026-04-07 the text is pre-cleaned via `strip_markdown_headers`
    so `/last30days` section labels can't contaminate the extraction.
    """
    if not text:
        return set()
    cleaned = strip_markdown_headers(text)
    matches: set[str] = set()
    for ipo_m in IPO_CONTEXT.finditer(cleaned):
        raw_start = max(0, ipo_m.start() - window)
        raw_end = min(len(cleaned), ipo_m.end() + window)
        # 2026-04-09 fix: snap window to whitespace boundaries so the
        # chunk slice never bisects a word. Without this, Python's `\b`
        # matches the artificial chunk-end and the regex captures
        # truncated halves like "Targeti" or "Scor" (observed in the
        # 2026-04-09 manual run audit).
        start = raw_start
        while start > 0 and not cleaned[start - 1].isspace():
            start -= 1
        end = raw_end
        while end < len(cleaned) and not cleaned[end].isspace():
            end += 1
        chunk = cleaned[start:end]
        for name_m in COMPANY_PATTERN.finditer(chunk):
            name = name_m.group(1).strip()
            if name in COMPANY_STOPWORDS:
                continue
            # Reject matches starting with a definite/indefinite article
            # ("The US", "The IPO", "An AI" etc.) — these are prose
            # fragments, not entity names. Identified 2026-04-08 audit.
            if name.startswith(("The ", "An ", "A ", "Their ", "Our ")):
                continue
            # Reject if the first token is a stop-word (e.g.
            # "Highlights Crusoe AI" where "Highlights" is a header label
            # that got glued onto a real company name).
            first_word = name.split()[0] if name else ""
            if first_word in COMPANY_STOPWORDS:
                continue
            if len(name) < 4:  # reject very short matches
                continue
            # Reject all-uppercase tokens — those are tickers (e.g. NVDA, PLTR),
            # not company names. The ticker-extraction path handles them.
            if name.isupper():
                continue
            matches.add(name)
    return matches


# ---------------------------------------------------------------------------
# /last30days subprocess wrapper
# ---------------------------------------------------------------------------

DEFAULT_SKILL_PATHS: list[Path] = [
    Path("/home/nodeuser/.claude/plugins/marketplaces/last30days-skill/scripts/last30days.py"),
    Path("/home/nodeuser/.claude/plugins/cache/last30days-skill/last30days/2.9.6/scripts/last30days.py"),
]


def resolve_last30days_script(override: str = "") -> Optional[Path]:
    """Return the first existing path from override or DEFAULT_SKILL_PATHS."""
    if override:
        p = Path(override)
        return p if p.is_file() else None
    for candidate in DEFAULT_SKILL_PATHS:
        if candidate.is_file():
            return candidate
    return None


async def run_last30days(
    topic: str,
    *,
    script_path: Optional[Path] = None,
    days: int = 7,
    timeout_seconds: float = 300.0,
) -> str:
    """Invoke /last30days as a subprocess and return its markdown output.

    Raises RuntimeError on subprocess failure or timeout. Callers should wrap
    this in their own try/except to log a discovery_log 'research_failed' row.
    """
    resolved = script_path or resolve_last30days_script()
    if resolved is None:
        raise RuntimeError("last30days skill script not found on disk")

    # 2026-04-09: --store flag dropped to work around a /last30days
    # v2.9.6 bug in store.py:341 (`max(int, NoneType)` TypeError on
    # duplicate findings with NULL engagement_score). We never read the
    # skill's local SQLite (`~/.local/share/last30days/research.db`) —
    # we parse stdout markdown directly — so dropping --store is a
    # zero-loss fix that bypasses the persistence path entirely.
    cmd = [
        "python3",
        str(resolved),
        "--emit", "md",
        "--quick",
        "--days", str(days),
        topic,
    ]
    logger.info("discovery_research: invoking last30days: %s", " ".join(cmd))

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=timeout_seconds
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"last30days timed out after {timeout_seconds}s on topic={topic!r}")

    if proc.returncode != 0:
        err_tail = (stderr or b"")[-500:].decode(errors="replace")
        raise RuntimeError(
            f"last30days exited {proc.returncode} on topic={topic!r}: {err_tail}"
        )

    return (stdout or b"").decode(errors="replace")


# ---------------------------------------------------------------------------
# Runner — wires it all together
# ---------------------------------------------------------------------------

class DiscoveryResearchRunner:
    """Orchestrates a single research cycle: subprocess → parse → discovery."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        discovery_engine_factory: Callable[[Session], DiscoveryEngine],
        *,
        script_path: Optional[Path] = None,
        timeout_seconds: float = 300.0,
    ) -> None:
        self.session_factory = session_factory
        self.discovery_engine_factory = discovery_engine_factory
        self.script_path = script_path
        self.timeout_seconds = timeout_seconds

    async def run_topic(self, topic: str, extra_context: str = "") -> dict:
        """Run one topic end-to-end. Returns a summary dict for logging.

        2026-04-08: ``extra_context`` (Whale Watcher summary etc.) is appended
        to the topic string passed to ``/last30days``. The skill takes the
        topic as a single CLI argument; concatenation is the cleanest
        injection path that requires no skill modification. Capped at 1500
        chars defensively.
        """
        summary: dict = {
            "topic": topic,
            "status": "ok",
            "tickers_fed": 0,
            "companies_fed": 0,
            "error": None,
        }
        if extra_context:
            extra_context = extra_context[:1500]
            effective_topic = (
                f"{topic}\n\nAdditional 24h Whale Watcher context "
                f"(high-signal X accounts):\n{extra_context}"
            )
        else:
            effective_topic = topic
        try:
            md_output = await run_last30days(
                effective_topic,
                script_path=self.script_path,
                timeout_seconds=self.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001 — we want to swallow
            logger.warning("discovery_research: topic %r failed: %s", topic, exc)
            summary["status"] = "failed"
            summary["error"] = str(exc)[:500]
            self._log_research_event(topic, "research_failed", summary)
            return summary

        tickers = extract_tickers_from_text(md_output)
        companies = extract_ipo_company_mentions(md_output)

        session = self.session_factory()
        try:
            # Filter out tickers already on the watchlist — those don't
            # need discovery-engine handoff.
            known = {
                row[0]
                for row in session.query(WatchlistTicker.symbol).all()
            }
            unknown_tickers = tickers - known

            engine = self.discovery_engine_factory(session)
            for ticker in sorted(unknown_tickers):
                try:
                    engine.process_signal(
                        entity_name=ticker,
                        entity_type="ticker",
                        source="last30days",
                        signal_score=LAST30DAYS_MENTION_SCORE,
                        details={"topic": topic},
                        evidence=f"last30days research: {topic}",
                    )
                    summary["tickers_fed"] += 1
                except Exception:
                    logger.exception(
                        "discovery_research: ticker %s handoff failed", ticker
                    )

            for company in sorted(companies):
                try:
                    engine.process_signal(
                        entity_name=company,
                        entity_type="private_company",
                        source="last30days",
                        signal_score=LAST30DAYS_MENTION_SCORE,
                        details={"topic": topic, "ipo_context": True},
                        evidence=f"last30days research (IPO context): {topic}",
                    )
                    summary["companies_fed"] += 1
                except Exception:
                    logger.exception(
                        "discovery_research: company %s handoff failed", company
                    )

            self._log_research_event(topic, "research_completed", summary, session=session)
        finally:
            session.close()

        logger.info(
            "discovery_research: topic=%r tickers_fed=%d companies_fed=%d",
            topic,
            summary["tickers_fed"],
            summary["companies_fed"],
        )
        return summary

    def _log_research_event(
        self,
        topic: str,
        action: str,
        details: dict,
        session: Optional[Session] = None,
    ) -> None:
        """Append a DiscoveryLog row for this research run."""
        own_session = session is None
        if own_session:
            session = self.session_factory()
        try:
            entry = DiscoveryLog(
                entity_name=f"research:{topic}",
                action=action,
                details=dict(details),
                automated=True,
            )
            session.add(entry)
            session.commit()
        except Exception:
            logger.exception("discovery_research: failed to write DiscoveryLog")
            session.rollback()
        finally:
            if own_session:
                session.close()
