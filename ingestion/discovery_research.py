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

TOPICS_PER_DAY: int = 4


def topics_for_day(day: Optional[datetime] = None) -> list[str]:
    """Return the 4 topics to research on a given date.

    Rotation is deterministic on day-of-year modulo (len(pool)/TOPICS_PER_DAY).
    With 8 topics / 4 per day, we cycle through the whole pool every 2 days.
    """
    ref = (day or datetime.now(timezone.utc)).date()
    day_idx = ref.toordinal() % (len(DISCOVERY_TOPICS) // TOPICS_PER_DAY)
    start = day_idx * TOPICS_PER_DAY
    return DISCOVERY_TOPICS[start : start + TOPICS_PER_DAY]


# ---------------------------------------------------------------------------
# Entity extraction (SPEC-028B §6)
# ---------------------------------------------------------------------------

TICKER_PATTERN = re.compile(r"\$([A-Z]{1,5})\b")
# Matches "Palantir", "Palantir Technologies", "CoreWeave", "SpaceX", "xAI Labs".
# Leading uppercase + any letters, followed by up to 3 additional capitalized words.
# The token pattern [A-Z][a-zA-Z]+ handles both PascalCase and CamelCase identifiers.
COMPANY_PATTERN = re.compile(r"\b([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3})\b")
VALUATION_PATTERN = re.compile(r"\$[\d.]+\s*[BMT](?:illion)?\b", re.IGNORECASE)
IPO_CONTEXT = re.compile(
    r"\b(?:IPO|S-1|F-1|going public|pre[- ]IPO|filed for|Form D)\b",
    re.IGNORECASE,
)

# Stop-words for the COMPANY_PATTERN — these are frequent proper-noun matches
# that are almost never real company names worth tracking.
COMPANY_STOPWORDS: frozenset[str] = frozenset({
    "United States", "United Kingdom", "New York", "Wall Street", "Silicon Valley",
    "Artificial Intelligence", "Machine Learning", "Large Language",
    "Federal Reserve", "Black Pine", "Hacker News", "Reuters", "Bloomberg",
    "The New York Times", "The Wall Street Journal", "CNBC", "Financial Times",
    "Reddit", "Twitter", "YouTube", "TikTok", "Instagram",
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "May", "June", "July", "August",
    "September", "October", "November", "December",
})


def extract_tickers_from_text(text: str) -> set[str]:
    """Return unique $TICKERs found in a text blob."""
    if not text:
        return set()
    return {m.group(1) for m in TICKER_PATTERN.finditer(text)}


def extract_ipo_company_mentions(text: str, window: int = 200) -> set[str]:
    """Return company names that appear within `window` chars of IPO_CONTEXT.

    This is a conservative filter — we only treat a company name as a
    discovery candidate when it is co-located with IPO-context language
    (S-1, pre-IPO, filed for, etc.). Otherwise any capitalized pair of
    words would flood the discovery pipeline.
    """
    if not text:
        return set()
    matches: set[str] = set()
    for ipo_m in IPO_CONTEXT.finditer(text):
        start = max(0, ipo_m.start() - window)
        end = min(len(text), ipo_m.end() + window)
        chunk = text[start:end]
        for name_m in COMPANY_PATTERN.finditer(chunk):
            name = name_m.group(1).strip()
            if name in COMPANY_STOPWORDS:
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

    cmd = [
        "python3",
        str(resolved),
        "--emit", "md",
        "--quick",
        "--store",
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

    async def run_topic(self, topic: str) -> dict:
        """Run one topic end-to-end. Returns a summary dict for logging."""
        summary: dict = {
            "topic": topic,
            "status": "ok",
            "tickers_fed": 0,
            "companies_fed": 0,
            "error": None,
        }
        try:
            md_output = await run_last30days(
                topic,
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
