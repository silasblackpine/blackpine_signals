# /home/nodeuser/blackpine_core/blackpine_signals/ingestion/x_watchdog.py
"""
Layer 2: X/FinTwit Watchdog — xAI Grok-powered monitoring.

Governing docs:
  /documents/2026-04-04-spec-028-black-pine-signals.md §4.2
  /documents/2026-04-07-spec-028c-x-watchdog-responses-api-migration.md

SPEC-028C (2026-04-07): Migrated from the deprecated
`/v1/chat/completions` live-search path to the xAI Responses API
(`POST /v1/responses`) with the built-in `x_search` tool. The old
endpoint silently returned empty payloads after xAI retired implicit
Live Search on chat completions (darkness since 2026-04-05).
"""
from __future__ import annotations
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Callable
import httpx
from sqlalchemy.orm import Session
from blackpine_signals.db.models import WatchlistTicker, XSignal
from blackpine_signals.models.credibility import classify_signal, compute_weighted_score, get_credibility_tier
# SPEC-028B Task 3: optional discovery hook
from blackpine_signals.ingestion.discovery_engine import DiscoveryEngine
# SPEC-028B Task 5b: optional ticker validation before scoring
from blackpine_signals.ingestion.symbol_validator import SymbolValidator

logger = logging.getLogger("bps.x_watchdog")

# SPEC-028C: Responses API endpoint (replaces chat/completions live search).
XAI_BASE = "https://api.x.ai/v1/responses"

# SPEC-028C: Reasoning model with Responses API + x_search tool support.
# Empirically validated 2026-04-07. Use reasoning.effort=low for cost control.
XAI_MODEL = "grok-4.20-0309-reasoning"

TICKER_PATTERN = re.compile(r"\$([A-Z]{1,5})\b")

WATCHDOG_PROMPT = """Use the x_search tool to find recent X/Twitter posts from high-signal finance accounts about AI stocks, IPOs, and market-moving events. Focus the search on these accounts and topics.

Accounts to monitor: {accounts}
Topics of interest: {topics}

Return ONLY a JSON array of posts found — no markdown fences, no prose, no explanation. Each post object MUST have exactly these keys: handle, text, likes, reposts, url, posted_at (ISO-8601). If no posts found, return the empty array [].
"""

DEFAULT_ACCOUNTS = [
    "EricNewcomer", "Deals", "jacqmelinek", "Techmeme",
    "unusual_whales", "Polymarket", "IPOtweet",
    "NirantK", "tom_doerr", "0xRicker", "cyrilXBT",
    "RoundtableSpace", "codewithimanshu", "Dan1ro0",
    "dreyk0o0", "shmidtqq", "BeatTheInsider",
    "InvestmentGuru_", "Abdullla_ai", "beatsinbrief",
    "Cointelegraph", "BitcoinNews", "AnthropicAI",
    "claudeai", "pstAsiatech",
]

DEFAULT_TOPICS = [
    "AI stocks", "IPO filing", "Anthropic", "NVIDIA earnings",
    "Polymarket", "AI investing", "S-1 filing", "pre-IPO",
]

def parse_grok_response(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract the assistant message JSON array from a Responses API payload.

    SPEC-028C: The Responses API returns ``data["output"]`` as a list of
    mixed-type items (tool calls, reasoning traces, and the final assistant
    message). We walk the list, locate the ``type=="message"`` item (role
    ``assistant``), grab the first ``output_text`` content block, and
    JSON-decode its text. Backward-compat: if the payload still has the old
    ``choices[0].message.content`` shape (e.g. in legacy fixtures), fall back
    to that path so existing tests/mocks are not silently broken.
    """
    content: str | None = None
    try:
        # SPEC-028C: new Responses API shape
        if "output" in data and isinstance(data["output"], list):
            for item in data["output"]:
                if not isinstance(item, dict):
                    continue
                if item.get("type") != "message":
                    continue
                content_blocks = item.get("content") or []
                for block in content_blocks:
                    if isinstance(block, dict) and block.get("type") == "output_text":
                        content = block.get("text")
                        break
                if content is not None:
                    break

        # Backward-compat: legacy chat/completions shape
        if content is None and "choices" in data:
            content = data["choices"][0]["message"]["content"]

        if content is None:
            logger.warning(
                "parse_grok_response: no output_text found in Responses payload | keys=%s",
                list(data.keys()),
            )
            return []

        content = content.strip()
        if content.startswith("```"):
            content = re.sub(r"^```\w*\n?", "", content)
            content = re.sub(r"\n?```$", "", content)

        parsed = json.loads(content)
        if not isinstance(parsed, list):
            logger.warning(
                "parse_grok_response: expected JSON array, got %s | first=%r",
                type(parsed).__name__,
                content[:200],
            )
            return []
        return parsed
    except (KeyError, IndexError) as exc:
        logger.warning("parse_grok_response: missing field in Grok payload: %s | raw=%r", exc, data)
        return []
    except json.JSONDecodeError as exc:
        logger.warning(
            "parse_grok_response: JSONDecodeError: %s | content=%r",
            exc,
            content[:500] if content else None,
        )
        return []

def extract_tickers(text: str) -> list[str]:
    return TICKER_PATTERN.findall(text)

class XWatchdog:
    def __init__(self, api_key: str, session_factory: Callable[[], Session],
                 accounts: list[str] | None = None, topics: list[str] | None = None,
                 discovery_engine_factory: Callable[[Session], DiscoveryEngine] | None = None,
                 symbol_validator: SymbolValidator | None = None) -> None:
        self.api_key = api_key
        self.session_factory = session_factory
        self.accounts = accounts or DEFAULT_ACCOUNTS
        self.topics = topics or DEFAULT_TOPICS
        # SPEC-028B Task 3: optional discovery engine factory.
        # Scanners remain standalone-runnable if no discovery hook is wired.
        self.discovery_engine_factory = discovery_engine_factory
        # SPEC-028B Task 5b: optional symbol validator. If provided, garbage
        # tickers (e.g. $WAGMI, $LFG) are filtered out before discovery
        # scoring. None = unknown (validator missing key) → accept.
        self.symbol_validator = symbol_validator

    async def _query_grok(self) -> dict[str, Any]:
        """Call the xAI Responses API with the `x_search` tool enabled.

        SPEC-028C: Migrated from `/v1/chat/completions` (Live Search
        deprecated) to `/v1/responses` with `tools=[{"type": "x_search"}]`.
        Grok auto-invokes the internal `x_keyword_search` with its own query
        derived from our prompt, then returns the structured tweet list in
        the final assistant message.
        """
        prompt = WATCHDOG_PROMPT.format(
            accounts=", ".join(f"@{a}" for a in self.accounts),
            topics=", ".join(self.topics),
        )
        body = {
            "model": XAI_MODEL,
            "input": [{"role": "user", "content": prompt}],
            "tools": [{"type": "x_search"}],
            "reasoning": {"effort": "low"},  # cost control
        }
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                XAI_BASE,
                headers={"Authorization": f"Bearer {self.api_key}"},
                json=body,
            )
            resp.raise_for_status()
            return resp.json()

    async def scan(self) -> int:
        data = await self._query_grok()
        posts = parse_grok_response(data)
        if not posts:
            return 0
        session = self.session_factory()
        try:
            inserted = 0
            # SPEC-028B Task 3: collect (ticker, handle, tier) tuples from
            # THIS scan cycle for discovery-engine handoff after the loop.
            scan_cycle_mentions: list[tuple[str, str, int]] = []
            for post in posts:
                handle = post.get("handle", "unknown")
                text = post.get("text", "")
                likes = post.get("likes", 0)
                reposts = post.get("reposts", 0)
                url = post.get("url", "")
                posted_at_str = post.get("posted_at")
                if url and session.query(XSignal).filter_by(url=url).first():
                    continue
                tier = get_credibility_tier(handle)
                engagement = float(likes + reposts * 2)
                weighted = compute_weighted_score(engagement, tier)
                signal_type = classify_signal(text)
                tickers = extract_tickers(text)
                lower = text.lower()
                sentiment = "bullish" if any(w in lower for w in ("up", "buy", "bull", "beat", "surge", "soar")) \
                    else "bearish" if any(w in lower for w in ("down", "sell", "bear", "miss", "drop", "crash")) \
                    else "neutral"
                try:
                    posted_at = datetime.fromisoformat(posted_at_str) if posted_at_str else datetime.now(timezone.utc)
                except (ValueError, TypeError):
                    posted_at = datetime.now(timezone.utc)
                session.add(XSignal(
                    source_handle=handle, source_tier=tier, ticker_refs=tickers,
                    text=text[:2000], engagement_score=engagement,
                    credibility_weighted_score=weighted, sentiment=sentiment,
                    signal_type=signal_type, url=url or None, posted_at=posted_at,
                ))
                inserted += 1
                for t in tickers:
                    scan_cycle_mentions.append((t, handle, tier))
            session.commit()

            # SPEC-028B Task 3: hand unknown tickers to the discovery engine.
            if self.discovery_engine_factory and scan_cycle_mentions:
                await self._emit_discoveries(session, scan_cycle_mentions)

            return inserted
        finally:
            session.close()

    # ------------------------------------------------------------------
    # SPEC-028B Task 3 + Task 5b
    # ------------------------------------------------------------------
    async def _emit_discoveries(
        self, session: Session, mentions: list[tuple[str, str, int]]
    ) -> None:
        """Cross-reference tickers against watchlist; optionally validate
        each unknown via SymbolValidator; feed survivors to discovery."""
        if self.discovery_engine_factory is None:
            return
        # Fetch all known symbols once
        known = {
            row[0]
            for row in session.query(WatchlistTicker.symbol).all()
        }
        unknown_mentions = [
            (t, h, tier) for (t, h, tier) in mentions if t not in known
        ]
        if not unknown_mentions:
            return

        # SPEC-028B Task 5b: validate each unknown ticker against AlphaVantage
        # SYMBOL_SEARCH before scoring it. validator.validate() returns:
        #   True  → real ticker, accept
        #   False → garbage (e.g. $WAGMI), reject
        #   None  → couldn't determine (no key, network error), accept
        if self.symbol_validator is not None:
            validated: list[tuple[str, str, int]] = []
            # Dedup tickers in this batch — same ticker can appear in multiple
            # posts; we only need to validate it once per scan cycle.
            seen_validation: dict[str, bool | None] = {}
            for ticker, handle, tier in unknown_mentions:
                if ticker not in seen_validation:
                    try:
                        seen_validation[ticker] = await self.symbol_validator.validate(ticker)
                    except Exception:
                        logger.exception("symbol_validator failed for $%s", ticker)
                        seen_validation[ticker] = None  # treat as unknown → accept
                if seen_validation[ticker] is False:
                    logger.info(
                        "symbol_validator rejected $%s — skipping discovery scoring",
                        ticker,
                    )
                    continue
                validated.append((ticker, handle, tier))
            unknown_mentions = validated
            if not unknown_mentions:
                return

        engine = self.discovery_engine_factory(session)
        for ticker, handle, tier in unknown_mentions:
            try:
                engine.ingest_x_ticker(ticker=ticker, handle=handle, tier=tier)
            except Exception:
                logger.exception(
                    "Discovery ingest failed for X ticker $%s from @%s", ticker, handle
                )
