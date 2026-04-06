# /home/nodeuser/blackpine_core/blackpine_signals/ingestion/x_watchdog.py
"""
Layer 2: X/FinTwit Watchdog — xAI Grok-powered monitoring.
Governing doc: /home/nodeuser/documents/2026-04-04-spec-028-black-pine-signals.md §4.2
"""
from __future__ import annotations
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Callable
import httpx
from sqlalchemy.orm import Session
from blackpine_signals.db.models import XSignal
from blackpine_signals.models.credibility import classify_signal, compute_weighted_score, get_credibility_tier

logger = logging.getLogger("bps.x_watchdog")
XAI_BASE = "https://api.x.ai/v1/chat/completions"
TICKER_PATTERN = re.compile(r"\$([A-Z]{1,5})\b")

WATCHDOG_PROMPT = """Search recent X/Twitter posts from the following high-signal finance accounts about AI stocks, IPOs, and market-moving events. Return ONLY a JSON array of posts found. Each post object must have: handle, text, likes, reposts, url, posted_at (ISO 8601).

Accounts to monitor: {accounts}
Search for posts about: {topics}
Return valid JSON only, no markdown fences. If no posts found, return [].
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
    try:
        content = data["choices"][0]["message"]["content"]
        content = content.strip()
        if content.startswith("```"):
            content = re.sub(r"^```\w*\n?", "", content)
            content = re.sub(r"\n?```$", "", content)
        return json.loads(content)
    except (KeyError, IndexError, json.JSONDecodeError):
        return []

def extract_tickers(text: str) -> list[str]:
    return TICKER_PATTERN.findall(text)

class XWatchdog:
    def __init__(self, api_key: str, session_factory: Callable[[], Session],
                 accounts: list[str] | None = None, topics: list[str] | None = None) -> None:
        self.api_key = api_key
        self.session_factory = session_factory
        self.accounts = accounts or DEFAULT_ACCOUNTS
        self.topics = topics or DEFAULT_TOPICS

    async def _query_grok(self) -> dict[str, Any]:
        prompt = WATCHDOG_PROMPT.format(
            accounts=", ".join(f"@{a}" for a in self.accounts),
            topics=", ".join(self.topics),
        )
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(XAI_BASE, headers={"Authorization": f"Bearer {self.api_key}"},
                json={"model": "grok-4-1-fast-non-reasoning",
                      "messages": [{"role": "user", "content": prompt}], "temperature": 0.0})
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
            for post in posts:
                handle = post.get("handle", "unknown")
                text = post.get("text", "")
                likes = post.get("likes", 0)
                reposts = post.get("reposts", 0)
                url = post.get("url", "")
                posted_at_str = post.get("posted_at")
                if url and session.query(XSignal).filter_by(x_post_url=url).first():
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
                    signal_type=signal_type, x_post_url=url or None, posted_at=posted_at,
                ))
                inserted += 1
            session.commit()
            return inserted
        finally:
            session.close()
