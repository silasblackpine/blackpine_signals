"""
Source Credibility Weighting System — the BPL Edge.
Governing doc: /home/nodeuser/documents/2026-04-04-spec-028-black-pine-signals.md §5
"""
from __future__ import annotations
import re

TIER_WEIGHTS = {1: 10.0, 2: 5.0, 3: 2.0, 4: 1.0}

TIER_1_HANDLES = frozenset({
    "EricNewcomer", "Deals", "jacqmelinek", "Techmeme",
    "unusual_whales", "Polymarket", "IPOtweet",
})

TIER_2_HANDLES = frozenset({
    "NirantK", "tom_doerr", "0xRicker", "cyrilXBT",
    "RoundtableSpace", "codewithimanshu", "Dan1ro0",
    "dreyk0o0", "shmidtqq",
})

IPO_KEYWORDS = frozenset({
    "ipo", "s-1", "s1 filing", "f-1", "going public",
    "pre-ipo", "lead banks", "roadshow", "direct listing",
    "confidential filing", "draft registration",
})

EARNINGS_KEYWORDS = frozenset({
    "earnings", "beat estimates", "revenue miss", "guidance",
    "eps", "quarterly results", "earnings call",
})

SIGNAL_KEYWORDS = frozenset({
    "upgrade", "downgrade", "price target", "analyst",
    "buy rating", "sell rating", "overweight", "underweight",
})

def get_credibility_tier(handle: str) -> int:
    clean = handle.lstrip("@")
    if clean in TIER_1_HANDLES:
        return 1
    if clean in TIER_2_HANDLES:
        return 2
    return 4

def compute_weighted_score(engagement: float, tier: int) -> float:
    return engagement * TIER_WEIGHTS.get(tier, 1.0)

def classify_signal(text: str) -> str | None:
    lower = text.lower()
    for kw in IPO_KEYWORDS:
        if kw in lower:
            return "ipo_signal"
    for kw in EARNINGS_KEYWORDS:
        if kw in lower:
            return "earnings"
    for kw in SIGNAL_KEYWORDS:
        if kw in lower:
            return "analyst_action"
    return None
