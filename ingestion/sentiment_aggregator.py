# /home/nodeuser/blackpine_core/blackpine_signals/ingestion/sentiment_aggregator.py
"""Layer 5: Cross-source Signal Fusion Engine. Governing doc: SPEC-028 §4.5"""
from __future__ import annotations
import logging
from dataclasses import dataclass
from sqlalchemy.orm import Session
from blackpine_signals.config import settings
from blackpine_signals.db.models import FusedSignal

logger = logging.getLogger("bps.fusion")

@dataclass
class SignalInput:
    source: str
    ticker: str | None = None
    score: float = 0.0
    signal_type: str = ""
    narrative: str = ""

@dataclass
class FusedResult:
    ticker: str | None
    fused_score: float
    source_count: int
    sources: dict
    signal_type: str
    narrative: str
    alert_tier: str

def classify_alert_tier(score: float) -> str:
    if score >= settings.signal_critical_threshold:
        return "critical"
    if score >= settings.signal_high_threshold:
        return "high"
    if score >= settings.signal_moderate_threshold:
        return "moderate"
    return "heatmap"

def fuse_signals(signals: list[SignalInput]) -> list[FusedResult]:
    by_ticker: dict[str | None, list[SignalInput]] = {}
    for s in signals:
        by_ticker.setdefault(s.ticker, []).append(s)
    results = []
    for ticker, group in by_ticker.items():
        total_score = sum(s.score for s in group)
        sources = {}
        narratives = []
        signal_types = set()
        for s in group:
            sources[s.source] = sources.get(s.source, 0) + 1
            if s.narrative:
                narratives.append(s.narrative)
            if s.signal_type:
                signal_types.add(s.signal_type)
        results.append(FusedResult(ticker=ticker, fused_score=total_score, source_count=len(group),
            sources=sources, signal_type=", ".join(sorted(signal_types)) or "mixed",
            narrative="; ".join(narratives) if narratives else "", alert_tier=classify_alert_tier(total_score)))
    return sorted(results, key=lambda r: r.fused_score, reverse=True)

def persist_fused_signals(session: Session, results: list[FusedResult]) -> int:
    inserted = 0
    for r in results:
        session.add(FusedSignal(ticker=r.ticker, signal_type=r.signal_type, fused_score=r.fused_score,
            source_count=r.source_count, sources=r.sources, narrative=r.narrative or None, alert_tier=r.alert_tier))
        inserted += 1
    session.commit()
    return inserted
