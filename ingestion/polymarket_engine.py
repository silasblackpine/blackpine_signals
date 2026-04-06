# /home/nodeuser/blackpine_core/blackpine_signals/ingestion/polymarket_engine.py
"""Layer 4: Polymarket Correlation Engine. Governing doc: SPEC-028 §4.4"""
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable
from sqlalchemy.orm import Session
from blackpine_signals.db.models import (
    DiscoveryCandidate,
    IPOPipeline,
    PolymarketCorrelation,
    PolymarketSignal,
    WatchlistTicker,
)
# SPEC-028B Task 3: discovery handoff
from blackpine_signals.ingestion.discovery_engine import DiscoveryEngine

logger = logging.getLogger("bps.polymarket_engine")

def check_odds_shift(current: float, previous: float, threshold: float = 5.0) -> bool:
    delta_pct = abs(current - previous) * 100
    return delta_pct >= threshold

def format_polysignal(market_title: str, odds: float, delta: float,
                       correlated: list[dict[str, Any]], volume_24h: float) -> str:
    direction = "+" if delta > 0 else ""
    odds_pct = f"{odds * 100:.0f}%"
    delta_pct = f"{direction}{delta * 100:.0f}%"
    lines = [f'🚨 POLYSIGNAL: "{market_title}" odds → {odds_pct} ({delta_pct} / 7d)']
    for t in correlated:
        symbol = t.get("symbol", "???")
        rel = t.get("relationship", "related")
        emoji = "📈" if delta > 0 else "📉"
        lines.append(f"├─ {emoji} ${symbol} ({rel})")
    lines.append(f"└─ 📊 Polymarket volume: ${volume_24h:,.0f} (24h)")
    return "\n".join(lines)

# SPEC-028B §2 Source 4 — AI/tech keywords for new-market discovery
DISCOVERY_KEYWORDS: list[str] = [
    "AI", "artificial intelligence", "OpenAI", "Anthropic", "NVIDIA",
    "IPO", "large language model", "AGI",
]


class PolymarketEngine:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        discovery_engine_factory: Callable[[Session], DiscoveryEngine] | None = None,
    ) -> None:
        self.session_factory = session_factory
        self._poller = None
        # SPEC-028B Task 3: optional discovery engine factory
        self.discovery_engine_factory = discovery_engine_factory

    async def _get_poller(self):
        if self._poller is None:
            import sys
            sys.path.insert(0, "/home/nodeuser/blackpine_core")
            from intelligence.polymarket_poller import PolymarketPoller
            self._poller = PolymarketPoller()
        return self._poller

    async def scan(self) -> list[dict[str, Any]]:
        session = self.session_factory()
        try:
            correlations = session.query(PolymarketCorrelation).all()
            if not correlations:
                return []
            alerts = []
            poller = await self._get_poller()
            async with poller:
                for corr in correlations:
                    try:
                        markets = await poller.search_events(corr.market_title, limit=1)
                        if not markets:
                            continue
                        event = markets[0] if isinstance(markets, list) else markets
                        event_markets = event.get("markets", []) if isinstance(event, dict) else []
                        for mkt in event_markets:
                            prices_str = mkt.get("outcome_prices", mkt.get("outcomePrices", ""))
                            if not prices_str:
                                continue
                            try:
                                prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
                                current_odds = float(prices[0]) if prices else 0.0
                            except (json.JSONDecodeError, IndexError, TypeError):
                                continue
                            volume = float(mkt.get("volume", 0))
                            prev_signal = session.query(PolymarketSignal).filter_by(
                                market_id=corr.market_id).order_by(PolymarketSignal.snapshot_at.desc()).first()
                            prev_odds = prev_signal.odds if prev_signal else 0.0
                            delta = current_odds - prev_odds
                            triggered = check_odds_shift(current_odds, prev_odds)
                            sig = PolymarketSignal(correlation_id=corr.id, market_id=corr.market_id,
                                odds=current_odds, odds_delta=delta, volume_24h=volume,
                                triggered_alert=triggered, snapshot_at=datetime.now(timezone.utc))
                            session.add(sig)
                            if triggered:
                                alert_msg = format_polysignal(corr.market_title, current_odds, delta,
                                    corr.correlated_tickers, volume)
                                alerts.append({"market_id": corr.market_id, "message": alert_msg,
                                    "odds": current_odds, "delta": delta, "correlated_tickers": corr.correlated_tickers})
                    except Exception:
                        logger.exception("Failed to scan market %s", corr.market_id)
            session.commit()
            return alerts
        finally:
            session.close()

    # ------------------------------------------------------------------
    # SPEC-028B §2 Source 4 / Task 3
    # ------------------------------------------------------------------
    async def discover_new_markets(
        self, keywords: list[str] | None = None, limit_per_keyword: int = 10
    ) -> int:
        """Poll Polymarket for events matching AI/tech keywords; feed any
        entity references not already in the watchlist, IPO pipeline, or
        discovery candidates into the discovery engine.

        Returns the number of new candidates created.
        """
        if self.discovery_engine_factory is None:
            logger.info("polymarket discover_new_markets: no discovery engine wired, skipping")
            return 0
        kws = keywords or DISCOVERY_KEYWORDS
        session = self.session_factory()
        created = 0
        try:
            known_tickers = {row[0] for row in session.query(WatchlistTicker.symbol).all()}
            known_ipo = {row[0] for row in session.query(IPOPipeline.company_name).all()}
            known_cand = {row[0] for row in session.query(DiscoveryCandidate.entity_name).all()}
            known = known_tickers | known_ipo | known_cand

            poller = await self._get_poller()
            engine = self.discovery_engine_factory(session)
            async with poller:
                for kw in kws:
                    try:
                        events = await poller.search_events(kw, limit=limit_per_keyword)
                    except Exception:
                        logger.exception("polymarket search failed for keyword %s", kw)
                        continue
                    if not events:
                        continue
                    for event in events if isinstance(events, list) else [events]:
                        if not isinstance(event, dict):
                            continue
                        title = event.get("title") or event.get("question") or ""
                        market_id = str(event.get("id") or event.get("market_id") or "")
                        # Crude entity extraction — title itself is the candidate.
                        # Skip if entity is already known (ticker, ipo, candidate).
                        entity = title.strip()
                        if not entity or entity in known:
                            continue
                        try:
                            engine.ingest_polymarket_entity(
                                entity_name=entity,
                                market_title=title,
                                market_id=market_id,
                            )
                            known.add(entity)
                            created += 1
                        except Exception:
                            logger.exception(
                                "Discovery ingest failed for polymarket entity %s", entity
                            )
            session.commit()
            return created
        finally:
            session.close()
