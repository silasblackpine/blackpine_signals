# /home/nodeuser/blackpine_core/blackpine_signals/ingestion/edgar_scanner.py
"""Layer 3: EDGAR Scanner — SEC filing detection. Governing doc: SPEC-028 §4.3"""
from __future__ import annotations
import logging
import re
from datetime import datetime, timezone
from typing import Any, Callable
import feedparser
from sqlalchemy.orm import Session
from blackpine_signals.db.models import EdgarFiling
from blackpine_signals.ipo.pipeline import advance_stage

logger = logging.getLogger("bps.edgar_scanner")
EDGAR_RSS_FEED = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=S-1&dateb=&owner=include&count=40&search_text=&action=getcompany&output=atom"

FORM_CLASSIFICATIONS = {
    "S-1": "ipo_primary", "S-1/A": "ipo_amendment", "F-1": "ipo_primary", "F-1/A": "ipo_amendment",
    "13-F": "institutional_holdings", "13F-HR": "institutional_holdings", "13F-HR/A": "institutional_holdings",
    "D": "exempt_offering", "D/A": "exempt_offering", "8-K": "material_event", "8-K/A": "material_event",
}
# "pricing" -> "priced" to match DB CheckConstraint in IPOPipeline.ck_ipo_stage
FORM_TO_IPO_STAGE = {"ipo_primary": "filed", "ipo_amendment": "priced", "institutional_holdings": "ghost", "exempt_offering": "ghost"}
TITLE_PATTERN = re.compile(r"^([\w\-/]+)\s*-\s*(.+?)\s*\(\d+\)")

def classify_filing(form_type: str) -> str | None:
    return FORM_CLASSIFICATIONS.get(form_type)

def is_target_entity(entity_name: str, targets: list[str]) -> bool:
    name_lower = entity_name.lower()
    return any(t.lower() in name_lower for t in targets)

def parse_edgar_rss_entry(entry: dict[str, Any]) -> dict[str, Any] | None:
    title = entry.get("title", "")
    match = TITLE_PATTERN.match(title)
    if not match:
        return None
    return {"entity_name": match.group(2).strip(), "form_type": match.group(1).strip(),
            "filing_url": entry.get("link", ""), "summary": entry.get("summary", ""),
            "filed_date": entry.get("updated", "")}

class EdgarScanner:
    def __init__(self, session_factory: Callable[[], Session], targets: list[str] | None = None) -> None:
        self.session_factory = session_factory
        self.targets = targets or ["Anthropic", "OpenAI", "xAI", "SpaceX", "Groq", "CoreWeave", "Databricks", "Stripe", "Canva", "Figma"]

    async def scan(self) -> int:
        feed = feedparser.parse(EDGAR_RSS_FEED)
        if not feed.entries:
            return 0
        session = self.session_factory()
        try:
            inserted = 0
            for entry in feed.entries:
                parsed = parse_edgar_rss_entry(entry)
                if not parsed:
                    continue
                if session.query(EdgarFiling).filter_by(filing_url=parsed["filing_url"]).first():
                    continue
                classification = classify_filing(parsed["form_type"])
                is_target = is_target_entity(parsed["entity_name"], self.targets)
                is_ipo = classification in ("ipo_primary", "ipo_amendment")
                try:
                    filed_date = datetime.fromisoformat(parsed["filed_date"]).date()
                except (ValueError, TypeError):
                    filed_date = datetime.now(timezone.utc).date()
                filing = EdgarFiling(entity_name=parsed["entity_name"], form_type=parsed["form_type"],
                    filed_date=filed_date, filing_url=parsed["filing_url"],
                    summary=parsed["summary"][:500] if parsed["summary"] else None,
                    is_ipo_signal=is_ipo and is_target)
                session.add(filing)
                inserted += 1
                if is_target and classification in FORM_TO_IPO_STAGE:
                    advance_stage(session, parsed["entity_name"], FORM_TO_IPO_STAGE[classification],
                        trigger_source="edgar_scanner", evidence=f"{parsed['form_type']} filing: {parsed['filing_url']}")
            session.commit()
            return inserted
        finally:
            session.close()
