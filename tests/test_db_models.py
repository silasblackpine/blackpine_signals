"""Tests for ORM models — validates table creation and basic CRUD."""
from datetime import date, datetime, timezone
from blackpine_signals.db.models import (
    FusedSignal, IPOPipeline, WatchlistTicker, XSignal,
)

def test_create_watchlist_ticker(db_session):
    t = WatchlistTicker(symbol="NVDA", name="NVIDIA Corporation", tier="mega_cap", sector="semiconductors")
    db_session.add(t)
    db_session.commit()
    result = db_session.query(WatchlistTicker).filter_by(symbol="NVDA").one()
    assert result.name == "NVIDIA Corporation"
    assert result.tier == "mega_cap"
    assert result.active is True

def test_create_ipo_pipeline_entry(db_session):
    entry = IPOPipeline(
        company_name="Anthropic PBC",
        stage="in_talks",
        estimated_valuation_low=400_000_000_000,
        estimated_valuation_high=500_000_000_000,
        lead_banks=["Goldman Sachs", "JPMorgan"],
        correlated_tickers=[{"symbol": "NVDA", "relationship": "investor"}],
        confidence_score=75,
    )
    db_session.add(entry)
    db_session.commit()
    result = db_session.query(IPOPipeline).filter_by(company_name="Anthropic PBC").one()
    assert result.stage == "in_talks"
    assert result.confidence_score == 75

def test_create_fused_signal(db_session):
    sig = FusedSignal(
        ticker="NVDA",
        signal_type="price_move",
        fused_score=85.0,
        source_count=3,
        sources={"x": 2, "edgar": 1},
        alert_tier="critical",
    )
    db_session.add(sig)
    db_session.commit()
    result = db_session.query(FusedSignal).first()
    assert result.fused_score == 85.0
    assert result.alert_tier == "critical"

def test_create_x_signal(db_session):
    sig = XSignal(
        source_handle="EricNewcomer",
        source_tier=1,
        ticker_refs=["NVDA", "GOOGL"],
        text="Anthropic in talks with Goldman for October IPO",
        engagement_score=5000.0,
        credibility_weighted_score=50000.0,
        sentiment="bullish",
        signal_type="ipo_rumor",
        posted_at=datetime.now(timezone.utc),
    )
    db_session.add(sig)
    db_session.commit()
    result = db_session.query(XSignal).first()
    assert result.source_tier == 1
    assert result.credibility_weighted_score == 50000.0
