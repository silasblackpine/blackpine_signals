from datetime import datetime, timezone
from blackpine_signals.db.models import PolymarketCorrelation, PolymarketSignal
from blackpine_signals.ingestion.polymarket_engine import check_odds_shift, format_polysignal

def test_check_odds_shift_triggers():
    assert check_odds_shift(0.68, 0.56, threshold=5.0) is True

def test_check_odds_shift_no_trigger():
    assert check_odds_shift(0.68, 0.65, threshold=5.0) is False

def test_format_polysignal():
    msg = format_polysignal("Will Anthropic IPO in 2026?", 0.68, 0.12,
        [{"symbol": "NVDA", "relationship": "investor", "weight": 0.8},
         {"symbol": "GOOGL", "relationship": "investor", "weight": 0.7}], 4_200_000.0)
    assert "Anthropic IPO" in msg
    assert "68%" in msg
    assert "+12%" in msg
    assert "NVDA" in msg
    assert "GOOGL" in msg

def test_store_polymarket_signal(db_session):
    corr = PolymarketCorrelation(market_id="anthropic-ipo-2026", market_title="Will Anthropic IPO in 2026?",
        correlated_tickers=[{"symbol": "NVDA", "relationship": "investor"}], correlation_type="ipo_trigger")
    db_session.add(corr)
    db_session.commit()
    sig = PolymarketSignal(correlation_id=corr.id, market_id="anthropic-ipo-2026",
        odds=0.68, odds_delta=0.12, volume_24h=4200000.0, triggered_alert=True,
        snapshot_at=datetime.now(timezone.utc))
    db_session.add(sig)
    db_session.commit()
    result = db_session.query(PolymarketSignal).first()
    assert result.odds == 0.68
    assert result.triggered_alert is True
