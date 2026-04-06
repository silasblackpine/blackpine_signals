from blackpine_signals.ingestion.sentiment_aggregator import classify_alert_tier, fuse_signals, SignalInput

def test_classify_critical():
    assert classify_alert_tier(85.0) == "critical"

def test_classify_high():
    assert classify_alert_tier(55.0) == "high"

def test_classify_moderate():
    assert classify_alert_tier(30.0) == "moderate"

def test_classify_heatmap():
    assert classify_alert_tier(10.0) == "heatmap"

def test_fuse_signals_single():
    signals = [SignalInput(source="x", ticker="NVDA", score=50.0, signal_type="ipo_signal")]
    result = fuse_signals(signals)
    assert len(result) == 1
    assert result[0].ticker == "NVDA"
    assert result[0].fused_score == 50.0

def test_fuse_signals_same_ticker_merged():
    signals = [
        SignalInput(source="x", ticker="NVDA", score=40.0, signal_type="ipo_signal"),
        SignalInput(source="edgar", ticker="NVDA", score=80.0, signal_type="ipo_primary"),
        SignalInput(source="polymarket", ticker="NVDA", score=30.0, signal_type="odds_shift"),
    ]
    result = fuse_signals(signals)
    assert len(result) == 1
    assert result[0].ticker == "NVDA"
    assert result[0].fused_score == 150.0
    assert result[0].source_count == 3
    assert result[0].alert_tier == "critical"

def test_fuse_signals_different_tickers():
    signals = [
        SignalInput(source="x", ticker="NVDA", score=40.0, signal_type="ipo_signal"),
        SignalInput(source="x", ticker="GOOGL", score=30.0, signal_type="earnings"),
    ]
    result = fuse_signals(signals)
    assert len(result) == 2
