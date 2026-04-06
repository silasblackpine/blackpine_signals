# /home/nodeuser/blackpine_core/blackpine_signals/tests/test_x_watchdog.py
"""Tests for X Watchdog and credibility system."""
from blackpine_signals.models.credibility import classify_signal, compute_weighted_score, get_credibility_tier
from blackpine_signals.ingestion.x_watchdog import parse_grok_response

def test_credibility_tier_1():
    assert get_credibility_tier("EricNewcomer") == 1
    assert get_credibility_tier("@Techmeme") == 1

def test_credibility_tier_2():
    assert get_credibility_tier("NirantK") == 2
    assert get_credibility_tier("@cyrilXBT") == 2

def test_credibility_tier_4_unknown():
    assert get_credibility_tier("random_user_123") == 4

def test_weighted_score():
    assert compute_weighted_score(1000.0, 1) == 10000.0
    assert compute_weighted_score(1000.0, 4) == 1000.0

def test_classify_ipo_signal():
    assert classify_signal("Anthropic files S-1 for October IPO") == "ipo_signal"
    assert classify_signal("Lead banks Goldman and JPM hired") == "ipo_signal"

def test_classify_earnings():
    assert classify_signal("NVDA beat estimates with record revenue") == "earnings"

def test_classify_analyst():
    assert classify_signal("Analyst upgrades AVGO with $500 price target") == "analyst_action"

def test_classify_none():
    assert classify_signal("Just had a great coffee this morning") is None

def test_parse_grok_response():
    raw = {"choices": [{"message": {"content": '[{"handle": "EricNewcomer", "text": "Anthropic IPO in October", "likes": 5000, "reposts": 200, "url": "https://x.com/EricNewcomer/status/123"}]'}}]}
    posts = parse_grok_response(raw)
    assert len(posts) == 1
    assert posts[0]["handle"] == "EricNewcomer"
    assert posts[0]["likes"] == 5000
