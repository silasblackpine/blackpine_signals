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

def test_parse_grok_response_legacy_chat_completions_shape():
    """Backward-compat: the old chat/completions shape still parses.

    SPEC-028C kept the legacy path so existing mocks/fixtures don't break.
    """
    raw = {
        "choices": [
            {
                "message": {
                    "content": '[{"handle": "EricNewcomer", "text": "Anthropic IPO in October", "likes": 5000, "reposts": 200, "url": "https://x.com/EricNewcomer/status/123"}]'
                }
            }
        ]
    }
    posts = parse_grok_response(raw)
    assert len(posts) == 1
    assert posts[0]["handle"] == "EricNewcomer"
    assert posts[0]["likes"] == 5000


def test_parse_grok_response_responses_api_shape():
    """SPEC-028C: new Responses API payload with tool_call + reasoning + message."""
    raw = {
        "id": "resp_abc",
        "output": [
            {
                "type": "custom_tool_call",
                "name": "x_keyword_search",
                "input": '{"query":"$NVDA within_time:6h","limit":"3"}',
                "status": "completed",
            },
            {"type": "reasoning", "summary": [], "status": "completed"},
            {
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [
                    {
                        "type": "output_text",
                        "text": '[{"handle":"thestockwhale","text":"$NVDA accumulation","likes":210,"reposts":20,"url":"https://x.com/thestockwhale/status/1","posted_at":"2026-04-07T12:20:17Z"}]',
                    }
                ],
            },
        ],
    }
    posts = parse_grok_response(raw)
    assert len(posts) == 1
    assert posts[0]["handle"] == "thestockwhale"
    assert posts[0]["likes"] == 210
    assert posts[0]["url"].startswith("https://x.com/")


def test_parse_grok_response_responses_api_empty():
    """Empty output_text array returns [] without error."""
    raw = {
        "output": [
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "[]"}],
            }
        ]
    }
    assert parse_grok_response(raw) == []


def test_parse_grok_response_malformed_returns_empty():
    """Missing output and choices → empty list, no exception."""
    assert parse_grok_response({}) == []
    assert parse_grok_response({"output": []}) == []
    assert parse_grok_response({"output": [{"type": "reasoning"}]}) == []


def test_parse_grok_response_non_list_payload_returns_empty():
    """If the text is valid JSON but not an array, return []."""
    raw = {
        "output": [
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": '{"error":"no data"}'}],
            }
        ]
    }
    assert parse_grok_response(raw) == []


def test_parse_grok_response_strips_markdown_fences():
    """Grok sometimes wraps output in ```json fences; we must strip them."""
    raw = {
        "output": [
            {
                "type": "message",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": '```json\n[{"handle":"x","text":"t","likes":1,"reposts":0,"url":"https://x.com/x/status/1","posted_at":"2026-04-07T00:00:00Z"}]\n```',
                    }
                ],
            }
        ]
    }
    posts = parse_grok_response(raw)
    assert len(posts) == 1
    assert posts[0]["handle"] == "x"
