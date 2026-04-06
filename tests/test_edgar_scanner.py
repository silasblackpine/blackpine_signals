from blackpine_signals.ingestion.edgar_scanner import classify_filing, is_target_entity, parse_edgar_rss_entry

def test_classify_s1():
    assert classify_filing("S-1") == "ipo_primary"
    assert classify_filing("S-1/A") == "ipo_amendment"
    assert classify_filing("F-1") == "ipo_primary"

def test_classify_13f():
    assert classify_filing("13-F") == "institutional_holdings"
    assert classify_filing("13F-HR") == "institutional_holdings"

def test_classify_form_d():
    assert classify_filing("D") == "exempt_offering"
    assert classify_filing("D/A") == "exempt_offering"

def test_classify_8k():
    assert classify_filing("8-K") == "material_event"

def test_classify_unknown():
    assert classify_filing("10-K") is None

def test_is_target_entity():
    targets = ["Anthropic", "OpenAI", "SpaceX"]
    assert is_target_entity("Anthropic PBC", targets) is True
    assert is_target_entity("ANTHROPIC, INC.", targets) is True
    assert is_target_entity("Random Corp", targets) is False

def test_parse_edgar_rss_entry():
    entry = {"title": "S-1 - Anthropic PBC (0001234567) (Filer)",
             "link": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001234567",
             "summary": "Filed 2026-04-04", "updated": "2026-04-04T12:00:00-04:00"}
    result = parse_edgar_rss_entry(entry)
    assert result is not None
    assert result["entity_name"] == "Anthropic PBC"
    assert result["form_type"] == "S-1"
