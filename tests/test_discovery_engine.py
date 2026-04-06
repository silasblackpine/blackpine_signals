"""
Tests for SPEC-028B Discovery Engine (Task 2).

Governing docs:
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §3, §5, §8
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 2

Covers:
- First-signal candidate creation
- Score aggregation across multiple signals
- Dict-shaped source_signals (SPEC-028B §4 schema)
- source_count = unique source types
- Propose threshold (≥40)
- Auto-add threshold (≥80)
- Terminal state immutability (auto_added cannot be re-promoted)
- DiscoveryLog audit rows (signal_ingested + promoted_to_*)
- trigger_source folded into details
- Discord webhook dispatch (mocked) — payload shape + color + title
- Webhook missing → no crash, warning only
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from blackpine_signals.db.models import DiscoveryCandidate, DiscoveryLog
from blackpine_signals.ingestion.discovery_engine import (
    AUTO_ADD_THRESHOLD,
    PROPOSE_THRESHOLD,
    DiscoveryEngine,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_post():
    return MagicMock(return_value=MagicMock(status_code=204))


@pytest.fixture
def engine(db_session, mock_post):
    return DiscoveryEngine(
        db_session=db_session,
        webhook_url="https://discord.test/webhook/pre-ipo",
        http_post=mock_post,
    )


@pytest.fixture
def engine_no_webhook(db_session, mock_post):
    return DiscoveryEngine(
        db_session=db_session,
        webhook_url="",  # explicitly empty — skip alerts
        http_post=mock_post,
    )


# ---------------------------------------------------------------------------
# Candidate creation & score aggregation
# ---------------------------------------------------------------------------

def test_first_signal_creates_candidate(engine, db_session):
    cand = engine.process_signal(
        entity_name="CoreWeave",
        entity_type="private_company",
        source="x_watchdog",
        signal_score=20.0,
        evidence="@acc1",
    )
    assert cand.id is not None
    assert cand.entity_name == "CoreWeave"
    assert cand.entity_type == "private_company"
    assert cand.discovery_score == 20.0
    assert cand.status == "pending"
    assert cand.source_count == 1
    assert len(cand.source_signals) == 1
    entry = cand.source_signals[0]
    assert entry["source"] == "x_watchdog"
    assert entry["score"] == 20.0
    assert entry["evidence"] == "@acc1"
    assert "timestamp" in entry


def test_multiple_signals_aggregate_score(engine):
    engine.process_signal("Cohere", "private_company", "x_watchdog", 20.0)
    engine.process_signal("Cohere", "private_company", "x_watchdog", 10.0)
    cand = engine.process_signal(
        "Cohere", "private_company", "edgar", 15.0
    )
    assert cand.discovery_score == 45.0
    # Two distinct sources: x_watchdog, edgar
    assert cand.source_count == 2
    assert len(cand.source_signals) == 3
    sources = {s["source"] for s in cand.source_signals}
    assert sources == {"x_watchdog", "edgar"}


def test_source_count_is_unique_not_cumulative(engine):
    for _ in range(5):
        engine.process_signal("X.AI", "private_company", "x_watchdog", 5.0)
    cand = engine.process_signal("X.AI", "private_company", "x_watchdog", 5.0)
    assert cand.source_count == 1  # still just one source type
    assert len(cand.source_signals) == 6  # but six signal entries


# ---------------------------------------------------------------------------
# ZHGP state machine thresholds
# ---------------------------------------------------------------------------

def test_pending_stays_pending_below_propose(engine, mock_post):
    cand = engine.process_signal("Tiny", "private_company", "x_watchdog", 30.0)
    assert cand.discovery_score == 30.0
    assert cand.status == "pending"
    mock_post.assert_not_called()


def test_propose_threshold_triggers_propose_alert(engine, mock_post):
    cand = engine.process_signal(
        "MidScore Co", "private_company", "edgar", PROPOSE_THRESHOLD
    )
    assert cand.status == "proposed"
    assert cand.action_taken_at is not None
    assert cand.action_taken and "proposed" in cand.action_taken
    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    embed = kwargs["json"]["embeds"][0]
    assert "PROPOSED" in embed["title"]
    assert embed["color"] == 16705372  # yellow


def test_auto_add_threshold_triggers_auto_add_alert(engine, mock_post):
    cand = engine.process_signal(
        "HotTake", "private_company", "fusion", AUTO_ADD_THRESHOLD
    )
    assert cand.status == "auto_added"
    assert cand.action_taken and "auto_added" in cand.action_taken
    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    embed = kwargs["json"]["embeds"][0]
    assert "AUTO-ADD" in embed["title"]
    assert embed["color"] == 5763719  # green


def test_upgrade_from_proposed_to_auto_added(engine, mock_post):
    # First signal: crosses propose threshold
    engine.process_signal("Upgrader", "private_company", "edgar", 50.0)
    # Second signal: pushes over auto-add threshold
    cand = engine.process_signal("Upgrader", "private_company", "x_watchdog", 35.0)
    assert cand.discovery_score == 85.0
    assert cand.status == "auto_added"
    # Two alerts: one for propose, one for auto_add
    assert mock_post.call_count == 2


def test_proposed_state_does_not_re_alert_on_same_tier(engine, mock_post):
    # First signal: propose (score 50)
    engine.process_signal("Plateau", "private_company", "edgar", 50.0)
    # Second signal: still under auto-add (score 60), already proposed
    cand = engine.process_signal("Plateau", "private_company", "edgar", 10.0)
    assert cand.status == "proposed"
    # Only one alert — no duplicate propose alert
    assert mock_post.call_count == 1


def test_terminal_auto_added_is_immutable(engine, mock_post):
    # Push to auto_added
    engine.process_signal("Terminal", "ticker", "fusion", 90.0)
    assert mock_post.call_count == 1
    # Further signals must not re-fire an alert, must not change status
    cand = engine.process_signal("Terminal", "ticker", "x_watchdog", 50.0)
    assert cand.status == "auto_added"
    assert mock_post.call_count == 1  # no new alert


def test_terminal_rejected_is_immutable(engine, db_session, mock_post):
    engine.process_signal("Rejected Co", "private_company", "x_watchdog", 10.0)
    cand = db_session.query(DiscoveryCandidate).filter_by(
        entity_name="Rejected Co"
    ).one()
    cand.status = "rejected"
    db_session.commit()
    # A huge new signal must not resurrect it
    engine.process_signal("Rejected Co", "private_company", "fusion", 100.0)
    cand_after = db_session.query(DiscoveryCandidate).filter_by(
        entity_name="Rejected Co"
    ).one()
    assert cand_after.status == "rejected"
    mock_post.assert_not_called()


# ---------------------------------------------------------------------------
# Audit log (DiscoveryLog)
# ---------------------------------------------------------------------------

def test_signal_ingested_writes_audit_row(engine, db_session):
    engine.process_signal(
        "Auditee", "private_company", "x_watchdog", 10.0, evidence="tweet-1"
    )
    logs = db_session.query(DiscoveryLog).filter_by(entity_name="Auditee").all()
    # At least one signal_ingested log (below thresholds → no promotion log)
    ingests = [l for l in logs if l.action == "signal_ingested"]
    assert len(ingests) == 1
    assert ingests[0].automated is True
    assert ingests[0].details["trigger_source"] == "x_watchdog"
    assert ingests[0].details["signal_score"] == 10.0
    assert ingests[0].details["evidence"] == "tweet-1"


def test_promotion_writes_promoted_log(engine, db_session):
    engine.process_signal("Promoted", "private_company", "fusion", AUTO_ADD_THRESHOLD)
    logs = db_session.query(DiscoveryLog).filter_by(entity_name="Promoted").all()
    actions = {l.action for l in logs}
    assert "signal_ingested" in actions
    assert "promoted_to_auto_added" in actions
    promo = [l for l in logs if l.action == "promoted_to_auto_added"][0]
    assert promo.details["trigger_source"] == "zhgp_engine"
    assert promo.details["new_score"] == AUTO_ADD_THRESHOLD


# ---------------------------------------------------------------------------
# Webhook absence graceful handling
# ---------------------------------------------------------------------------

def test_missing_webhook_does_not_crash(engine_no_webhook, mock_post):
    cand = engine_no_webhook.process_signal(
        "NoAlert", "private_company", "fusion", 90.0
    )
    assert cand.status == "auto_added"
    mock_post.assert_not_called()


def test_webhook_http_failure_does_not_crash(db_session):
    import requests as real_requests

    def boom(*_a, **_kw):
        raise real_requests.exceptions.ConnectionError("boom")

    eng = DiscoveryEngine(
        db_session=db_session,
        webhook_url="https://discord.test/webhook/pre-ipo",
        http_post=boom,
    )
    cand = eng.process_signal("Resilient", "ticker", "fusion", 90.0)
    assert cand.status == "auto_added"  # state still committed
