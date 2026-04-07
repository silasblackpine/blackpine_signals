"""
Tests for SPEC-028B Task 4 — Nightly Discovery Introspection.

Governing docs:
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §7
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 4
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import sessionmaker

from blackpine_signals.db.models import DiscoveryCandidate, DiscoveryLog
from blackpine_signals.ingestion.discovery_introspection import (
    AUTO_ADD_STALE_HOURS,
    DECAY_RATE_PER_DAY,
    EXPIRY_DAYS,
    DiscoveryIntrospection,
)


def _make_candidate(session, **overrides):
    defaults = dict(
        entity_name=f"Entity-{id(overrides)}",
        entity_type="ticker",
        discovery_score=50.0,
        source_signals=[],
        source_count=1,
        status="pending",
        first_seen=datetime.now(timezone.utc),
        last_seen=datetime.now(timezone.utc),
    )
    defaults.update(overrides)
    cand = DiscoveryCandidate(**defaults)
    session.add(cand)
    session.commit()
    return cand


def _make_factory(db_session):
    """Return a session_factory that yields fresh sessions bound to the same
    engine as the test's db_session. The introspection will close its own
    session without killing the test's session or detaching test-scoped
    candidate instances."""
    return sessionmaker(bind=db_session.get_bind())


def _requery(db_session, entity_name):
    """Expire the test session and re-fetch a candidate by name so we see
    the state the introspection wrote via its own fresh session."""
    db_session.expire_all()
    return (
        db_session.query(DiscoveryCandidate)
        .filter_by(entity_name=entity_name)
        .one()
    )


def test_introspection_decays_stale_candidate(db_session):
    now = datetime(2026, 4, 7, 2, 0, 0, tzinfo=timezone.utc)
    two_days_ago = now - timedelta(days=2)
    _make_candidate(
        db_session,
        entity_name="StaleCo",
        discovery_score=50.0,
        last_seen=two_days_ago,
    )

    intro = DiscoveryIntrospection(session_factory=_make_factory(db_session))
    summary = intro.run(now=now)

    cand = _requery(db_session, "StaleCo")
    # After 2 days at 10% decay: 50 * 0.9^2 = 40.5
    assert abs(cand.discovery_score - (50.0 * (1.0 - DECAY_RATE_PER_DAY) ** 2)) < 0.01
    assert summary["decayed"] >= 1


def test_introspection_skips_fresh_candidate(db_session):
    now = datetime(2026, 4, 7, 2, 0, 0, tzinfo=timezone.utc)
    _make_candidate(
        db_session,
        entity_name="FreshCo",
        discovery_score=50.0,
        last_seen=now - timedelta(hours=6),  # less than 24h
    )
    intro = DiscoveryIntrospection(session_factory=_make_factory(db_session))
    intro.run(now=now)
    cand = _requery(db_session, "FreshCo")
    assert cand.discovery_score == 50.0


def test_introspection_expires_old_candidate(db_session):
    now = datetime(2026, 4, 7, 2, 0, 0, tzinfo=timezone.utc)
    very_old = now - timedelta(days=EXPIRY_DAYS + 1)
    _make_candidate(
        db_session,
        entity_name="AncientCo",
        discovery_score=50.0,
        last_seen=very_old,
    )

    intro = DiscoveryIntrospection(session_factory=_make_factory(db_session))
    summary = intro.run(now=now)

    cand = _requery(db_session, "AncientCo")
    assert cand.status == "expired"
    assert cand.action_taken is not None
    assert "30" in cand.action_taken
    assert summary["expired"] == 1

    log = (
        db_session.query(DiscoveryLog)
        .filter_by(entity_name="AncientCo", action="expire")
        .first()
    )
    assert log is not None


def test_introspection_flags_stale_auto_add(db_session):
    now = datetime(2026, 4, 7, 2, 0, 0, tzinfo=timezone.utc)
    old_auto_add_ts = now - timedelta(hours=AUTO_ADD_STALE_HOURS + 5)
    _make_candidate(
        db_session,
        entity_name="GhostedCo",
        status="auto_added",
        source_signals=[
            {"source": "x_watchdog", "timestamp": old_auto_add_ts.isoformat(), "evidence": "x", "score": 20.0}
        ],
        action_taken_at=old_auto_add_ts,
    )

    intro = DiscoveryIntrospection(session_factory=_make_factory(db_session))
    summary = intro.run(now=now)

    assert summary["auto_add_stale_flagged"] == 1
    log = (
        db_session.query(DiscoveryLog)
        .filter_by(entity_name="GhostedCo", action="auto_add_stale_flag")
        .first()
    )
    assert log is not None
    # Per SPEC-028B §12: no auto-removal. Status must stay auto_added.
    cand = _requery(db_session, "GhostedCo")
    assert cand.status == "auto_added"


def test_introspection_returns_summary_dict(db_session):
    now = datetime(2026, 4, 7, 2, 0, 0, tzinfo=timezone.utc)
    intro = DiscoveryIntrospection(session_factory=_make_factory(db_session))
    summary = intro.run(now=now)
    assert set(summary.keys()) >= {
        "decayed",
        "expired",
        "auto_add_stale_flagged",
        "total_reviewed",
        "ran_at",
    }
    assert summary["ran_at"] == now.isoformat()


def test_introspection_does_not_decay_terminal_states(db_session):
    now = datetime(2026, 4, 7, 2, 0, 0, tzinfo=timezone.utc)
    two_days_ago = now - timedelta(days=2)
    _make_candidate(
        db_session,
        entity_name="RejectedCo",
        discovery_score=50.0,
        status="rejected",
        last_seen=two_days_ago,
    )
    intro = DiscoveryIntrospection(session_factory=_make_factory(db_session))
    intro.run(now=now)
    cand = _requery(db_session, "RejectedCo")
    # Terminal-state candidates are not in the pending/proposed query
    assert cand.discovery_score == 50.0
