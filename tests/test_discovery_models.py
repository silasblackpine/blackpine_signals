"""
Tests for SPEC-028B discovery ORM models (Task 1).

Governing docs:
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §4
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 1

Covers:
- Table creation via conftest in-memory SQLite
- DiscoveryCandidate: unique entity_name, CHECK on entity_type + status,
  JSON round-trip on source_signals, defaults, indexes
- DiscoveryLog: timestamp default, JSON details round-trip, automated default
- Migration script idempotency
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError

from blackpine_signals.db.models import Base, DiscoveryCandidate, DiscoveryLog


# ---------------------------------------------------------------------------
# DiscoveryCandidate
# ---------------------------------------------------------------------------

def test_create_discovery_candidate_minimal(db_session):
    cand = DiscoveryCandidate(entity_name="CoreWeave", entity_type="private_company")
    db_session.add(cand)
    db_session.commit()
    result = db_session.query(DiscoveryCandidate).filter_by(entity_name="CoreWeave").one()
    assert result.entity_type == "private_company"
    assert result.status == "pending"
    assert result.discovery_score == 0.0
    assert result.source_count == 1
    assert result.source_signals == []
    assert result.first_seen is not None
    assert result.last_seen is not None


def test_discovery_candidate_full_payload(db_session):
    signals = [
        {"source": "x_watchdog", "timestamp": "2026-04-06T10:00:00Z", "evidence": "@acc1", "score": 20},
        {"source": "edgar", "timestamp": "2026-04-06T11:00:00Z", "evidence": "13-F", "score": 30},
    ]
    cand = DiscoveryCandidate(
        entity_name="Databricks",
        entity_type="private_company",
        discovery_score=92.0,
        source_signals=signals,
        source_count=2,
        status="auto_added",
        action_taken="added to ipo_pipeline at Whisper stage",
        notes="triple-source convergence",
    )
    db_session.add(cand)
    db_session.commit()

    got = db_session.query(DiscoveryCandidate).filter_by(entity_name="Databricks").one()
    assert got.discovery_score == 92.0
    assert got.source_count == 2
    assert got.status == "auto_added"
    assert got.source_signals == signals  # JSON round-trip
    assert got.source_signals[0]["source"] == "x_watchdog"


def test_discovery_candidate_unique_entity_name(db_session):
    db_session.add(DiscoveryCandidate(entity_name="Cohere", entity_type="private_company"))
    db_session.commit()
    db_session.add(DiscoveryCandidate(entity_name="Cohere", entity_type="private_company"))
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_discovery_candidate_entity_type_check(db_session):
    bad = DiscoveryCandidate(entity_name="Mystery Co", entity_type="not_a_real_type")
    db_session.add(bad)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_discovery_candidate_status_check(db_session):
    bad = DiscoveryCandidate(entity_name="XCorp", entity_type="ticker", status="bogus")
    db_session.add(bad)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_discovery_candidate_proposed_ticker_indexable(db_session):
    db_session.add(
        DiscoveryCandidate(
            entity_name="Palantir Technologies",
            entity_type="ticker",
            proposed_ticker="PLTR",
            discovery_score=85.0,
        )
    )
    db_session.commit()
    got = db_session.query(DiscoveryCandidate).filter_by(proposed_ticker="PLTR").one()
    assert got.entity_name == "Palantir Technologies"


# ---------------------------------------------------------------------------
# DiscoveryLog
# ---------------------------------------------------------------------------

def test_create_discovery_log_default_automated(db_session):
    entry = DiscoveryLog(
        action="auto_add_ticker",
        entity_name="PLTR",
        details={"score": 85, "sources": ["x", "edgar"]},
    )
    db_session.add(entry)
    db_session.commit()
    got = db_session.query(DiscoveryLog).filter_by(entity_name="PLTR").one()
    assert got.action == "auto_add_ticker"
    assert got.automated is True
    assert got.details == {"score": 85, "sources": ["x", "edgar"]}
    assert got.timestamp is not None


def test_discovery_log_human_action(db_session):
    entry = DiscoveryLog(
        action="reject",
        entity_name="FakeCo",
        details={"reason": "human override"},
        automated=False,
    )
    db_session.add(entry)
    db_session.commit()
    got = db_session.query(DiscoveryLog).filter_by(entity_name="FakeCo").one()
    assert got.automated is False


def test_discovery_log_multiple_entries_same_entity(db_session):
    """Log is append-only; same entity can appear many times."""
    for action in ("propose", "auto_add_ticker", "expire"):
        db_session.add(
            DiscoveryLog(action=action, entity_name="ACME", details={"step": action})
        )
    db_session.commit()
    got = db_session.query(DiscoveryLog).filter_by(entity_name="ACME").all()
    assert len(got) == 3
    assert {e.action for e in got} == {"propose", "auto_add_ticker", "expire"}


# ---------------------------------------------------------------------------
# Schema / inspector assertions
# ---------------------------------------------------------------------------

def test_discovery_tables_exist_in_metadata():
    assert "discovery_candidates" in Base.metadata.tables
    assert "discovery_log" in Base.metadata.tables


def test_discovery_candidate_indexes_present(db_session):
    insp = inspect(db_session.get_bind())
    idx_cols = {
        tuple(idx["column_names"])
        for idx in insp.get_indexes("discovery_candidates")
    }
    # entity_name, proposed_ticker, discovery_score, status should each be indexed
    flat = {c for cols in idx_cols for c in cols}
    assert "entity_name" in flat
    assert "proposed_ticker" in flat
    assert "discovery_score" in flat
    assert "status" in flat


def test_discovery_log_indexes_present(db_session):
    insp = inspect(db_session.get_bind())
    idx_cols = {
        tuple(idx["column_names"])
        for idx in insp.get_indexes("discovery_log")
    }
    flat = {c for cols in idx_cols for c in cols}
    assert "timestamp" in flat
    assert "action" in flat
    assert "entity_name" in flat


# ---------------------------------------------------------------------------
# Migration script idempotency
# ---------------------------------------------------------------------------

def test_migration_script_is_idempotent():
    """Running the migration twice on a fresh engine must not raise."""
    from blackpine_signals.db.migrations import add_discovery_tables as migration

    engine = create_engine("sqlite:///:memory:")
    # First run: creates tables
    migration.run(engine=engine)
    # Second run: must be no-op, not raise
    migration.run(engine=engine)
    insp = inspect(engine)
    assert "discovery_candidates" in insp.get_table_names()
    assert "discovery_log" in insp.get_table_names()
    engine.dispose()
