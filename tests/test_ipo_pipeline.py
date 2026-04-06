from blackpine_signals.db.models import IPOPipeline
from blackpine_signals.ipo.pipeline import advance_stage, IPO_STAGES

def test_ipo_stages_order():
    # "priced" matches the DB CheckConstraint (ck_ipo_stage uses 'priced', not 'pricing')
    assert IPO_STAGES == ["ghost", "whisper", "in_talks", "filed", "priced", "listed"]

def test_advance_stage_ghost_to_whisper(db_session):
    entry = IPOPipeline(company_name="TestCo", stage="ghost", confidence_score=10)
    db_session.add(entry)
    db_session.commit()
    advanced = advance_stage(db_session, "TestCo", "whisper", trigger_source="x_watchdog", evidence="3 Tier-1 mentions")
    assert advanced is True
    result = db_session.query(IPOPipeline).filter_by(company_name="TestCo").one()
    assert result.stage == "whisper"
    assert len(result.stage_history) == 1

def test_advance_stage_rejects_backward(db_session):
    entry = IPOPipeline(company_name="TestCo", stage="filed", confidence_score=80)
    db_session.add(entry)
    db_session.commit()
    advanced = advance_stage(db_session, "TestCo", "whisper", trigger_source="test", evidence="test")
    assert advanced is False
    assert db_session.query(IPOPipeline).filter_by(company_name="TestCo").one().stage == "filed"

def test_advance_stage_creates_new_entry(db_session):
    advanced = advance_stage(db_session, "NewCo", "ghost", trigger_source="edgar", evidence="Form D detected")
    assert advanced is True
    assert db_session.query(IPOPipeline).filter_by(company_name="NewCo").one().stage == "ghost"
