# /home/nodeuser/blackpine_core/blackpine_signals/ipo/pipeline.py
"""6-Stage IPO Pipeline State Machine. Governing doc: SPEC-028 §6"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from blackpine_signals.db.models import IPOPipeline

logger = logging.getLogger("bps.ipo_pipeline")
# NOTE: "priced" matches the DB CheckConstraint in IPOPipeline.ck_ipo_stage
IPO_STAGES = ["ghost", "whisper", "in_talks", "filed", "priced", "listed"]

def advance_stage(session: Session, company_name: str, target_stage: str, trigger_source: str, evidence: str) -> bool:
    if target_stage not in IPO_STAGES:
        return False
    entry = session.query(IPOPipeline).filter_by(company_name=company_name).first()
    if entry is None:
        entry = IPOPipeline(company_name=company_name, stage=target_stage, stage_history=[{
            "from_stage": None, "to_stage": target_stage, "trigger_source": trigger_source,
            "evidence": evidence, "timestamp": datetime.now(timezone.utc).isoformat(),
        }])
        session.add(entry)
        session.commit()
        return True
    current_idx = IPO_STAGES.index(entry.stage)
    target_idx = IPO_STAGES.index(target_stage)
    if target_idx <= current_idx:
        return False
    history = list(entry.stage_history or [])
    history.append({"from_stage": entry.stage, "to_stage": target_stage, "trigger_source": trigger_source,
                     "evidence": evidence, "timestamp": datetime.now(timezone.utc).isoformat()})
    entry.stage = target_stage
    entry.stage_history = history
    entry.updated_at = datetime.now(timezone.utc)
    session.commit()
    return True
