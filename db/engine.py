"""
SQLAlchemy engine and session factory.
Governing doc: /home/nodeuser/documents/2026-04-04-spec-028-black-pine-signals.md
"""
from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from blackpine_signals.config import settings

engine = create_engine(settings.database_url, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)

def get_session() -> Session:
    """Return a new database session. Caller must close."""
    return SessionLocal()
