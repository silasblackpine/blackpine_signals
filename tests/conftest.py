"""Shared test fixtures — in-memory SQLite for fast tests."""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from blackpine_signals.db.models import Base

@pytest.fixture
def db_session():
    """In-memory SQLite session with all tables created."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()
    engine.dispose()
