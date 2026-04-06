"""
Migration: add discovery_candidates + discovery_log tables (SPEC-028B Task 1).

Governing docs:
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §4
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 1

Idempotent — safe to re-run. Uses SQLAlchemy `create_all` restricted to the
two SPEC-028B tables so it does not touch existing Phase 1 schema.

Run:
    python -m blackpine_signals.db.migrations.add_discovery_tables

Or programmatically:
    from blackpine_signals.db.migrations import add_discovery_tables
    add_discovery_tables.run()
"""
from __future__ import annotations

import logging
import sys

from sqlalchemy.engine import Engine

from blackpine_signals.db.engine import engine as default_engine
from blackpine_signals.db.models import Base, DiscoveryCandidate, DiscoveryLog

logger = logging.getLogger("bps.migrations.add_discovery_tables")

DISCOVERY_TABLES = [
    DiscoveryCandidate.__table__,
    DiscoveryLog.__table__,
]


def run(engine: Engine | None = None) -> None:
    """Create discovery_candidates and discovery_log tables if not present.

    This is idempotent: SQLAlchemy's `create_all(checkfirst=True)` will skip
    tables that already exist. Safe to re-run on every deploy.
    """
    target_engine = engine or default_engine
    logger.info("Creating SPEC-028B discovery tables (idempotent)...")
    Base.metadata.create_all(bind=target_engine, tables=DISCOVERY_TABLES, checkfirst=True)
    logger.info("Done. Tables ensured: %s", [t.name for t in DISCOVERY_TABLES])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    try:
        run()
    except Exception as exc:  # noqa: BLE001
        logger.error("Migration failed: %s", exc)
        sys.exit(1)
