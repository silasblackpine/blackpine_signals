"""
SPEC-028B Task 4 — Nightly Discovery Introspection (ZHGP §7).

Runs at 02:00 ET nightly. Reviews the discovery pipeline's last 24 hours,
applies recency decay to pending candidates, expires stale candidates,
and validates recent auto-adds against downstream signal coverage.

Governing docs:
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §7
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 4

Design notes:
- Applies a 10% per-day recency decay to `discovery_score` for candidates
  in `pending` or `proposed` status where `last_seen` is >24h old.
- Candidates whose `last_seen` is older than 30 days with no new signals
  transition to `expired` (terminal state).
- Auto-added candidates with zero follow-up signals after 48 hours are
  flagged via a `discovery_log` row with action `auto_add_stale_flag`.
  Per SPEC-028B §12, we do NOT auto-remove — only humans can.
- All writes are logged to `discovery_log` for audit.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Callable

from sqlalchemy.orm import Session

from blackpine_signals.db.models import DiscoveryCandidate, DiscoveryLog

logger = logging.getLogger("bps.discovery_introspection")

DECAY_RATE_PER_DAY: float = 0.10
EXPIRY_DAYS: int = 30
AUTO_ADD_STALE_HOURS: int = 48


class DiscoveryIntrospection:
    """Nightly hygiene job for the discovery pipeline."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def run(self, *, now: datetime | None = None) -> dict:
        """Execute one introspection pass. Returns a summary dict."""
        now_ts = now or datetime.now(timezone.utc)
        session = self.session_factory()
        summary = {
            "decayed": 0,
            "expired": 0,
            "auto_add_stale_flagged": 0,
            "total_reviewed": 0,
            "ran_at": now_ts.isoformat(),
        }
        try:
            pending_or_proposed = (
                session.query(DiscoveryCandidate)
                .filter(DiscoveryCandidate.status.in_(("pending", "proposed")))
                .all()
            )
            summary["total_reviewed"] = len(pending_or_proposed)

            expiry_cutoff = now_ts - timedelta(days=EXPIRY_DAYS)

            for cand in pending_or_proposed:
                last_seen = cand.last_seen
                if last_seen is None:
                    continue
                # Normalize tz
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)

                # Expire stale candidates
                if last_seen < expiry_cutoff:
                    cand.status = "expired"
                    cand.action_taken = "expired: no new signals in 30+ days"
                    cand.action_taken_at = now_ts
                    summary["expired"] += 1
                    self._log(
                        session,
                        entity_name=cand.entity_name,
                        action="expire",
                        details={
                            "trigger_source": "introspection",
                            "reason": "stale_30d",
                            "last_seen": last_seen.isoformat(),
                            "score_before_expiry": cand.discovery_score,
                        },
                    )
                    continue

                # Apply recency decay for candidates older than 24h
                days_stale = (now_ts - last_seen).total_seconds() / 86400.0
                if days_stale >= 1.0:
                    decay_factor = (1.0 - DECAY_RATE_PER_DAY) ** days_stale
                    new_score = float(cand.discovery_score) * decay_factor
                    if new_score < cand.discovery_score:
                        cand.discovery_score = new_score
                        summary["decayed"] += 1
                        self._log(
                            session,
                            entity_name=cand.entity_name,
                            action="score_decay",
                            details={
                                "trigger_source": "introspection",
                                "days_stale": days_stale,
                                "new_score": new_score,
                                "decay_factor": decay_factor,
                            },
                        )

            # Auto-add stale flag pass
            stale_cutoff = now_ts - timedelta(hours=AUTO_ADD_STALE_HOURS)
            recently_auto_added = (
                session.query(DiscoveryCandidate)
                .filter(DiscoveryCandidate.status == "auto_added")
                .filter(DiscoveryCandidate.action_taken_at < stale_cutoff)
                .all()
            )
            for cand in recently_auto_added:
                signals = cand.source_signals or []
                latest = max(
                    (s.get("timestamp", "") for s in signals),
                    default="",
                )
                if latest and latest < stale_cutoff.isoformat():
                    summary["auto_add_stale_flagged"] += 1
                    self._log(
                        session,
                        entity_name=cand.entity_name,
                        action="auto_add_stale_flag",
                        details={
                            "trigger_source": "introspection",
                            "note": "no new signals >48h post-auto-add",
                            "latest_signal_ts": latest,
                        },
                    )

            session.commit()
        except Exception:
            logger.exception("discovery_introspection: run failed")
            session.rollback()
            raise
        finally:
            session.close()

        logger.info(
            "discovery_introspection: reviewed=%d decayed=%d expired=%d stale_flagged=%d",
            summary["total_reviewed"],
            summary["decayed"],
            summary["expired"],
            summary["auto_add_stale_flagged"],
        )
        return summary

    def _log(
        self,
        session: Session,
        *,
        entity_name: str,
        action: str,
        details: dict,
    ) -> None:
        entry = DiscoveryLog(
            entity_name=entity_name,
            action=action,
            details=details,
            automated=True,
        )
        session.add(entry)
