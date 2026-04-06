"""
SPEC-028B Discovery Engine — the brain of the Zero-Human Growth Protocol.

Ingests signals from the 5 discovery sources (X Watchdog, EDGAR Scanner,
Polymarket Engine, /last30days research, cross-signal fusion), aggregates
score per entity, runs the ZHGP state machine, and fires Discord alerts
when thresholds are crossed.

Governing docs:
  /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §3, §5, §8
  /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 2

Design notes:
- source_signals is stored as a list of dicts per SPEC-028B §4:
    [{"source": str, "timestamp": iso8601, "evidence": str, "score": float}, ...]
  Callers pass a single signal dict per call; engine appends and recomputes
  source_count as the number of unique `source` values.
- trigger_source is folded into DiscoveryLog.details (no schema change).
- Terminal states (auto_added, rejected, expired) are immutable — no
  re-promotion. auto_removal is NOT permitted per SPEC-028B §12.
- Thresholds are hard-coded constants per CLAUDE.md Hard Rule #5
  (no env-var override, prevents accidental permissiveness).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from sqlalchemy.orm import Session

from blackpine_signals.db.models import DiscoveryCandidate, DiscoveryLog

logger = logging.getLogger("bps.discovery_engine")

# SPEC-028B §5 thresholds — hard-coded, NOT env-configurable
AUTO_ADD_THRESHOLD: float = 80.0
PROPOSE_THRESHOLD: float = 40.0

# Terminal states — once a candidate reaches any of these it cannot be
# promoted further by the engine. Only human intervention can change them.
TERMINAL_STATES = frozenset({"auto_added", "rejected", "expired"})


class DiscoveryEngine:
    """Score aggregator + ZHGP state machine for discovery candidates."""

    def __init__(
        self,
        db_session: Session,
        webhook_url: Optional[str] = None,
        http_post=requests.post,  # injectable for tests
    ) -> None:
        self.db = db_session
        self.webhook_url = webhook_url if webhook_url is not None else os.getenv(
            "DISCORD_WEBHOOK_PRE_IPO"
        )
        self._http_post = http_post

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_signal(
        self,
        entity_name: str,
        entity_type: str,
        source: str,
        signal_score: float,
        details: Optional[dict[str, Any]] = None,
        evidence: str = "",
    ) -> DiscoveryCandidate:
        """Ingest one signal, update the candidate's score, evaluate ZHGP thresholds.

        Parameters
        ----------
        entity_name : str
            Canonical entity identifier (e.g. "CoreWeave", "Palantir Technologies").
            Callers should normalize before passing.
        entity_type : str
            One of: ticker, private_company, fund, unknown. Only applied on
            initial creation; subsequent calls respect existing entity_type.
        source : str
            Source slug (e.g. "x_watchdog", "edgar", "polymarket",
            "last30days", "fusion").
        signal_score : float
            Pre-computed base score from the SPEC-028B §5 scoring table.
            Callers own the scoring math; engine just aggregates.
        details : dict, optional
            Free-form metadata for the audit log.
        evidence : str
            Short human-readable evidence string (e.g. "@acc1 tweet 2026-04-06").
        """
        details = dict(details or {})

        candidate = (
            self.db.query(DiscoveryCandidate)
            .filter_by(entity_name=entity_name)
            .first()
        )
        if candidate is None:
            candidate = DiscoveryCandidate(
                entity_name=entity_name,
                entity_type=entity_type,
                discovery_score=0.0,
                source_signals=[],
                source_count=0,
                status="pending",
            )
            self.db.add(candidate)
            self.db.flush()  # assign id without committing

        # Append the signal dict per SPEC-028B §4 schema
        now = datetime.now(timezone.utc)
        signal_entry = {
            "source": source,
            "timestamp": now.isoformat(),
            "evidence": evidence,
            "score": float(signal_score),
        }
        updated_signals = list(candidate.source_signals or [])
        updated_signals.append(signal_entry)
        candidate.source_signals = updated_signals
        candidate.source_count = len({s.get("source") for s in updated_signals if s.get("source")})

        candidate.discovery_score = float(candidate.discovery_score or 0.0) + float(signal_score)
        candidate.last_seen = now

        self.db.commit()

        self._log_action(
            candidate,
            action="signal_ingested",
            trigger_source=source,
            details={**details, "signal_score": signal_score, "evidence": evidence},
        )

        self._evaluate_thresholds(candidate)
        return candidate

    # ------------------------------------------------------------------
    # State machine
    # ------------------------------------------------------------------

    def _evaluate_thresholds(self, candidate: DiscoveryCandidate) -> None:
        if candidate.status in TERMINAL_STATES:
            return

        if candidate.discovery_score >= AUTO_ADD_THRESHOLD:
            self._promote_candidate(candidate, "auto_added")
        elif (
            candidate.discovery_score >= PROPOSE_THRESHOLD
            and candidate.status != "proposed"
        ):
            self._promote_candidate(candidate, "proposed")

    def _promote_candidate(
        self, candidate: DiscoveryCandidate, new_status: str
    ) -> None:
        candidate.status = new_status
        candidate.action_taken_at = datetime.now(timezone.utc)
        if new_status == "auto_added":
            candidate.action_taken = "auto_added to watchlist/ipo_pipeline (ZHGP)"
        elif new_status == "proposed":
            candidate.action_taken = "proposed for review (ZHGP)"
        self.db.commit()

        self._log_action(
            candidate,
            action=f"promoted_to_{new_status}",
            trigger_source="zhgp_engine",
            details={
                "new_score": candidate.discovery_score,
                "source_count": candidate.source_count,
            },
        )
        self._dispatch_discord_alert(candidate, new_status)

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------

    def _log_action(
        self,
        candidate: DiscoveryCandidate,
        action: str,
        trigger_source: str,
        details: dict[str, Any],
    ) -> None:
        """Append an immutable audit row. trigger_source is folded into details
        (DiscoveryLog has no dedicated trigger_source column — see SPEC-028B §4)."""
        merged_details = {"trigger_source": trigger_source, **(details or {})}
        log_entry = DiscoveryLog(
            entity_name=candidate.entity_name,
            action=action,
            details=merged_details,
            automated=True,
        )
        self.db.add(log_entry)
        self.db.commit()

    # ------------------------------------------------------------------
    # Discord alert
    # ------------------------------------------------------------------

    def _dispatch_discord_alert(
        self, candidate: DiscoveryCandidate, status: str
    ) -> None:
        if not self.webhook_url:
            logger.warning(
                "DISCORD_WEBHOOK_PRE_IPO not set — skipping ZHGP alert for %s",
                candidate.entity_name,
            )
            return

        color = 5763719 if status == "auto_added" else 16705372  # green / yellow
        title = (
            f"[ZHGP] AUTO-ADD TRIGGERED: {candidate.entity_name}"
            if status == "auto_added"
            else f"[ZHGP] CANDIDATE PROPOSED: {candidate.entity_name}"
        )

        source_trail = ", ".join(
            sorted({s.get("source", "?") for s in (candidate.source_signals or [])})
        ) or "(none)"

        payload = {
            "embeds": [
                {
                    "title": title,
                    "color": color,
                    "fields": [
                        {
                            "name": "Discovery Score",
                            "value": f"{candidate.discovery_score:.1f}",
                            "inline": True,
                        },
                        {
                            "name": "Unique Sources",
                            "value": str(candidate.source_count),
                            "inline": True,
                        },
                        {
                            "name": "Source Trail",
                            "value": source_trail,
                            "inline": False,
                        },
                    ],
                    "footer": {
                        "text": (
                            f"Entity Type: {candidate.entity_type} | "
                            f"Black Pine Signals"
                        )
                    },
                }
            ]
        }

        try:
            self._http_post(self.webhook_url, json=payload, timeout=5)
        except requests.exceptions.RequestException as exc:
            logger.error(
                "Discord dispatch failed for %s: %s", candidate.entity_name, exc
            )
