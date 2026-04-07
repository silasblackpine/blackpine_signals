"""
Black Pine Signals — Daemon Entrypoint
Governing doc: /home/nodeuser/documents/2026-04-04-spec-028-black-pine-signals.md

Runs FastAPI (port 8003, 127.0.0.1) with APScheduler for ingestion jobs.
Wires: PriceEngine, XWatchdog, EdgarScanner, PolymarketEngine, Stripe billing.
"""

from __future__ import annotations

import logging
from typing import Any

import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, HTTPException, Request, Response

from blackpine_signals.billing.stripe_handler import handle_stripe_event
from blackpine_signals.config import settings
from blackpine_signals.db.engine import engine, get_session
from blackpine_signals.db.models import Base, FusedSignal, IPOPipeline, WatchlistTicker
from blackpine_signals.db.seed import seed_database
from blackpine_signals.ingestion.discovery_engine import DiscoveryEngine
from blackpine_signals.ingestion.discovery_introspection import DiscoveryIntrospection
from blackpine_signals.ingestion.discovery_research import (
    DiscoveryResearchRunner,
    resolve_last30days_script,
    topics_for_day,
)
from blackpine_signals.ingestion.edgar_scanner import EdgarScanner
from blackpine_signals.ingestion.polymarket_engine import PolymarketEngine
from blackpine_signals.ingestion.price_engine import PriceEngine
from blackpine_signals.ingestion.symbol_validator import SymbolValidator
from blackpine_signals.ingestion.x_watchdog import XWatchdog

logger = logging.getLogger("bps")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

app = FastAPI(title="Black Pine Signals", version="0.1.0")
scheduler = AsyncIOScheduler(timezone="America/New_York")

# Engine instances — populated at startup
price_engine: PriceEngine | None = None
x_watchdog: XWatchdog | None = None
edgar_scanner: EdgarScanner | None = None
polymarket_engine: PolymarketEngine | None = None
# SPEC-028B Task 4
research_runner: DiscoveryResearchRunner | None = None
introspection: DiscoveryIntrospection | None = None


# ---------------------------------------------------------------------------
# Scheduler job wrappers
# ---------------------------------------------------------------------------
async def _job_price_poll() -> None:
    if price_engine:
        try:
            count = await price_engine.poll_all()
            logger.info("[job:price_poll] inserted=%d", count)
        except Exception:
            logger.exception("[job:price_poll] failed")


async def _job_x_scan() -> None:
    if x_watchdog:
        try:
            count = await x_watchdog.scan()
            logger.info("[job:x_scan] inserted=%d", count)
        except Exception:
            logger.exception("[job:x_scan] failed")


async def _job_edgar_scan() -> None:
    if edgar_scanner:
        try:
            count = await edgar_scanner.scan()
            logger.info("[job:edgar_scan] inserted=%d", count)
        except Exception:
            logger.exception("[job:edgar_scan] failed")


async def _job_polymarket_scan() -> None:
    if polymarket_engine:
        try:
            alerts = await polymarket_engine.scan()
            logger.info("[job:polymarket_scan] alerts=%d", len(alerts))
        except Exception:
            logger.exception("[job:polymarket_scan] failed")


# SPEC-028B Task 4 — discovery research + introspection jobs
async def _job_discovery_research_slot(slot_idx: int) -> None:
    """Run one of the 4 daily research topics (slot_idx in 0..3)."""
    if research_runner is None:
        return
    topics = topics_for_day()
    if slot_idx >= len(topics):
        logger.warning("[job:discovery_research] slot %d out of range", slot_idx)
        return
    topic = topics[slot_idx]
    try:
        summary = await research_runner.run_topic(topic)
        logger.info("[job:discovery_research] slot=%d %s", slot_idx, summary)
    except Exception:
        logger.exception("[job:discovery_research] slot=%d topic=%r failed", slot_idx, topic)


def _job_discovery_introspection() -> None:
    """Nightly 2 AM ET introspection pass."""
    if introspection is None:
        return
    try:
        summary = introspection.run()
        logger.info("[job:discovery_introspection] %s", summary)
    except Exception:
        logger.exception("[job:discovery_introspection] failed")


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup() -> None:
    global price_engine, x_watchdog, edgar_scanner, polymarket_engine
    global research_runner, introspection

    # Create tables
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables ensured.")

    # Seed
    session = get_session()
    try:
        seed_database(session)
    finally:
        session.close()

    # Initialise engines
    price_engine = PriceEngine(
        api_key=settings.alpha_vantage_api_key,
        session_factory=get_session,
    )

    # SPEC-028B Task 5b: shared symbol validator + discovery engine factory.
    # - SymbolValidator wraps AlphaVantage SYMBOL_SEARCH (24h LRU cache,
    #   graceful None fallback when key missing, never raises).
    # - discovery_engine_factory is called per scan cycle with the scanner's
    #   live session so the DiscoveryEngine participates in the same
    #   transaction boundary as the scanner's own writes.
    symbol_validator = SymbolValidator(api_key=settings.alpha_vantage_api_key)

    def discovery_engine_factory(session):
        return DiscoveryEngine(
            db_session=session,
            webhook_url=getattr(settings, "discord_webhook_pre_ipo", None),
        )

    x_watchdog = XWatchdog(
        api_key=settings.xai_api_key,
        session_factory=get_session,
        discovery_engine_factory=discovery_engine_factory,
        symbol_validator=symbol_validator,
    )
    edgar_scanner = EdgarScanner(
        session_factory=get_session,
        targets=settings.edgar_targets,
        discovery_engine_factory=discovery_engine_factory,
    )
    polymarket_engine = PolymarketEngine(
        session_factory=get_session,
        discovery_engine_factory=discovery_engine_factory,
    )

    # Register scheduler jobs
    scheduler.add_job(
        _job_price_poll,
        trigger=CronTrigger(day_of_week="mon-fri", hour="9-15", minute="*/15", timezone="America/New_York"),
        id="price_poll",
        replace_existing=True,
    )
    scheduler.add_job(
        _job_x_scan,
        trigger=CronTrigger(day_of_week="mon-fri", hour="6-20", minute="*/10", timezone="America/New_York"),
        id="x_scan",
        replace_existing=True,
    )
    scheduler.add_job(
        _job_edgar_scan,
        trigger=IntervalTrigger(seconds=60),
        id="edgar_scan",
        replace_existing=True,
    )
    scheduler.add_job(
        _job_polymarket_scan,
        trigger=IntervalTrigger(minutes=5),
        id="polymarket_scan",
        replace_existing=True,
    )

    # SPEC-028B Task 4 — Research Runner + Introspection (gated on flag)
    jobs_registered = 4
    if getattr(settings, "discovery_scheduler_enabled", False):
        research_runner = DiscoveryResearchRunner(
            session_factory=get_session,
            discovery_engine_factory=discovery_engine_factory,
            script_path=resolve_last30days_script(
                getattr(settings, "last30days_script_path", "") or ""
            ),
        )
        introspection = DiscoveryIntrospection(session_factory=get_session)

        # 4 daily research slots at 08:00/08:30/09:00/09:30 ET (M-F only)
        for slot_idx, (hour, minute) in enumerate(
            [(8, 0), (8, 30), (9, 0), (9, 30)]
        ):
            scheduler.add_job(
                _job_discovery_research_slot,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=hour,
                    minute=minute,
                    timezone="America/New_York",
                ),
                id=f"discovery_research_slot_{slot_idx}",
                kwargs={"slot_idx": slot_idx},
                replace_existing=True,
            )
            jobs_registered += 1

        # Nightly 02:00 ET introspection
        scheduler.add_job(
            _job_discovery_introspection,
            trigger=CronTrigger(hour=2, minute=0, timezone="America/New_York"),
            id="discovery_introspection",
            replace_existing=True,
        )
        jobs_registered += 1
        logger.info(
            "SPEC-028B Task 4 discovery scheduler ENABLED — 4 research slots + nightly introspection registered."
        )
    else:
        logger.info(
            "SPEC-028B Task 4 discovery scheduler DISABLED (set DISCOVERY_SCHEDULER_ENABLED=true to enable)."
        )

    scheduler.start()
    logger.info(
        "Black Pine Signals started on 127.0.0.1:8003 — %d scheduler jobs registered.",
        jobs_registered,
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    scheduler.shutdown(wait=False)
    logger.info("Black Pine Signals stopped.")


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
@app.get("/health")
async def health() -> dict[str, Any]:
    jobs = [
        {"id": job.id, "next_run": str(job.next_run_time)}
        for job in scheduler.get_jobs()
    ]
    return {
        "status": "ok",
        "service": "blackpine_signals",
        "version": "0.1.0",
        "jobs": jobs,
    }


# ---------------------------------------------------------------------------
# Watchlist
# ---------------------------------------------------------------------------
@app.get("/v1/watchlist")
async def get_watchlist() -> dict[str, Any]:
    session = get_session()
    try:
        tickers = session.query(WatchlistTicker).filter_by(active=True).all()
        return {
            "count": len(tickers),
            "tickers": [
                {
                    "symbol": t.symbol,
                    "name": t.name,
                    "tier": t.tier,
                    "sector": t.sector,
                    "active": t.active,
                }
                for t in tickers
            ],
        }
    finally:
        session.close()


# ---------------------------------------------------------------------------
# IPO Pipeline
# ---------------------------------------------------------------------------
@app.get("/v1/ipo/pipeline")
async def get_ipo_pipeline() -> dict[str, Any]:
    session = get_session()
    try:
        entries = (
            session.query(IPOPipeline)
            .order_by(IPOPipeline.confidence_score.desc())
            .all()
        )
        return {
            "count": len(entries),
            "pipeline": [
                {
                    "id": e.id,
                    "company_name": e.company_name,
                    "stage": e.stage,
                    "confidence_score": e.confidence_score,
                    "estimated_valuation_low": e.estimated_valuation_low,
                    "estimated_valuation_high": e.estimated_valuation_high,
                    "lead_banks": e.lead_banks,
                    "correlated_tickers": e.correlated_tickers,
                    "updated_at": str(e.updated_at),
                }
                for e in entries
            ],
        }
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Fused Signals
# ---------------------------------------------------------------------------
@app.get("/v1/signals")
async def get_signals(
    tier: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    session = get_session()
    try:
        query = session.query(FusedSignal).order_by(FusedSignal.created_at.desc())
        if tier:
            valid_tiers = {"critical", "high", "moderate", "low"}
            if tier not in valid_tiers:
                raise HTTPException(status_code=400, detail=f"tier must be one of {sorted(valid_tiers)}")
            query = query.filter(FusedSignal.alert_tier == tier)
        total = query.count()
        signals = query.offset(offset).limit(limit).all()
        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "signals": [
                {
                    "id": s.id,
                    "ticker": s.ticker,
                    "signal_type": s.signal_type,
                    "fused_score": s.fused_score,
                    "source_count": s.source_count,
                    "sources": s.sources,
                    "narrative": s.narrative,
                    "alert_tier": s.alert_tier,
                    "dispatched": s.dispatched,
                    "created_at": str(s.created_at),
                }
                for s in signals
            ],
        }
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Stripe Webhook
# ---------------------------------------------------------------------------
@app.post("/v1/stripe/webhook")
async def stripe_webhook(request: Request) -> Response:
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    # Verify signature if webhook secret is configured
    if settings.stripe_webhook_secret:
        try:
            import stripe  # type: ignore[import]
            event = stripe.Webhook.construct_event(
                payload, sig_header, settings.stripe_webhook_secret
            )
            event_dict = dict(event)
        except Exception as exc:
            logger.warning("Stripe webhook signature invalid: %s", exc)
            raise HTTPException(status_code=400, detail="Invalid signature")
    else:
        import json
        try:
            event_dict = json.loads(payload)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

    session = get_session()
    try:
        handle_stripe_event(session, event_dict)
    except Exception:
        logger.exception("handle_stripe_event failed")
        raise HTTPException(status_code=500, detail="Event processing failed")
    finally:
        session.close()

    return Response(content='{"received": true}', media_type="application/json")


# ---------------------------------------------------------------------------
# Manual Ops Triggers
# ---------------------------------------------------------------------------
@app.post("/v1/ops/trigger-price")
async def trigger_price() -> dict[str, str]:
    await _job_price_poll()
    return {"status": "triggered", "job": "price_poll"}


@app.post("/v1/ops/trigger-x-scan")
async def trigger_x_scan() -> dict[str, str]:
    await _job_x_scan()
    return {"status": "triggered", "job": "x_scan"}


@app.post("/v1/ops/trigger-edgar")
async def trigger_edgar() -> dict[str, str]:
    await _job_edgar_scan()
    return {"status": "triggered", "job": "edgar_scan"}


@app.post("/v1/ops/trigger-polymarket")
async def trigger_polymarket() -> dict[str, str]:
    await _job_polymarket_scan()
    return {"status": "triggered", "job": "polymarket_scan"}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run("blackpine_signals.main:app", host="127.0.0.1", port=8003, reload=False)
