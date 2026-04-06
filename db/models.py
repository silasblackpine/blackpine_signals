"""
ORM models for Black Pine Signals — 10 tables.
Governing doc: /home/nodeuser/documents/2026-04-04-spec-028-black-pine-signals.md

Note: JSON is used instead of JSONB for SQLite test compatibility.
On PostgreSQL, JSON columns behave identically to JSONB for our use case.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# 1. WatchlistTicker
# ---------------------------------------------------------------------------
class WatchlistTicker(Base):
    __tablename__ = "watchlist_tickers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(16), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    tier: Mapped[str] = mapped_column(String(64), nullable=False)
    sector: Mapped[str] = mapped_column(String(128), nullable=False)
    added_date: Mapped[Optional[date]] = mapped_column(Date, server_default=func.current_date())
    active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true", nullable=False)

    # Relationships
    price_snapshots: Mapped[list["PriceSnapshot"]] = relationship(
        "PriceSnapshot", back_populates="ticker", cascade="all, delete-orphan"
    )
    fundamentals: Mapped[list["Fundamental"]] = relationship(
        "Fundamental", back_populates="ticker", cascade="all, delete-orphan"
    )


# ---------------------------------------------------------------------------
# 2. PriceSnapshot
# ---------------------------------------------------------------------------
class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("watchlist_tickers.id", ondelete="CASCADE"), nullable=False, index=True
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    open: Mapped[Optional[float]] = mapped_column(Float)
    high: Mapped[Optional[float]] = mapped_column(Float)
    low: Mapped[Optional[float]] = mapped_column(Float)
    close: Mapped[Optional[float]] = mapped_column(Float)
    volume: Mapped[Optional[float]] = mapped_column(Float)

    # Relationship
    ticker: Mapped["WatchlistTicker"] = relationship("WatchlistTicker", back_populates="price_snapshots")


# ---------------------------------------------------------------------------
# 3. Fundamental
# ---------------------------------------------------------------------------
class Fundamental(Base):
    __tablename__ = "fundamentals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("watchlist_tickers.id", ondelete="CASCADE"), nullable=False, index=True
    )
    quarter: Mapped[Optional[str]] = mapped_column(String(16))  # e.g. "Q1-2026"
    revenue: Mapped[Optional[float]] = mapped_column(Float)
    earnings: Mapped[Optional[float]] = mapped_column(Float)
    pe_ratio: Mapped[Optional[float]] = mapped_column(Float)
    gross_margin: Mapped[Optional[float]] = mapped_column(Float)
    net_margin: Mapped[Optional[float]] = mapped_column(Float)
    analyst_target_low: Mapped[Optional[float]] = mapped_column(Float)
    analyst_target_high: Mapped[Optional[float]] = mapped_column(Float)
    analyst_target_mean: Mapped[Optional[float]] = mapped_column(Float)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationship
    ticker: Mapped["WatchlistTicker"] = relationship("WatchlistTicker", back_populates="fundamentals")


# ---------------------------------------------------------------------------
# 4. IPOPipeline
# ---------------------------------------------------------------------------
class IPOPipeline(Base):
    __tablename__ = "ipo_pipeline"
    __table_args__ = (
        CheckConstraint(
            "stage IN ('ghost','whisper','in_talks','filed','priced','listed')",
            name="ck_ipo_stage",
        ),
        CheckConstraint(
            "confidence_score >= 0 AND confidence_score <= 100",
            name="ck_ipo_confidence",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_name: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    stage: Mapped[str] = mapped_column(String(32), nullable=False, default="ghost")
    stage_history: Mapped[Optional[dict]] = mapped_column(JSON)  # [{stage, timestamp}, ...]
    estimated_valuation_low: Mapped[Optional[float]] = mapped_column(Float)
    estimated_valuation_high: Mapped[Optional[float]] = mapped_column(Float)
    lead_banks: Mapped[Optional[list]] = mapped_column(JSON)   # ["Goldman", "JPMorgan"]
    strategic_investors: Mapped[Optional[list]] = mapped_column(JSON)  # ["Google", "Spark Capital"]
    correlated_tickers: Mapped[Optional[list]] = mapped_column(JSON)  # [{"symbol": "NVDA", ...}]
    early_signals: Mapped[Optional[dict]] = mapped_column(JSON)  # {"x_mentions": 12, ...}
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationship
    edgar_filings: Mapped[list["EdgarFiling"]] = relationship(
        "EdgarFiling", back_populates="ipo_pipeline", cascade="all, delete-orphan"
    )


# ---------------------------------------------------------------------------
# 5. XSignal
# ---------------------------------------------------------------------------
class XSignal(Base):
    __tablename__ = "x_signals"
    __table_args__ = (
        CheckConstraint(
            "source_tier IN (1, 2, 3, 4)",
            name="ck_xsignal_tier",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_handle: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    source_tier: Mapped[int] = mapped_column(Integer, nullable=False)
    ticker_refs: Mapped[Optional[list]] = mapped_column(JSON)  # ["NVDA", "GOOGL"]
    text: Mapped[str] = mapped_column(Text, nullable=False)
    engagement_score: Mapped[Optional[float]] = mapped_column(Float)
    credibility_weighted_score: Mapped[Optional[float]] = mapped_column(Float)
    sentiment: Mapped[Optional[str]] = mapped_column(String(32))  # bullish/bearish/neutral
    signal_type: Mapped[Optional[str]] = mapped_column(String(64))  # ipo_rumor, price_move, etc.
    url: Mapped[Optional[str]] = mapped_column(String(512))
    posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# 6. EdgarFiling
# ---------------------------------------------------------------------------
class EdgarFiling(Base):
    __tablename__ = "edgar_filings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_name: Mapped[str] = mapped_column(String(256), nullable=False)
    ticker_ref: Mapped[Optional[str]] = mapped_column(String(16))
    form_type: Mapped[str] = mapped_column(String(32), nullable=False)  # S-1, 10-K, 8-K, etc.
    filed_date: Mapped[Optional[date]] = mapped_column(Date)
    filing_url: Mapped[str] = mapped_column(String(512), nullable=False, unique=True)
    summary: Mapped[Optional[str]] = mapped_column(Text)
    is_ipo_signal: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    ipo_pipeline_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("ipo_pipeline.id", ondelete="SET NULL")
    )
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationship
    ipo_pipeline: Mapped[Optional["IPOPipeline"]] = relationship(
        "IPOPipeline", back_populates="edgar_filings"
    )


# ---------------------------------------------------------------------------
# 7. PolymarketCorrelation
# ---------------------------------------------------------------------------
class PolymarketCorrelation(Base):
    __tablename__ = "polymarket_correlations"
    __table_args__ = (
        CheckConstraint(
            "correlation_type IN ('ipo_trigger','macro','geopolitical','earnings','regulatory')",
            name="ck_poly_correlation_type",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    market_title: Mapped[str] = mapped_column(String(512), nullable=False)
    correlated_tickers: Mapped[Optional[list]] = mapped_column(JSON)  # ["NVDA", "GOOGL"]
    correlation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationship
    signals: Mapped[list["PolymarketSignal"]] = relationship(
        "PolymarketSignal", back_populates="correlation", cascade="all, delete-orphan"
    )


# ---------------------------------------------------------------------------
# 8. PolymarketSignal
# ---------------------------------------------------------------------------
class PolymarketSignal(Base):
    __tablename__ = "polymarket_signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    correlation_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("polymarket_correlations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    odds: Mapped[Optional[float]] = mapped_column(Float)
    odds_delta: Mapped[Optional[float]] = mapped_column(Float)
    volume_24h: Mapped[Optional[float]] = mapped_column(Float)
    triggered_alert: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    snapshot_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationship
    correlation: Mapped["PolymarketCorrelation"] = relationship(
        "PolymarketCorrelation", back_populates="signals"
    )


# ---------------------------------------------------------------------------
# 9. FusedSignal
# ---------------------------------------------------------------------------
class FusedSignal(Base):
    __tablename__ = "fused_signals"
    __table_args__ = (
        CheckConstraint(
            "alert_tier IN ('critical','high','moderate','low')",
            name="ck_fused_alert_tier",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    signal_type: Mapped[str] = mapped_column(String(64), nullable=False)
    fused_score: Mapped[float] = mapped_column(Float, nullable=False)
    source_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sources: Mapped[Optional[dict]] = mapped_column(JSON)  # {"x": 2, "edgar": 1}
    narrative: Mapped[Optional[str]] = mapped_column(Text)
    alert_tier: Mapped[str] = mapped_column(String(16), nullable=False, default="low")
    dispatched: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# 10. Subscriber
# ---------------------------------------------------------------------------
class Subscriber(Base):
    __tablename__ = "subscribers"
    __table_args__ = (
        CheckConstraint(
            "tier IN ('free','pro','quant')",
            name="ck_subscriber_tier",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    discord_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True)
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(String(128), unique=True)
    tier: Mapped[str] = mapped_column(String(16), nullable=False, default="free")
    api_key: Mapped[Optional[str]] = mapped_column(String(256), unique=True)
    custom_watchlist: Mapped[Optional[list]] = mapped_column(JSON)  # ["NVDA", "AAPL"]
    webhook_urls: Mapped[Optional[dict]] = mapped_column(JSON)  # {"discord": "https://..."}
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationship (viewonly — api_key is a string, not a FK)
    api_usages: Mapped[list["APIUsage"]] = relationship(
        "APIUsage",
        primaryjoin="Subscriber.api_key == foreign(APIUsage.api_key)",
        viewonly=True,
    )


# ---------------------------------------------------------------------------
# 11. APIUsage
# ---------------------------------------------------------------------------
class APIUsage(Base):
    __tablename__ = "api_usage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    api_key: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    endpoint: Mapped[str] = mapped_column(String(256), nullable=False)
    request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    window_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    window_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Relationship back to Subscriber (viewonly — api_key is a string, not a FK)
    subscriber: Mapped[Optional["Subscriber"]] = relationship(
        "Subscriber",
        primaryjoin="APIUsage.api_key == foreign(Subscriber.api_key)",
        viewonly=True,
    )


# ---------------------------------------------------------------------------
# 12. DiscoveryCandidate  (SPEC-028B Task 1)
# Governing docs:
#   /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §4
#   /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 1
# ---------------------------------------------------------------------------
class DiscoveryCandidate(Base):
    __tablename__ = "discovery_candidates"
    __table_args__ = (
        CheckConstraint(
            "entity_type IN ('ticker', 'private_company', 'fund', 'unknown')",
            name="ck_discovery_candidates_entity_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'auto_added', 'proposed', 'rejected', 'expired')",
            name="ck_discovery_candidates_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_name: Mapped[str] = mapped_column(String(256), nullable=False, unique=True, index=True)
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False)
    proposed_ticker: Mapped[Optional[str]] = mapped_column(String(16), index=True)
    discovery_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0, index=True)
    source_signals: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    source_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    first_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending", server_default="pending", index=True
    )
    action_taken: Mapped[Optional[str]] = mapped_column(String(256))
    action_taken_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    notes: Mapped[Optional[str]] = mapped_column(Text)


# ---------------------------------------------------------------------------
# 13. DiscoveryLog  (SPEC-028B Task 1)
# Governing docs:
#   /documents/2026-04-05-spec-028b-auto-discovery-zhgp.md §4
#   /documents/2026-04-06-spec-028b-phase2-implementation-plan.md §4 Task 1
# ---------------------------------------------------------------------------
class DiscoveryLog(Base):
    __tablename__ = "discovery_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )
    action: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    entity_name: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    details: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    automated: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
