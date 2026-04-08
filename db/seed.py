"""
Seed data for Black Pine Signals database.
Governing doc: /home/nodeuser/documents/2026-04-04-spec-028-black-pine-signals.md

Safe to re-run — checks for existing data before inserting.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from blackpine_signals.db.models import IPOPipeline, PolymarketCorrelation, WatchlistTicker

# ---------------------------------------------------------------------------
# Initial Watchlist — 13 AI tickers across 5 tiers
# ---------------------------------------------------------------------------
INITIAL_TICKERS: list[dict] = [
    # Tier 1: Mega-cap infrastructure
    {"symbol": "NVDA",  "name": "NVIDIA Corporation",          "tier": "mega_cap",        "sector": "semiconductors"},
    {"symbol": "AVGO",  "name": "Broadcom Inc.",                "tier": "mega_cap",        "sector": "semiconductors"},
    {"symbol": "TSM",   "name": "Taiwan Semiconductor Mfg",    "tier": "mega_cap",        "sector": "semiconductors"},
    # Tier 2: Memory / compute
    {"symbol": "MU",    "name": "Micron Technology",            "tier": "large_cap",       "sector": "semiconductors"},
    # Tier 3: AI platform hyperscalers
    {"symbol": "MSFT",  "name": "Microsoft Corporation",        "tier": "mega_cap",        "sector": "cloud_ai"},
    {"symbol": "GOOGL", "name": "Alphabet Inc.",                "tier": "mega_cap",        "sector": "cloud_ai"},
    {"symbol": "META",  "name": "Meta Platforms Inc.",          "tier": "mega_cap",        "sector": "cloud_ai"},
    # Tier 4: Pure-play AI
    {"symbol": "NBIS",  "name": "Nebius Group N.V.",            "tier": "small_cap",       "sector": "cloud_ai"},
    {"symbol": "IREN",  "name": "Iris Energy Ltd.",             "tier": "small_cap",       "sector": "ai_infrastructure"},
    {"symbol": "SOUN",  "name": "SoundHound AI Inc.",           "tier": "micro_cap",       "sector": "applied_ai"},
    {"symbol": "PATH",  "name": "UiPath Inc.",                  "tier": "mid_cap",         "sector": "applied_ai"},
    # Tier 5: DevTools / creative AI
    {"symbol": "GTLB",  "name": "GitLab Inc.",                  "tier": "mid_cap",         "sector": "devtools_ai"},
    {"symbol": "ADBE",  "name": "Adobe Inc.",                   "tier": "large_cap",       "sector": "creative_ai"},
]

# ---------------------------------------------------------------------------
# Initial IPO Pipeline — 10 pre-IPO targets
# ---------------------------------------------------------------------------
INITIAL_IPO_TARGETS: list[dict] = [
    {
        "company_name": "Anthropic PBC",
        "stage": "in_talks",
        "estimated_valuation_low":  400_000_000_000,
        "estimated_valuation_high": 500_000_000_000,
        "lead_banks": ["Goldman Sachs", "JPMorgan Chase"],
        "strategic_investors": ["Google", "Amazon", "Spark Capital"],
        "correlated_tickers": [
            {"symbol": "NVDA",  "relationship": "chip_supplier"},
            {"symbol": "GOOGL", "relationship": "investor"},
            {"symbol": "AMZN",  "relationship": "investor"},
        ],
        "confidence_score": 75,
    },
    {
        "company_name": "SpaceX",
        "stage": "filed",
        "estimated_valuation_low":  210_000_000_000,
        "estimated_valuation_high": 250_000_000_000,
        "lead_banks": ["Morgan Stanley", "Goldman Sachs"],
        "strategic_investors": ["Fidelity", "Google"],
        "correlated_tickers": [
            {"symbol": "IREN", "relationship": "data_center_power"},
        ],
        "confidence_score": 80,
    },
    {
        "company_name": "OpenAI",
        "stage": "whisper",
        "estimated_valuation_low":  150_000_000_000,
        "estimated_valuation_high": 300_000_000_000,
        "lead_banks": ["Goldman Sachs"],
        "strategic_investors": ["Microsoft", "Tiger Global"],
        "correlated_tickers": [
            {"symbol": "MSFT",  "relationship": "investor_partner"},
            {"symbol": "NVDA",  "relationship": "chip_supplier"},
        ],
        "confidence_score": 60,
    },
    {
        "company_name": "xAI",
        "stage": "ghost",
        "estimated_valuation_low":  40_000_000_000,
        "estimated_valuation_high": 80_000_000_000,
        "lead_banks": [],
        "strategic_investors": ["Andreessen Horowitz", "Valor Equity"],
        "correlated_tickers": [
            {"symbol": "NVDA", "relationship": "chip_supplier"},
            {"symbol": "TSLA", "relationship": "elon_halo"},
        ],
        "confidence_score": 35,
    },
    {
        "company_name": "Groq",
        "stage": "ghost",
        "estimated_valuation_low":  2_800_000_000,
        "estimated_valuation_high": 5_000_000_000,
        "lead_banks": [],
        "strategic_investors": ["Tiger Global", "Battery Ventures"],
        "correlated_tickers": [
            {"symbol": "NVDA", "relationship": "competitor"},
            {"symbol": "AVGO", "relationship": "chip_ecosystem"},
        ],
        "confidence_score": 25,
    },
    {
        "company_name": "CoreWeave",
        "stage": "whisper",
        "estimated_valuation_low":  19_000_000_000,
        "estimated_valuation_high": 35_000_000_000,
        "lead_banks": ["Goldman Sachs", "Morgan Stanley"],
        "strategic_investors": ["Nvidia", "Magnetar Capital"],
        "correlated_tickers": [
            {"symbol": "NVDA", "relationship": "supplier_investor"},
            {"symbol": "MSFT", "relationship": "customer"},
        ],
        "confidence_score": 55,
    },
    {
        "company_name": "Databricks",
        "stage": "ghost",
        "estimated_valuation_low":  43_000_000_000,
        "estimated_valuation_high": 60_000_000_000,
        "lead_banks": [],
        "strategic_investors": ["Andreessen Horowitz", "NVIDIA"],
        "correlated_tickers": [
            {"symbol": "NVDA",  "relationship": "investor"},
            {"symbol": "GOOGL", "relationship": "cloud_partner"},
            {"symbol": "MSFT",  "relationship": "cloud_partner"},
        ],
        "confidence_score": 40,
    },
    {
        "company_name": "Stripe",
        "stage": "whisper",
        "estimated_valuation_low":  65_000_000_000,
        "estimated_valuation_high": 100_000_000_000,
        "lead_banks": ["Goldman Sachs", "JPMorgan Chase"],
        "strategic_investors": ["Sequoia Capital", "Andreessen Horowitz"],
        "correlated_tickers": [
            {"symbol": "ADBE", "relationship": "payments_integration"},
        ],
        "confidence_score": 50,
    },
    {
        "company_name": "Canva",
        "stage": "ghost",
        "estimated_valuation_low":  26_000_000_000,
        "estimated_valuation_high": 40_000_000_000,
        "lead_banks": [],
        "strategic_investors": ["T. Rowe Price", "Franklin Templeton"],
        "correlated_tickers": [
            {"symbol": "ADBE", "relationship": "competitor"},
        ],
        "confidence_score": 30,
    },
    {
        "company_name": "Figma",
        "stage": "ghost",
        "estimated_valuation_low":  10_000_000_000,
        "estimated_valuation_high": 20_000_000_000,
        "lead_banks": [],
        "strategic_investors": ["Index Ventures", "Greylock"],
        "correlated_tickers": [
            {"symbol": "ADBE", "relationship": "acquisition_target_blocked"},
            {"symbol": "MSFT", "relationship": "platform_customer"},
        ],
        "confidence_score": 30,
    },
    # ── Discovered 2026-04-08 via /last30days research ────────────────
    {
        "company_name": "Skims",
        "stage": "ghost",
        "estimated_valuation_low":  4_000_000_000,
        "estimated_valuation_high": 6_000_000_000,
        "lead_banks": [],
        "strategic_investors": ["Imaginary Ventures", "Thrive Capital"],
        "correlated_tickers": [],
        "confidence_score": 25,
    },
    {
        "company_name": "Replit",
        "stage": "ghost",
        "estimated_valuation_low":  3_000_000_000,
        "estimated_valuation_high": 5_000_000_000,
        "lead_banks": [],
        "strategic_investors": ["a16z", "Khosla Ventures", "Coatue"],
        "correlated_tickers": [
            {"symbol": "MSFT", "relationship": "github_competitor"},
            {"symbol": "GOOGL", "relationship": "cloud_partner"},
        ],
        "confidence_score": 30,
    },
    {
        "company_name": "Sarvam AI",
        "stage": "ghost",
        "estimated_valuation_low":  1_500_000_000,
        "estimated_valuation_high": 2_000_000_000,
        "lead_banks": [],
        "strategic_investors": ["NVIDIA", "Accel", "HCLTech"],
        "correlated_tickers": [
            {"symbol": "NVDA", "relationship": "investor_chip_supplier"},
        ],
        "confidence_score": 35,
    },
]

# ---------------------------------------------------------------------------
# Initial Polymarket Correlations — 1 seed correlation
# ---------------------------------------------------------------------------
INITIAL_POLY_CORRELATIONS: list[dict] = [
    {
        "market_id": "will-anthropic-ipo-2026",
        "market_title": "Will Anthropic complete an IPO in 2026?",
        "correlated_tickers": ["NVDA", "GOOGL", "AMZN"],
        "correlation_type": "ipo_trigger",
    },
]


# ---------------------------------------------------------------------------
# Seed function — idempotent
# ---------------------------------------------------------------------------
def seed_database(session: Session) -> None:
    """Seed initial data. Safe to re-run — skips if data already exists."""

    # --- Tickers ---
    existing_symbols = {row.symbol for row in session.query(WatchlistTicker.symbol).all()}
    new_tickers = [
        WatchlistTicker(**t) for t in INITIAL_TICKERS if t["symbol"] not in existing_symbols
    ]
    if new_tickers:
        session.add_all(new_tickers)
        session.commit()
        print(f"[seed] Inserted {len(new_tickers)} tickers.")
    else:
        print("[seed] Tickers already seeded — skipping.")

    # --- IPO Pipeline ---
    existing_companies = {
        row.company_name for row in session.query(IPOPipeline.company_name).all()
    }
    new_ipos = [
        IPOPipeline(**target)
        for target in INITIAL_IPO_TARGETS
        if target["company_name"] not in existing_companies
    ]
    if new_ipos:
        session.add_all(new_ipos)
        session.commit()
        print(f"[seed] Inserted {len(new_ipos)} IPO pipeline entries.")
    else:
        print("[seed] IPO pipeline already seeded — skipping.")

    # --- Polymarket Correlations ---
    existing_market_ids = {
        row.market_id for row in session.query(PolymarketCorrelation.market_id).all()
    }
    new_correlations = [
        PolymarketCorrelation(**c)
        for c in INITIAL_POLY_CORRELATIONS
        if c["market_id"] not in existing_market_ids
    ]
    if new_correlations:
        session.add_all(new_correlations)
        session.commit()
        print(f"[seed] Inserted {len(new_correlations)} Polymarket correlations.")
    else:
        print("[seed] Polymarket correlations already seeded — skipping.")

    print("[seed] Done.")
