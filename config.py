"""
Centralized configuration for Black Pine Signals.
Governing doc: /home/nodeuser/documents/2026-04-04-spec-028-black-pine-signals.md
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """All config sourced from env vars or .env file."""

    # Database
    database_url: str = "postgresql://bps_user:bps_pass@127.0.0.1:5434/blackpine_signals"

    # Alpha Vantage
    alpha_vantage_api_key: str = ""

    # xAI Grok
    xai_api_key: str = ""

    # Stripe
    stripe_secret_key: str = ""
    stripe_webhook_secret: str = ""
    stripe_price_pro: str = ""
    stripe_price_quant: str = ""

    # Discord webhooks
    discord_webhook_daily_brief: str = ""
    discord_webhook_ipo_radar: str = ""
    discord_webhook_realtime: str = ""
    discord_webhook_pre_ipo: str = ""
    discord_webhook_polysignal: str = ""
    discord_bot_token: str = ""

    # Polling intervals
    price_poll_interval_minutes: int = 15
    edgar_poll_interval_seconds: int = 60
    polymarket_odds_threshold: float = 5.0

    # Signal thresholds
    signal_critical_threshold: float = 80.0
    signal_high_threshold: float = 50.0
    signal_moderate_threshold: float = 20.0

    # EDGAR targets
    edgar_targets: list[str] = Field(default=[
        "Anthropic", "OpenAI", "xAI", "SpaceX", "Groq",
        "CoreWeave", "Databricks", "Stripe", "Canva", "Figma",
    ])

    model_config = {"env_file": str(Path(__file__).parent / ".env"), "extra": "ignore"}


settings = Settings()
