# /home/nodeuser/blackpine_core/blackpine_signals/delivery/discord_bot.py
"""Discord delivery — webhook dispatch. Governing doc: SPEC-028 §8"""
from __future__ import annotations
import asyncio
import logging
from typing import Any
import httpx
from blackpine_signals.config import settings

logger = logging.getLogger("bps.discord")

CHANNELS = {
    "daily_brief": lambda: settings.discord_webhook_daily_brief,
    "ipo_radar": lambda: settings.discord_webhook_ipo_radar,
    "realtime": lambda: settings.discord_webhook_realtime,
    "pre_ipo": lambda: settings.discord_webhook_pre_ipo,
    "polysignal": lambda: settings.discord_webhook_polysignal,
}

TIER_CHANNELS = {"critical": ["realtime", "polysignal"], "high": ["realtime"], "moderate": [], "heatmap": []}
TIER_COLORS = {"critical": 0xFF0000, "high": 0xFF8C00, "moderate": 0xFFD700, "heatmap": 0x808080}

async def send_embed(channel: str, title: str, description: str, color: int = 0x1A1A2E,
                      fields: list[dict[str, Any]] | None = None) -> bool:
    webhook_url = CHANNELS.get(channel, lambda: "")()
    if not webhook_url:
        logger.warning("No webhook for channel: %s", channel)
        return False
    embed: dict[str, Any] = {"title": title, "description": description, "color": color}
    if fields:
        embed["fields"] = fields
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(3):
            try:
                resp = await client.post(webhook_url, json={"embeds": [embed]})
                if resp.status_code == 204:
                    return True
                if resp.status_code == 429:
                    await asyncio.sleep(resp.json().get("retry_after", 2 ** attempt))
                    continue
                if resp.status_code >= 500:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return False
            except httpx.HTTPError:
                logger.exception("Discord dispatch failed (attempt %d)", attempt + 1)
    return False

async def dispatch_signal(ticker: str | None, signal_type: str, score: float, alert_tier: str,
                           narrative: str, sources: dict) -> None:
    channels = TIER_CHANNELS.get(alert_tier, [])
    if not channels:
        return
    color = TIER_COLORS.get(alert_tier, 0x1A1A2E)
    title = f"{'🚨' if alert_tier == 'critical' else '⚠️'} {ticker or 'Market'} — {signal_type}"
    description = narrative or f"Score: {score:.0f} | Sources: {sources}"
    fields = [
        {"name": "Score", "value": f"{score:.0f}", "inline": True},
        {"name": "Alert Tier", "value": alert_tier.upper(), "inline": True},
        {"name": "Sources", "value": ", ".join(f"{k}: {v}" for k, v in sources.items()), "inline": True},
    ]
    for channel in channels:
        await send_embed(channel, title, description, color=color, fields=fields)
