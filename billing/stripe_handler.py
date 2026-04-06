# /home/nodeuser/blackpine_core/blackpine_signals/billing/stripe_handler.py
"""Stripe subscription handler. Governing doc: SPEC-028 §9"""
from __future__ import annotations
import logging
import uuid
from typing import Any
from sqlalchemy.orm import Session
from blackpine_signals.db.models import Subscriber

logger = logging.getLogger("bps.stripe")

def generate_api_key() -> str:
    return f"bpl_sk_{uuid.uuid4().hex}"

def provision_subscriber(session: Session, discord_id: str, tier: str, stripe_id: str,
                          discord_username: str | None = None) -> Subscriber:
    sub = session.query(Subscriber).filter_by(discord_id=discord_id).first()
    if sub is None:
        sub = Subscriber(discord_id=discord_id)
        session.add(sub)
    sub.tier = tier
    sub.stripe_customer_id = stripe_id
    # discord_username is accepted for forward-compat but not persisted
    # (Subscriber model does not carry a discord_username column)
    if tier == "quant" and not sub.api_key:
        sub.api_key = generate_api_key()
    elif tier != "quant":
        sub.api_key = None
    session.commit()
    return sub

def downgrade_subscriber(session: Session, stripe_id: str) -> None:
    sub = session.query(Subscriber).filter_by(stripe_customer_id=stripe_id).first()
    if sub is None:
        return
    sub.tier = "free"
    sub.api_key = None
    session.commit()

def handle_stripe_event(session: Session, event: dict[str, Any]) -> None:
    event_type = event.get("type", "")
    if event_type == "checkout.session.completed":
        data = event["data"]["object"]
        discord_id = data.get("metadata", {}).get("discord_id", "")
        stripe_id = data.get("customer", "")
        line_items = data.get("line_items", {}).get("data", [])
        tier = "pro"
        for item in line_items:
            price_id = item.get("price", {}).get("id", "")
            if "quant" in price_id.lower():
                tier = "quant"
                break
        if discord_id and stripe_id:
            provision_subscriber(session, discord_id=discord_id, tier=tier, stripe_id=stripe_id)
    elif event_type == "customer.subscription.deleted":
        data = event["data"]["object"]
        stripe_id = data.get("customer", "")
        downgrade_subscriber(session, stripe_id=stripe_id)
