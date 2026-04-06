from blackpine_signals.billing.stripe_handler import generate_api_key, provision_subscriber, downgrade_subscriber
from blackpine_signals.db.models import Subscriber

def test_generate_api_key():
    key = generate_api_key()
    assert key.startswith("bpl_sk_")
    assert len(key) > 20

def test_provision_pro_subscriber(db_session):
    provision_subscriber(db_session, discord_id="123456", tier="pro", stripe_id="cus_abc")
    sub = db_session.query(Subscriber).filter_by(discord_id="123456").one()
    assert sub.tier == "pro"
    assert sub.api_key is None

def test_provision_quant_subscriber(db_session):
    provision_subscriber(db_session, discord_id="789012", tier="quant", stripe_id="cus_xyz")
    sub = db_session.query(Subscriber).filter_by(discord_id="789012").one()
    assert sub.tier == "quant"
    assert sub.api_key is not None
    assert sub.api_key.startswith("bpl_sk_")

def test_downgrade_subscriber(db_session):
    provision_subscriber(db_session, discord_id="111111", tier="quant", stripe_id="cus_qqq")
    downgrade_subscriber(db_session, stripe_id="cus_qqq")
    sub = db_session.query(Subscriber).filter_by(discord_id="111111").one()
    assert sub.tier == "free"
    assert sub.api_key is None
