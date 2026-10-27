# Black Pine Signals

A Python service that watches several public data sources, turns raw events into scored, per-ticker signals, and delivers alerts to Discord. It runs as a single FastAPI app with a built-in scheduler, stores everything in PostgreSQL, and includes Stripe subscription handling.

It's a working example of the kind of system we build for clients: many APIs feeding one pipeline, an LLM in the loop, scheduled jobs, a database, outbound alerts and billing, all covered by automated tests.

## What it does

**Data sources (ingestion)**
- **Prices**: intraday price polling via the Alpha Vantage API (`ingestion/price_engine.py`)
- **SEC filings**: scans the SEC EDGAR RSS feed, classifies filing types and matches target companies (`ingestion/edgar_scanner.py`)
- **Prediction markets**: tracks Polymarket odds and flags shifts above a configurable threshold (`ingestion/polymarket_engine.py`)
- **X (Twitter) sentiment**: uses the xAI Grok API to surface relevant posts, extracts tickers and weights posts by source credibility (`ingestion/x_watchdog.py`, `models/credibility.py`)
- **Large-account watcher**: tracks posts from high-signal accounts, with simple bullish, bearish or neutral sentiment tagging and ticker extraction (`ingestion/whale_watcher.py`)
- **Discovery research**: a scheduled research pass that finds newly mentioned tickers and IPO candidates, validates symbols and records what it finds (`ingestion/discovery_*.py`, `ingestion/symbol_validator.py`)
- **IPO pipeline**: tracks companies through IPO stages (`ipo/pipeline.py`)

**Signal fusion**
`ingestion/sentiment_aggregator.py` groups incoming signals by ticker, combines their scores, records which sources contributed, and assigns an alert tier (`critical`, `high`, `moderate` or `heatmap`) using thresholds you set in the environment. The fused results are saved to the database.

**Delivery (Discord)**
`delivery/discord_bot.py` sends formatted embeds to Discord webhooks: a daily brief, IPO radar, real-time alerts, pre-IPO and Polymarket channels. Critical and high-tier signals go to the real-time channels automatically.

**Billing (Stripe)**
`billing/stripe_handler.py` processes Stripe webhook events. It provisions subscribers on two tiers (Pro and Quant), issues API keys and downgrades subscribers when a subscription ends.

**API**
| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Service health |
| GET | `/v1/watchlist` | Current watchlist |
| GET | `/v1/ipo/pipeline` | IPO pipeline stages |
| GET | `/v1/signals` | Fused signals |
| POST | `/v1/stripe/webhook` | Stripe events |
| POST | `/v1/ops/trigger-{price,x-scan,edgar,polymarket}` | Run an ingestion job on demand |

## Architecture

```
 Alpha Vantage ─┐
 SEC EDGAR ─────┤
 Polymarket ────┼─► Ingestion engines ─► Signal fusion ─► PostgreSQL ─► FastAPI (/v1/*)
 X via xAI Grok ┤     (APScheduler)       (score + tier)        │
 Research pass ─┘                                               └─► Discord webhooks
                                    Stripe webhooks ─► Billing ─► subscribers / API keys
```

- **FastAPI + Uvicorn** serves the API
- **APScheduler** runs the jobs: market-hours price polling, a frequent X scan on weekdays, EDGAR checks every minute, Polymarket checks every 5 minutes, and nightly research and introspection
- **SQLAlchemy 2** models with **PostgreSQL 16** (via Docker Compose)
- **httpx** for async API calls, **pydantic-settings** for configuration

## Project layout

```
billing/      Stripe webhook handling and subscriber provisioning
db/           SQLAlchemy engine, models, seed data, migrations
delivery/     Discord webhook delivery
ingestion/    Data source engines, signal fusion, discovery research
ipo/          IPO stage tracking
models/       Source credibility scoring and signal classification
tests/        pytest suite (in-memory SQLite)
main.py       App entrypoint: API routes and scheduler wiring
config.py     Settings loaded from environment / .env
```

## Setup

**Requirements:** Python 3.10+, Docker (for Postgres).

1. **Clone and install**
   ```bash
   git clone https://github.com/silasblackpine/blackpine_signals.git
   python -m venv .venv && source .venv/bin/activate
   pip install -r blackpine_signals/requirements.txt
   ```
   The code imports itself as the `blackpine_signals` package, so run commands from the folder that contains the repo.

2. **Configure the environment**
   ```bash
   cp blackpine_signals/.env.example blackpine_signals/.env
   ```
   Fill in your own values. `.env` is gitignored; only the placeholder file is committed.
   - `POSTGRES_PASSWORD`: password for the Postgres role. Docker Compose reads it from the environment (`POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}` in `docker-compose.yml`). Compose also loads the project `.env` file for this substitution.
   - `DATABASE_URL`: Postgres connection string. Use the same password as `POSTGRES_PASSWORD`.
   - `BPL_XMCP_GATEWAY_URL`: XMCP gateway for Whale Watcher. When unset, the app defaults to `http://localhost:8010`.
   - `ALPHA_VANTAGE_API_KEY`, `XAI_API_KEY`: data source keys
   - `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `STRIPE_PRICE_PRO`, `STRIPE_PRICE_QUANT`: only needed for billing
   - `DISCORD_WEBHOOK_*`, `DISCORD_BOT_TOKEN`: only needed for Discord delivery
   - Polling intervals and the `SIGNAL_*_THRESHOLD` values tune scheduling and alert tiers

   Never commit your `.env` file.

3. **Start Postgres**
   ```bash
   cd blackpine_signals && docker compose up -d && cd ..
   ```

4. **Run the service**
   ```bash
   python -m blackpine_signals.main
   ```
   The API listens on `127.0.0.1:8003`. Check `GET /health`.

## Tests

```bash
pytest blackpine_signals/tests
```

The tests run against an in-memory SQLite database, so they don't need Postgres or any API keys. They cover the models, config, every ingestion engine, signal fusion, the IPO pipeline, Stripe handling and the discovery and scanner integration.

## Status

This is a prototype under active development (v0.1.0). It's for research and alerting only and isn't financial advice.

---

Built by **Black Pine Lab**: AI automation, agents, integrations and data pipelines.
