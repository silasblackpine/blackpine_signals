"""
2026-04-08 — XMCP Whale Watcher.

Pulls the last 24h of X/Twitter posts from a curated allowlist of high-signal
finance accounts via the bpl-xmcp-gateway (Tailscale 100.123.117.38:8010),
runs lightweight in-module sentiment + batch-relative anomaly detection, feeds
newly detected tickers into the BPS Discovery Engine, and emits a compact
summary string suitable for injection into the 06:00 ET /last30days research
prompt.

Governing plan:
  /home/nodeuser/documents/2026-04-08-bps-cadence-whale-watcher.md

Hard rules honored:
  #3 (no boundary violations) — sentiment is keyword-only, anomaly is
      batch-relative, no historical baselines, no quant signal generation.
      All quant analysis remains Silas's domain.
  #6 (plan before code) — see governing plan above.
  #8 (graceful degradation) — never raises; the 06:00 ET research slot must
      always proceed even if XMCP is down or X API credits are depleted.
"""
from __future__ import annotations

import json
import logging
import re
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Literal, Optional, TypedDict

import httpx
from sqlalchemy.orm import Session

from blackpine_signals.ingestion.discovery_engine import DiscoveryEngine
from blackpine_signals.ingestion.discovery_research import TICKER_BLACKLIST
from blackpine_signals.ingestion.x_watchdog import DEFAULT_ACCOUNTS
from blackpine_signals.models.credibility import get_credibility_tier

logger = logging.getLogger("bps.whale_watcher")


# ---------------------------------------------------------------------------
# Sentiment lexicon (matches existing x_watchdog.py heuristic — keyword only)
# ---------------------------------------------------------------------------

_BULL_WORDS = (
    "up", "buy", "bull", "beat", "surge", "soar", "rally", "breakout",
    "moon", "pump", "rip", "ripping",
)
_BEAR_WORDS = (
    "down", "sell", "bear", "miss", "drop", "crash", "dump", "rug", "tank",
    "collapse",
)

TICKER_PATTERN = re.compile(r"\$([A-Z]{1,5})\b")

# Conservative cap so the OR query stays well under the X v2 query length
# limit on the basic tier (~512 chars). 12 handles × ~20 chars ≈ 240.
_BATCH_SIZE = 12
_SUMMARY_MAX_CHARS = 1500


def _classify_sentiment(text: str) -> Literal["bullish", "bearish", "neutral"]:
    lower = text.lower()
    if any(w in lower for w in _BULL_WORDS):
        return "bullish"
    if any(w in lower for w in _BEAR_WORDS):
        return "bearish"
    return "neutral"


def _extract_tickers(text: str) -> list[str]:
    """Return unique $TICKERs found, filtered against TICKER_BLACKLIST."""
    seen: set[str] = set()
    out: list[str] = []
    for m in TICKER_PATTERN.finditer(text or ""):
        sym = m.group(1)
        if sym in TICKER_BLACKLIST or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


def _is_credits_depleted(payload: dict[str, Any]) -> bool:
    """Detect XMCP gateway's wrapped 402 CreditsDepleted error.

    Verified live 2026-04-08 with the BPS consumer key:
        {"isError": true,
         "content": [{"type":"text","text":"Error calling tool ...
            HTTP error 402: Payment Required ... CreditsDepleted ..."}]}
    """
    if not payload.get("isError"):
        return False
    blob = json.dumps(payload.get("content") or [])
    return "402" in blob and (
        "CreditsDepleted" in blob or "Payment Required" in blob
    )


def _parse_posts_from_mcp(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Walk the MCP content list and extract a flat list of post dicts.

    The xmcp upstream returns posts as JSON inside a text block. We tolerate
    both top-level JSON arrays and the X v2 ``{"data": [...]}`` shape.
    Unknown shapes return [] rather than raising.
    """
    posts: list[dict[str, Any]] = []
    for block in payload.get("content") or []:
        if not isinstance(block, dict):
            continue
        if block.get("type") != "text":
            continue
        text = block.get("text") or ""
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, list):
            posts.extend(p for p in obj if isinstance(p, dict))
        elif isinstance(obj, dict):
            data = obj.get("data")
            if isinstance(data, list):
                posts.extend(p for p in data if isinstance(p, dict))
    return posts


class WhaleWatcherResult(TypedDict):
    status: Literal["ok", "credits_depleted", "error", "no_data"]
    posts_seen: int
    notable_count: int
    tickers_ingested: int
    summary_text: str
    error: Optional[str]


class WhaleWatcher:
    """Inline pre-06:00 ET job: 24h X pull → ingest tickers + build prompt context."""

    def __init__(
        self,
        gateway_url: str,
        api_key: str,
        session_factory: Callable[[], Session],
        discovery_engine_factory: Callable[[Session], DiscoveryEngine],
        accounts: Optional[list[str]] = None,
        lookback_hours: int = 24,
        max_results_per_batch: int = 100,
        timeout_seconds: float = 60.0,
    ) -> None:
        self.gateway_url = gateway_url.rstrip("/")
        self.api_key = api_key
        self.session_factory = session_factory
        self.discovery_engine_factory = discovery_engine_factory
        self.accounts = accounts or list(DEFAULT_ACCOUNTS)
        self.lookback_hours = lookback_hours
        self.max_results_per_batch = max_results_per_batch
        self.timeout_seconds = timeout_seconds

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    async def run(self) -> WhaleWatcherResult:
        result: WhaleWatcherResult = {
            "status": "ok",
            "posts_seen": 0,
            "notable_count": 0,
            "tickers_ingested": 0,
            "summary_text": "",
            "error": None,
        }
        try:
            all_posts: list[dict[str, Any]] = []
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                for batch in self._account_batches():
                    batch_result = await self._call_search(client, batch)
                    if batch_result is None:
                        # credits_depleted — short-circuit, return immediately
                        result["status"] = "credits_depleted"
                        logger.info(
                            "[whale_watcher] Awaiting X API Credits — 24h pull bypassed"
                        )
                        return result
                    all_posts.extend(batch_result)

            result["posts_seen"] = len(all_posts)
            if not all_posts:
                result["status"] = "no_data"
                return result

            # ----- sentiment + anomaly + ticker ingest --------------------
            enriched = self._enrich_posts(all_posts)
            notable = [p for p in enriched if p["notable"]]
            result["notable_count"] = len(notable)

            ingested = self._ingest_tickers(notable)
            result["tickers_ingested"] = ingested

            result["summary_text"] = self._build_summary(enriched, notable)
            return result

        except httpx.HTTPError as exc:
            logger.exception("[whale_watcher] gateway HTTP error")
            result["status"] = "error"
            result["error"] = f"http: {exc}"[:200]
            return result
        except Exception as exc:  # noqa: BLE001 — never raise
            logger.exception("[whale_watcher] unexpected failure")
            result["status"] = "error"
            result["error"] = str(exc)[:200]
            return result

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _account_batches(self) -> list[list[str]]:
        return [
            self.accounts[i : i + _BATCH_SIZE]
            for i in range(0, len(self.accounts), _BATCH_SIZE)
        ]

    def _build_query(self, batch: list[str]) -> str:
        parts = " OR ".join(f"from:{h}" for h in batch)
        return f"({parts}) -is:reply"

    async def _call_search(
        self, client: httpx.AsyncClient, batch: list[str]
    ) -> Optional[list[dict[str, Any]]]:
        """Return list of posts, or None if credits are depleted (sentinel)."""
        start_time = (
            datetime.now(timezone.utc) - timedelta(hours=self.lookback_hours)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        body = {
            "tool": "searchPostsRecent",
            "arguments": {
                "query": self._build_query(batch),
                "start_time": start_time,
                "max_results": self.max_results_per_batch,
                "tweet.fields": "public_metrics,created_at,author_id",
            },
        }
        resp = await client.post(
            f"{self.gateway_url}/mcp/call",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json=body,
        )
        if resp.status_code != 200:
            logger.warning(
                "[whale_watcher] gateway returned HTTP %d for batch of %d accounts: %s",
                resp.status_code, len(batch), resp.text[:200],
            )
            return []
        payload = resp.json()
        if _is_credits_depleted(payload):
            return None
        if payload.get("isError"):
            logger.warning(
                "[whale_watcher] upstream tool error (non-402): %s",
                str(payload.get("content"))[:300],
            )
            return []
        posts = _parse_posts_from_mcp(payload)
        # Annotate each post with the upstream handle. The X v2 search
        # response includes author_id, not handle; we can't resolve it
        # without a second call. The "from:..." query already constrains
        # the result to our allowlist, so we leave handle="" when missing
        # and let the summary attribution fall back to author_id.
        for p in posts:
            p.setdefault("handle", p.get("username") or p.get("author_id") or "unknown")
            p.setdefault("text", p.get("text") or "")
            metrics = p.get("public_metrics") or {}
            p.setdefault("likes", metrics.get("like_count", 0))
            p.setdefault("reposts", metrics.get("retweet_count", 0))
        return posts

    def _enrich_posts(self, posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # Compute engagement and the batch P75 threshold
        engagements: list[float] = []
        for p in posts:
            eng = float(p.get("likes", 0)) + 2.0 * float(p.get("reposts", 0))
            p["engagement"] = eng
            engagements.append(eng)
        if len(engagements) >= 4:
            try:
                threshold = statistics.quantiles(engagements, n=4)[2]  # P75
            except statistics.StatisticsError:
                threshold = max(engagements)
        else:
            threshold = max(engagements) if engagements else 0.0

        for p in posts:
            p["sentiment"] = _classify_sentiment(p.get("text", ""))
            p["tickers"] = _extract_tickers(p.get("text", ""))
            p["notable"] = p["engagement"] >= threshold and p["engagement"] > 0
        return posts

    def _ingest_tickers(self, notable_posts: list[dict[str, Any]]) -> int:
        """Feed unique notable-post tickers into DiscoveryEngine.ingest_x_ticker."""
        if not notable_posts:
            return 0
        seen: set[tuple[str, str]] = set()
        ingested = 0
        session = self.session_factory()
        try:
            engine = self.discovery_engine_factory(session)
            for post in notable_posts:
                handle = post.get("handle", "unknown")
                tier = get_credibility_tier(handle)
                for ticker in post.get("tickers", []):
                    key = (ticker, handle)
                    if key in seen:
                        continue
                    seen.add(key)
                    try:
                        engine.ingest_x_ticker(
                            ticker=ticker, handle=handle, tier=tier
                        )
                        ingested += 1
                    except Exception:
                        logger.exception(
                            "[whale_watcher] ingest_x_ticker failed for $%s @%s",
                            ticker, handle,
                        )
            try:
                session.commit()
            except Exception:
                logger.exception("[whale_watcher] session commit failed")
                session.rollback()
        finally:
            session.close()
        return ingested

    def _build_summary(
        self,
        all_posts: list[dict[str, Any]],
        notable_posts: list[dict[str, Any]],
    ) -> str:
        if not all_posts:
            return ""
        bull = sum(1 for p in all_posts if p["sentiment"] == "bullish")
        bear = sum(1 for p in all_posts if p["sentiment"] == "bearish")
        neut = len(all_posts) - bull - bear

        # Top 5 notable by engagement
        top = sorted(notable_posts, key=lambda p: p["engagement"], reverse=True)[:5]

        # Ticker frequency across notable posts
        freq: dict[str, int] = {}
        for p in notable_posts:
            for t in p.get("tickers", []):
                freq[t] = freq.get(t, 0) + 1
        top_tickers = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:8]

        lines = [
            f"Pulled {len(all_posts)} posts from the last {self.lookback_hours}h "
            f"({len(notable_posts)} notable, P75 engagement).",
            f"Sentiment skew: {bull} bullish / {bear} bearish / {neut} neutral.",
        ]
        if top_tickers:
            lines.append(
                "Top mentioned tickers: "
                + ", ".join(f"${t} (x{n})" for t, n in top_tickers)
                + "."
            )
        if top:
            lines.append("Most engaged posts:")
            for p in top:
                tickers_str = " ".join(f"${t}" for t in p.get("tickers", []))
                snippet = (p.get("text") or "").replace("\n", " ").strip()[:140]
                lines.append(
                    f"- @{p.get('handle','?')} ({int(p['engagement'])} eng): "
                    f"{snippet}{(' [' + tickers_str + ']') if tickers_str else ''}"
                )
        summary = "\n".join(lines)
        return summary[:_SUMMARY_MAX_CHARS]
