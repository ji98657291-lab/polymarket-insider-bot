"""
REALTIME Polymarket anomaly scanner + automatic BUY execution.

Flow:
  WebSocket trades -> anomaly detection -> scoring -> if severity >= 8 -> BUY

Auto-buy configuration:
  AUTO_BUY_ENABLED=true/false       (default false; set true for live orders)
  AUTO_BUY_USD=10                    USD budget per signal
  AUTO_BUY_MAX_PRICE=0.95            never buy above this price
  AUTO_BUY_PRICE_OFFSET=0.02         aggressive limit-price offset
  AUTO_BUY_RETRIES=3                 order retries

CLOB credentials are read from the same environment variables used by the
existing copy-trading bot:
  POLY_PRIVATE_KEY, POLY_FUNDER, POLY_API_KEY, POLY_SECRET, POLY_PASSPHRASE

Telegram:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""
import asyncio
import json
import os
import sys
import time
import httpx
from concurrent.futures import ThreadPoolExecutor
import py_clob_client_v2.http_helpers.helpers as _clob_http
import urllib.request

import websockets

from config import (
    REALTIME_WS_URL,
    REALTIME_SCORE_INTERVAL_SECONDS,
    REALTIME_BUFFER_MAX_AGE_SECONDS,
    MIN_SEVERITY_SCORE,
    WALLET_REPUTATION_ENABLED,
    NEWS_CHECK_ENABLED,
    FEEDBACK_ENABLED,
)
from detector import detect_anomalies
from state_store import (
    load_state, save_state, filter_new_alerts, is_paused, get_min_severity,
    find_cross_market_wallets, record_wallet_activity,
    find_conflicting_market_signal, record_market_signal,
)
from market_metadata import fetch_market_metadata_batch_cached, build_market_thresholds
from wallet_reputation import assess_wallet_freshness
from news_check import check_recent_news
from scoring import enrich_and_score
from command_handler import process_pending_commands
from telegram_notifier import notify_alerts
from outcome_tracker import record_alert_outcome, review_short_term, review_long_term
from weekly_report import maybe_send_weekly_report

SUBSCRIBE_MESSAGE = {
    "action": "subscribe",
    "subscriptions": [{"topic": "activity", "type": "trades", "filters": ""}],
}

# ── AUTO BUY ──────────────────────────────────────────────────────────────────
# Hard safety gate: the bot will ONLY auto-buy severity >= 8.
AUTO_BUY_MIN_SEVERITY = 8
AUTO_BUY_ENABLED = os.environ.get("AUTO_BUY_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
AUTO_BUY_USD = float(os.environ.get("AUTO_BUY_USD", "10"))
AUTO_BUY_MAX_PRICE = float(os.environ.get("AUTO_BUY_MAX_PRICE", "0.95"))
AUTO_BUY_PRICE_OFFSET = float(os.environ.get("AUTO_BUY_PRICE_OFFSET", "0.02"))
AUTO_BUY_RETRIES = max(1, int(os.environ.get("AUTO_BUY_RETRIES", "3")))

POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "")
POLY_FUNDER = os.environ.get("POLY_FUNDER", "")
POLY_API_KEY = os.environ.get("POLY_API_KEY", "")
POLY_SECRET = os.environ.get("POLY_SECRET", "")
POLY_PASSPHRASE = os.environ.get("POLY_PASSPHRASE", "")
PROXY_URL = os.environ.get("PROXY_URL", "")
CLOB_URL = "https://clob.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

if PROXY_URL:
    _clob_http._http_client = httpx.Client(http2=True, proxy=PROXY_URL, timeout=30.0)
    print(f"Proxy patched: {PROXY_URL}", flush=True)


clob_client = None
executor = ThreadPoolExecutor(max_workers=1)


def init_clob_client():
    """Initialize the same CLOB client used by the existing copy bot."""
    global clob_client

    required = [
        POLY_PRIVATE_KEY,
        POLY_FUNDER,
        POLY_API_KEY,
        POLY_SECRET,
        POLY_PASSPHRASE,
    ]
    if not all(required):
        print("[AUTO BUY] CLOB credentials incomplete -> DRY RUN", flush=True)
        return

    try:
        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds

        clob_client = ClobClient(
            host=CLOB_URL,
            chain_id=137,
            key=POLY_PRIVATE_KEY,
            creds=ApiCreds(
                api_key=POLY_API_KEY,
                api_secret=POLY_SECRET,
                api_passphrase=POLY_PASSPHRASE,
            ),
            signature_type=2,
            funder=POLY_FUNDER,
        )
        print("[AUTO BUY] CLOB client initialized.", flush=True)
    except Exception as e:
        print(f"[AUTO BUY] CLOB init error -> DRY RUN: {e}", file=sys.stderr, flush=True)


def place_buy_order_sync(token_id: str, shares: float, price: float):
    """Synchronous CLOB BUY; executed in the thread pool."""
    if clob_client is None:
        return {"success": False, "dry_run": True, "reason": "CLOB client unavailable"}

    from py_clob_client_v2.clob_types import OrderArgs, OrderType
    from py_clob_client_v2.order_builder.constants import BUY

    signed = clob_client.create_order(OrderArgs(
        token_id=token_id,
        size=shares,
        side=BUY,
        price=price,
    ))
    return clob_client.post_order(signed, OrderType.GTC)


async def get_midpoint(token_id: str) -> float:
    """Get current midpoint without blocking the event loop."""
    import urllib.request
    import urllib.parse

    def _fetch():
        url = CLOB_URL + "/midpoint?" + urllib.parse.urlencode({"token_id": token_id})
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as response:
            data = json.loads(response.read().decode("utf-8"))
        return float(data.get("mid", 0) or 0)

    try:
        return await asyncio.to_thread(_fetch)
    except Exception as e:
        print(f"[AUTO BUY] midpoint lookup failed: {e}", file=sys.stderr, flush=True)
        return 0.0


def get_alert_token_id(alert: dict) -> str:
    condition_id = alert["conditionId"]
    try:
        url = f"{GAMMA_URL}/markets?condition_ids={condition_id}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as response:
            markets = json.loads(response.read().decode("utf-8"))

        if not markets:
            print(f"[TOKEN LOOKUP] No market found for {condition_id}", flush=True)
            return {}

        market = markets[0]

        raw_outcomes = market.get("outcomes", [])
        raw_token_ids = market.get("clobTokenIds", [])

        # Gamma sometimes returns these as JSON-encoded strings, sometimes as
        # already-parsed lists — handle both.
        outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
        token_ids = json.loads(raw_token_ids) if isinstance(raw_token_ids, str) else raw_token_ids

        if not outcomes or not token_ids or len(outcomes) != len(token_ids):
            print(f"[TOKEN LOOKUP] Mismatched or empty outcomes/tokens for {condition_id}", flush=True)
            return {}

        result = dict(zip(outcomes, token_ids))

        return result

    except Exception as e:
        print(f"[TOKEN LOOKUP] Failed for {condition_id}: {e}", flush=True)
        return {}


def get_alert_price(alert: dict) -> float:
    for key in ("price", "current_price", "currentPrice", "entry_price"):
        value = alert.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
    return 0.0


async def auto_buy(alert: dict) -> dict:
    """Buy AUTO_BUY_USD worth of the signalled token.

    This function contains an explicit severity >= 8 gate even though the
    caller also gates, so a future refactor cannot accidentally bypass it.
    """
    if alert.get("side") != "BUY":
      raise ValueError("Signal is not a buy signal")
    
    severity = float(alert.get("severity_score", 0))
    if severity < AUTO_BUY_MIN_SEVERITY:
        return {"success": False, "skipped": True, "reason": f"severity {severity} < 8"}

    outcome_tokens = get_alert_token_id(alert)
    token_id = outcome_tokens.get(alert["outcome"])
  
    if not token_id:
        raise ValueError("Signal has no token ID / asset field")

    market_price = alert.get("current_price")

    if market_price <= 0:
        raise ValueError(f"No valid market price for token {token_id}")

    if market_price > AUTO_BUY_MAX_PRICE:
        return {
            "success": False,
            "skipped": True,
            "reason": f"price {market_price:.4f} > max {AUTO_BUY_MAX_PRICE:.4f}",
            "token_id": token_id,
            "price": market_price,
        }

    # OrderArgs.size is token shares, not USD. Convert the USD budget to shares.
    shares = round(AUTO_BUY_USD / market_price, 4)
    limit_price = round(min(AUTO_BUY_MAX_PRICE, market_price + AUTO_BUY_PRICE_OFFSET), 4)

    if shares <= 0:
        raise ValueError(f"Calculated share size is invalid: {shares}")

    if not AUTO_BUY_ENABLED:
        return {
            "success": True,
            "dry_run": True,
            "token_id": token_id,
            "price": limit_price,
            "market_price": market_price,
            "shares": shares,
            "usd_budget": AUTO_BUY_USD,
        }

    if clob_client is None:
        raise RuntimeError("AUTO_BUY_ENABLED=true but CLOB client is not initialized")

    loop = asyncio.get_running_loop()
    last_response = None

    for attempt in range(1, AUTO_BUY_RETRIES + 1):
        try:
            response = await loop.run_in_executor(
                executor,
                place_buy_order_sync,
                token_id,
                shares,
                limit_price,
            )
            last_response = response
            success = response.get("success", False) if isinstance(response, dict) else False

            if success:
                return {
                    "success": True,
                    "dry_run": False,
                    "attempt": attempt,
                    "response": response,
                    "token_id": token_id,
                    "price": limit_price,
                    "market_price": market_price,
                    "shares": shares,
                    "usd_budget": AUTO_BUY_USD,
                }

            print(f"[AUTO BUY] attempt {attempt}/{AUTO_BUY_RETRIES} rejected: {response}", flush=True)
        except Exception as e:
            last_response = str(e)
            print(f"[AUTO BUY] attempt {attempt}/{AUTO_BUY_RETRIES} exception: {e}", file=sys.stderr, flush=True)

        if attempt < AUTO_BUY_RETRIES:
            await asyncio.sleep(0.75)

    return {
        "success": False,
        "dry_run": False,
        "token_id": token_id,
        "price": limit_price,
        "market_price": market_price,
        "shares": shares,
        "usd_budget": AUTO_BUY_USD,
        "response": last_response,
    }


async def send_auto_buy_telegram(alert: dict, result: dict) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    import urllib.request

    status = "DRY RUN" if result.get("dry_run") else "LIVE BUY"
    title = str(alert.get("title", ""))[:120]
    outcome = alert.get("outcome", "")
    score = alert.get("severity_score", 0)
    price = result.get("price", 0)
    shares = result.get("shares", 0)
    token_id = result.get("token_id", "")

    if result.get("success"):
        message = (
            f"📈 *AUTO BUY — {status}*\n\n"
            f"Severity: *{score}/10*\n"
            f"Outcome: {outcome}\n"
            f"Market: {title}\n"
            f"Budget: ${AUTO_BUY_USD:.2f}\n"
            f"Shares: {shares:.4f}\n"
            f"Limit price: {price:.4f}\n"
            f"Token: `{token_id[:24]}...`"
        )
    else:
        message = (
            f"⚠️ *AUTO BUY SKIPPED/FAILED*\n\n"
            f"Severity: *{score}/10*\n"
            f"Market: {title}\n"
            f"Reason: {result.get('reason', result.get('response', 'unknown'))}"
        )

    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
    }).encode("utf-8")

    def _send():
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=8):
            pass

    try:
        await asyncio.to_thread(_send)
    except Exception as e:
        print(f"[AUTO BUY] Telegram error: {e}", file=sys.stderr, flush=True)


class TradeBuffer:
    """Sliding window of trades received live through WebSocket."""

    def __init__(self):
        self._trades: list[dict] = []

    def add(self, payload: dict) -> None:
        try:
            payload = dict(payload)
            payload["usd_value"] = float(payload.get("size", 0)) * float(payload.get("price", 0))
            self._trades.append(payload)
        except (TypeError, ValueError):
            pass

    def snapshot(self) -> list[dict]:
        cutoff = time.time() - REALTIME_BUFFER_MAX_AGE_SECONDS
        self._trades = [t for t in self._trades if t.get("timestamp", 0) >= cutoff]
        return list(self._trades)


async def listen(buffer: TradeBuffer) -> None:
    backoff = 2
    while True:
        try:
            async with websockets.connect(
                REALTIME_WS_URL, ping_interval=15, ping_timeout=10
            ) as ws:
                await ws.send(json.dumps(SUBSCRIBE_MESSAGE))
                print("[REALTIME] Connected and subscribed.", flush=True)
                backoff = 2
                last_message_time = time.time()

                async def watchdog():
                    while True:
                        await asyncio.sleep(30)
                        if time.time() - last_message_time > 5:
                            print("[REALTIME] No messages for 5s — forcing reconnect.", flush=True)
                            await ws.close()
                            return

                watchdog_task = asyncio.create_task(watchdog())

                try:
                    async for raw_message in ws:
                        last_message_time = time.time()
                        try:
                            message = json.loads(raw_message)
                        except json.JSONDecodeError:
                            continue
                        if message.get("topic") == "activity" and message.get("type") == "trades":
                            buffer.add(message.get("payload", {}))
                finally:
                    watchdog_task.cancel()

        except Exception as e:
            print(f"[REALTIME] Connection lost ({e}). Reconnecting in {backoff}s...", file=sys.stderr, flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)



async def periodic_scan(buffer: TradeBuffer) -> None:
    """Run detection/scoring periodically and auto-buy severity >= 8 signals."""
    state = load_state()
    init_clob_client()

    print(
        f"[REALTIME] Auto-buy={'LIVE' if AUTO_BUY_ENABLED else 'DRY RUN'} | "
        f"minimum auto-buy severity={AUTO_BUY_MIN_SEVERITY} | "
        f"budget=${AUTO_BUY_USD:.2f} | max price={AUTO_BUY_MAX_PRICE:.4f}",
        flush=True,
    )

    while True:
        await asyncio.sleep(REALTIME_SCORE_INTERVAL_SECONDS)

        try:
            n_commands = process_pending_commands(state)
            if n_commands:
                print(f"[REALTIME] {n_commands} Telegram command(s) processed.", flush=True)

            if maybe_send_weekly_report(state):
                print("[REALTIME] Weekly report sent.", flush=True)

            if FEEDBACK_ENABLED:
                review_short_term(state)
                review_long_term(state)

            if is_paused(state):
                save_state(state)
                continue

            trades = buffer.snapshot()
            if not trades:
                continue

            condition_ids = sorted({t.get("conditionId") for t in trades if t.get("conditionId")})
            metadata_by_market = fetch_market_metadata_batch_cached(condition_ids, state)
            market_thresholds = build_market_thresholds(metadata_by_market)

            alerts = detect_anomalies(trades, market_thresholds)
            candidate_alerts = filter_new_alerts(alerts, state)

            min_severity = get_min_severity(state, MIN_SEVERITY_SCORE)
            new_alerts = []

            for alert in candidate_alerts:
                metadata = metadata_by_market.get(alert["conditionId"], {})

                reputation = (
                    assess_wallet_freshness(alert["wallets"])
                    if WALLET_REPUTATION_ENABLED else None
                )
                cross_market_wallets = find_cross_market_wallets(
                    state, alert["wallets"], alert["conditionId"]
                )
                news = (
                    check_recent_news(alert.get("title", ""))
                    if NEWS_CHECK_ENABLED else None
                )
                conflicting_signal = find_conflicting_market_signal(
                    state,
                    alert["conditionId"],
                    alert["outcome"],
                    alert["side"],
                )

                scored = enrich_and_score(
                    alert,
                    metadata,
                    reputation,
                    cross_market_wallets,
                    news,
                    conflicting_signal,
                )

                severity = float(scored.get("severity_score", 0))
                print(
                    f"[REALTIME] {scored['type']} on '{scored.get('title')}' "
                    f"-> score {severity}/10 ({scored['severity_label']})",
                    flush=True,
                )

                record_wallet_activity(
                    state,
                    alert["wallets"],
                    alert["conditionId"],
                )

                if severity >= min_severity:
                    new_alerts.append(scored)
                    record_market_signal(
                        state,
                        alert["conditionId"],
                        alert["outcome"],
                        alert["side"],
                        severity,
                    )

                # ── HARD AUTO-BUY GATE: severity must be >= 8 ───────────────
                if severity >= AUTO_BUY_MIN_SEVERITY:
                    try:
                        result = await auto_buy(scored)
                        if result.get("success"):
                            mode = "DRY RUN" if result.get("dry_run") else "LIVE"
                            print(
                                f"[REALTIME] AUTO BUY {mode}: "
                                f"${result.get('usd_budget', AUTO_BUY_USD):.2f} "
                                f"/ {result.get('shares', 0):.4f} shares "
                                f"@ {result.get('price', 0):.4f}",
                                flush=True,
                            )
                        else:
                            print(
                                f"[REALTIME] AUTO BUY skipped/failed: {result}",
                                file=sys.stderr,
                                flush=True,
                            )
                        await send_auto_buy_telegram(scored, result)
                    except Exception as e:
                        print(
                            f"[REALTIME] AUTO BUY ERROR: {e}",
                            file=sys.stderr,
                            flush=True,
                        )
                        await send_auto_buy_telegram(scored, {
                            "success": False,
                            "reason": str(e),
                        })

            if new_alerts:
                notify_alerts(new_alerts)
                print(f"[REALTIME] {len(new_alerts)} alert(s) sent.", flush=True)
                if FEEDBACK_ENABLED:
                    for alert in new_alerts:
                        record_alert_outcome(state, alert)

            save_state(state)

        except Exception as e:
            print(
                f"[REALTIME] Error during scoring cycle: {e}",
                file=sys.stderr,
                flush=True,
            )


async def main() -> None:
    buffer = TradeBuffer()
    await asyncio.gather(
        listen(buffer),
        periodic_scan(buffer),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[REALTIME] Stop requested, shutting down.")
