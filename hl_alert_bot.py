"""
Hyperliquid -> Telegram price alert bot (WebSocket version).

Alerts when a coin moves more than THRESHOLD_PCT (default 2%) within
WINDOW_SEC (default 5 minutes). Prices stream live over Hyperliquid's WebSocket.

    pip install -r requirements.txt
    set TELEGRAM_BOT_TOKEN=...   set TELEGRAM_CHAT_ID=...
    python hl_alert_bot.py
"""
import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque

import requests
import websockets

HL_URL = "https://api.hyperliquid.xyz/info"
WS_URL = os.getenv("HL_WS_URL", "wss://api.hyperliquid.xyz/ws")

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

THRESHOLD_PCT = float(os.getenv("THRESHOLD_PCT", "2"))      # alert if move > 2%
WINDOW_SEC = int(os.getenv("WINDOW_SEC", "300"))            # within 5 minutes
SAMPLE_SEC = float(os.getenv("SAMPLE_SEC", "1"))            # store 1 price/sec per coin
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "300"))        # per-coin re-alert delay
MAX_RUNTIME_SEC = int(os.getenv("MAX_RUNTIME_SEC", "0"))    # 0 = run forever
SEND_STARTUP = os.getenv("SEND_STARTUP", "1") == "1"
INCLUDE_SPOT = os.getenv("INCLUDE_SPOT", "0") == "1"        # "@107" style spot pairs
INCLUDE_OUTCOMES = os.getenv("INCLUDE_OUTCOMES", "0") == "1"  # "#79471" prediction markets
WATCHLIST = {c.strip().upper() for c in os.getenv("WATCHLIST", "").split(",") if c.strip()}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hl-bot")

history = defaultdict(deque)   # coin -> deque[(timestamp, price)]
last_alert = {}                # coin -> timestamp of last alert
_ctx_cache = {"t": 0.0, "data": None}


# ---------- HTTP helpers (run in worker threads) ----------

def hl_info(payload):
    r = requests.post(HL_URL, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()


def send_telegram(text):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=10,
        )
        if not r.ok:
            log.error("Telegram error %s: %s", r.status_code, r.text)
    except requests.RequestException as e:
        log.error("Telegram request failed: %s", e)


def fmt_usd(x):
    if x is None:
        return "n/a"
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if x >= div:
            return f"${x / div:.2f}{suffix}"
    return f"${x:,.0f}"


def fmt_price(p):
    if p >= 100:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.6g}"


def get_ctx(coin):
    """Asset context (funding, OI, 24h volume) for a perp. Cached 10s to save API weight."""
    if ":" in coin:  # builder-deployed perps live on other dexes; skipped
        return None
    try:
        if time.time() - _ctx_cache["t"] > 10 or _ctx_cache["data"] is None:
            _ctx_cache["data"] = hl_info({"type": "metaAndAssetCtxs"})
            _ctx_cache["t"] = time.time()
        meta, ctxs = _ctx_cache["data"]
        for asset, ctx in zip(meta["universe"], ctxs):
            if asset["name"] == coin:
                return ctx
    except Exception as e:
        log.warning("asset ctx failed for %s: %s", coin, e)
    return None


def get_volumes(coin, now, ctx):
    """(volume_5m, volume_24h) in USD notional; None if unavailable."""
    end = int(now * 1000)
    vol5 = vol24 = None
    try:
        candles = hl_info({"type": "candleSnapshot", "req": {
            "coin": coin, "interval": "1m",
            "startTime": end - 5 * 60 * 1000, "endTime": end}})
        vol5 = sum(float(c["v"]) * float(c["c"]) for c in candles)
    except Exception as e:
        log.warning("5m volume failed for %s: %s", coin, e)
    try:
        if ctx is not None:
            vol24 = float(ctx["dayNtlVlm"])
        else:
            candles = hl_info({"type": "candleSnapshot", "req": {
                "coin": coin, "interval": "1h",
                "startTime": end - 24 * 3600 * 1000, "endTime": end}})
            vol24 = sum(float(c["v"]) * float(c["c"]) for c in candles)
    except Exception as e:
        log.warning("24h volume failed for %s: %s", coin, e)
    return vol5, vol24


def binance_symbols(coin):
    """kPEPE -> 1000PEPEUSDT, ENA -> ENAUSDT."""
    if len(coin) > 1 and coin[0] == "k" and coin[1].isupper():
        return ["1000" + coin[1:].upper() + "USDT", coin[1:].upper() + "USDT"]
    return [coin.upper() + "USDT"]


def get_binance_price(coin):
    """Binance futures price, falling back to spot."""
    urls = ["https://fapi.binance.com/fapi/v1/ticker/price",
            "https://data-api.binance.vision/api/v3/ticker/price"]
    for symbol in binance_symbols(coin):
        for url in urls:
            try:
                r = requests.get(url, params={"symbol": symbol}, timeout=5)
                if r.ok:
                    return float(r.json()["price"])
            except Exception:
                continue
    return None


def build_message(coin, price, ref, change, minutes, now):
    arrow = "🟢" if change > 0 else "🔴"
    ctx = get_ctx(coin)
    vol5, vol24 = get_volumes(coin, now, ctx)

    bn = get_binance_price(coin)
    if bn:
        bn_line = f"Binance: {fmt_price(bn)} (HL {(price - bn) / bn * 100:+.2f}%)"
    else:
        bn_line = "Binance: n/a"

    try:
        fd = f"{float(ctx['funding']) * 100:+.4f}%/ h" if ctx else "n/a"
    except (KeyError, TypeError, ValueError):
        fd = "n/a"
    try:
        oi = float(ctx["openInterest"]) * float(ctx["markPx"]) if ctx else None
    except (KeyError, TypeError, ValueError):
        oi = None

    return (
        f"{arrow} <b>{coin}</b> {change:+.2f}% in {minutes:.1f}m "
        f"{fmt_price(ref)} → <b>{fmt_price(price)}</b>\n"
        f"{bn_line}\n"
        f"FD: {fd} | OI: <b>{fmt_usd(oi)}</b>\n"
        f"Vol 5m: <b>{fmt_usd(vol5)}</b> | 24h: <b>{fmt_usd(vol24)}</b>\n"
        f'<a href="https://app.hyperliquid.xyz/trade/{coin}">HL</a>'
    )


# ---------- price tracking ----------

def wanted(coin):
    if not INCLUDE_SPOT and coin.startswith("@"):
        return False
    if not INCLUDE_OUTCOMES and coin.startswith("#"):
        return False
    if WATCHLIST and coin.upper() not in WATCHLIST:
        return False
    return True


def update(coin, price, now):
    """Record a price; return (ref, change_pct, minutes) if it should alert, else None."""
    dq = history[coin]
    while dq and now - dq[0][0] > WINDOW_SEC:
        dq.popleft()
    if not dq or now - dq[-1][0] >= SAMPLE_SEC:
        dq.append((now, price))
    if len(dq) < 2:
        return None

    prices = [p for _, p in dq]
    lo, hi = min(prices), max(prices)
    up = (price - lo) / lo * 100 if lo else 0       # rise from window low
    down = (price - hi) / hi * 100 if hi else 0     # drop from window high
    change = up if abs(up) >= abs(down) else down

    if abs(change) <= THRESHOLD_PCT:
        return None
    if now - last_alert.get(coin, 0) < COOLDOWN_SEC:
        return None

    last_alert[coin] = now
    ref = lo if change > 0 else hi
    return ref, change, (now - dq[0][0]) / 60


# ---------- WebSocket loop ----------

sem = None
pending = set()


async def alert_worker(coin, price, ref, change, minutes, now):
    async with sem:
        log.info("ALERT %s %+.2f%%", coin, change)
        msg = await asyncio.to_thread(build_message, coin, price, ref, change, minutes, now)
        await asyncio.to_thread(send_telegram, msg)


def handle_mids(mids):
    now = time.time()
    for coin, px in mids.items():
        if not wanted(coin):
            continue
        try:
            price = float(px)
        except (TypeError, ValueError):
            continue
        sig = update(coin, price, now)
        if sig:
            ref, change, minutes = sig
            t = asyncio.create_task(alert_worker(coin, price, ref, change, minutes, now))
            pending.add(t)
            t.add_done_callback(pending.discard)


async def heartbeat(ws):
    while True:
        await asyncio.sleep(30)
        await ws.send(json.dumps({"method": "ping"}))


async def stream():
    backoff = 1
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20,
                                          max_size=None) as ws:
                await ws.send(json.dumps(
                    {"method": "subscribe", "subscription": {"type": "allMids"}}))
                log.info("WebSocket connected")
                backoff = 1
                pinger = asyncio.create_task(heartbeat(ws))
                try:
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("channel") == "allMids":
                            handle_mids(msg["data"]["mids"])
                finally:
                    pinger.cancel()
        except Exception as e:
            log.warning("WebSocket dropped (%s); reconnecting in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def main():
    global sem
    if not TOKEN or not CHAT_ID:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables.")
    sem = asyncio.Semaphore(3)

    if SEND_STARTUP:
        await asyncio.to_thread(
            send_telegram,
            f"✅ Hyperliquid alert bot started\n"
            f"Alert when price moves &gt;{THRESHOLD_PCT:g}% within {WINDOW_SEC // 60} min.")
    log.info("Started. threshold=%s%% window=%ss", THRESHOLD_PCT, WINDOW_SEC)

    try:
        if MAX_RUNTIME_SEC:
            await asyncio.wait_for(stream(), MAX_RUNTIME_SEC)
        else:
            await stream()
    except asyncio.TimeoutError:
        log.info("Max runtime reached, exiting so the next run can take over.")
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
