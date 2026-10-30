"""
Hyperliquid -> Telegram price alert bot.

Sends a Telegram message whenever a coin's price moves more than
THRESHOLD_PCT (default 3%) within WINDOW_SEC (default 5 minutes).

Setup:
    pip install requests
    export TELEGRAM_BOT_TOKEN="123456:ABC..."   # from @BotFather
    export TELEGRAM_CHAT_ID="123456789"         # your chat id (see README)
    python hl_alert_bot.py
"""
import logging
import os
import time
from collections import defaultdict, deque

import requests

HL_URL = "https://api.hyperliquid.xyz/info"

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

THRESHOLD_PCT = float(os.getenv("THRESHOLD_PCT", "2"))   # alert if move > 3%
WINDOW_SEC = int(os.getenv("WINDOW_SEC", "300"))         # within 5 minutes
POLL_SEC = int(os.getenv("POLL_SEC", "10"))              # check every 10s
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "300"))     # per-coin re-alert delay
MAX_RUNTIME_SEC = int(os.getenv("MAX_RUNTIME_SEC", "0"))  # 0 = run forever
SEND_STARTUP = os.getenv("SEND_STARTUP", "1") == "1"
INCLUDE_SPOT = os.getenv("INCLUDE_SPOT", "0") == "1"     # spot pairs look like "@107"
INCLUDE_OUTCOMES = os.getenv("INCLUDE_OUTCOMES", "0") == "1"  # prediction markets like "#79471"
# Optional: only watch these coins, e.g. WATCHLIST="BTC,ETH,SOL"
WATCHLIST = {c.strip().upper() for c in os.getenv("WATCHLIST", "").split(",") if c.strip()}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hl-bot")

history = defaultdict(deque)   # coin -> deque[(timestamp, price)]
last_alert = {}                # coin -> timestamp of last alert


def fetch_mids():
    r = requests.post(HL_URL, json={"type": "allMids"}, timeout=10)
    r.raise_for_status()
    out = {}
    for coin, px in r.json().items():
        if not INCLUDE_SPOT and coin.startswith("@"):
            continue
        if not INCLUDE_OUTCOMES and coin.startswith("#"):
            continue
        if WATCHLIST and coin.upper() not in WATCHLIST:
            continue
        try:
            out[coin] = float(px)
        except (TypeError, ValueError):
            continue
    return out


def send_telegram(text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=10,
    )
    if not r.ok:
        log.error("Telegram error %s: %s", r.status_code, r.text)


def hl_info(payload):
    r = requests.post(HL_URL, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()


def fmt_usd(x):
    if x is None:
        return "n/a"
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if x >= div:
            return f"${x / div:.2f}{suffix}"
    return f"${x:,.0f}"


def get_ctx(coin):
    """Return the asset context (funding, 24h volume, ...) for a perp, or None."""
    if ":" in coin:  # builder-deployed perps live on other dexes; skipped
        return None
    try:
        meta, ctxs = hl_info({"type": "metaAndAssetCtxs"})
        for asset, ctx in zip(meta["universe"], ctxs):
            if asset["name"] == coin:
                return ctx
    except Exception as e:
        log.warning("asset ctx failed for %s: %s", coin, e)
    return None


def get_volumes(coin, now, ctx):
    """Return (volume_5m, volume_24h) in USD notional (None if unavailable)."""
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
        else:  # fallback: sum 1h candles
            candles = hl_info({"type": "candleSnapshot", "req": {
                "coin": coin, "interval": "1h",
                "startTime": end - 24 * 3600 * 1000, "endTime": end}})
            vol24 = sum(float(c["v"]) * float(c["c"]) for c in candles)
    except Exception as e:
        log.warning("24h volume failed for %s: %s", coin, e)

    return vol5, vol24


def binance_symbols(coin):
    """Candidate Binance symbols for a Hyperliquid coin (kPEPE -> 1000PEPEUSDT)."""
    base = coin.upper()
    cands = []
    if len(coin) > 1 and coin[0] == "k" and coin[1].isupper():
        cands.append("1000" + coin[1:].upper() + "USDT")
        cands.append(coin[1:].upper() + "USDT")
    else:
        cands.append(base + "USDT")
    return cands


def get_binance_price(coin):
    """Binance futures price, falling back to spot. Returns float or None."""
    urls = [
        "https://fapi.binance.com/fapi/v1/ticker/price",      # futures
        "https://data-api.binance.vision/api/v3/ticker/price",  # spot (not geo-blocked)
    ]
    for symbol in binance_symbols(coin):
        for url in urls:
            try:
                r = requests.get(url, params={"symbol": symbol}, timeout=5)
                if r.ok:
                    return float(r.json()["price"])
            except Exception:
                continue
    return None


def fmt_price(p):
    if p >= 100:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.6g}"


def check(coin, price, now):
    dq = history[coin]
    dq.append((now, price))
    while dq and now - dq[0][0] > WINDOW_SEC:
        dq.popleft()

    if len(dq) < 2:
        return

    prices = [p for _, p in dq]
    lo, hi = min(prices), max(prices)
    up = (price - lo) / lo * 100 if lo else 0     # rise from window low
    down = (price - hi) / hi * 100 if hi else 0   # drop from window high
    change = up if abs(up) >= abs(down) else down

    if abs(change) <= THRESHOLD_PCT:
        return
    if now - last_alert.get(coin, 0) < COOLDOWN_SEC:
        return

    last_alert[coin] = now
    ref = lo if change > 0 else hi
    arrow = "🟢" if change > 0 else "🔴"
    minutes = (now - dq[0][0]) / 60

    ctx = get_ctx(coin)
    vol5, vol24 = get_volumes(coin, now, ctx)

    bn = get_binance_price(coin)
    if bn:
        diff = (price - bn) / bn * 100
        bn_line = f"Binance: {fmt_price(bn)} (HL {diff:+.2f}%)"
    else:
        bn_line = "Binance: n/a"

    try:
        funding = float(ctx["funding"]) * 100 if ctx else None  # per hour, in %
    except (KeyError, TypeError, ValueError):
        funding = None
    try:
        oi = float(ctx["openInterest"]) * float(ctx["markPx"]) if ctx else None  # USD
    except (KeyError, TypeError, ValueError):
        oi = None
    fund_line = f"Fd: {funding:+.4f}%/h" if funding is not None else "Fd: n/a"

    msg = (
        f"{arrow} <b>{coin}</b> {change:+.2f}% in {minutes:.1f} min "
        f"{fmt_price(ref)} → <b>{fmt_price(price)}</b>\n"
        f"{bn_line}\n"
        f"{fund_line}\n"
        f"Vol 5m: <b>{fmt_usd(vol5)}</b> | 24h: <b>{fmt_usd(vol24)}</b> | OI: <b>{fmt_usd(oi)}</b>\n"
        f'<a href="https://app.hyperliquid.xyz/trade/{coin}">HL</a>'
    )
    log.info("ALERT %s %+.2f%%", coin, change)
    send_telegram(msg)


def main():
    if not TOKEN or not CHAT_ID:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables.")

    if SEND_STARTUP:
        send_telegram(
            f"✅ Hyperliquid alert bot started\n"
            f"Alert when price moves &gt;{THRESHOLD_PCT}% within {WINDOW_SEC // 60} min."
        )
    log.info("Started. threshold=%s%% window=%ss poll=%ss", THRESHOLD_PCT, WINDOW_SEC, POLL_SEC)

    deadline = time.time() + MAX_RUNTIME_SEC if MAX_RUNTIME_SEC else None
    while deadline is None or time.time() < deadline:
        started = time.time()
        try:
            mids = fetch_mids()
            now = time.time()
            for coin, price in mids.items():
                check(coin, price, now)
        except requests.RequestException as e:
            log.warning("Network error: %s", e)
        except Exception:
            log.exception("Unexpected error")
        time.sleep(max(0.0, POLL_SEC - (time.time() - started)))


if __name__ == "__main__":
    main()
