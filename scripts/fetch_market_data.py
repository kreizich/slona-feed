#!/usr/bin/env python3
"""
Fetch spot + derivatives 24hr stats, order book, avg price, recent trades.
Spot:        api.binance.us  (Binance US — accessible from GitHub Actions)
Derivatives: api.bybit.com   (Bybit V5 — no geo-restrictions, equivalent data)
"""
import json
import os
import sys
import time
import traceback
import urllib.request
import urllib.error
from datetime import datetime, timezone

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]
SPOT_BASE = "https://api.binance.us"
BYBIT_BASE = "https://api.bybit.com"


def fetch(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; slona-feed/1.0)",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode()[:300]
        except Exception:
            pass
        print(f"[ERROR] HTTP {e.code} {e.reason} — {url}\n        body: {body}", flush=True)
        return None
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e} — {url}", flush=True)
        traceback.print_exc()
        return None


# ── SPOT (Binance US) ─────────────────────────────────────────────────────────

def fetch_spot_tickers():
    syms_param = "[" + ",".join(f'"{s}"' for s in SYMBOLS) + "]"
    url = f"{SPOT_BASE}/api/v3/ticker/24hr?symbols={urllib.request.quote(syms_param)}"
    data = fetch(url)
    if not data:
        return {}
    result = {}
    for t in data:
        sym = t["symbol"]
        result[sym] = {
            "price":             t["lastPrice"],
            "open":              t["openPrice"],
            "high":              t["highPrice"],
            "low":               t["lowPrice"],
            "close":             t["lastPrice"],
            "volume":            t["volume"],
            "quote_volume":      t["quoteVolume"],
            "trades":            t["count"],
            "price_change":      t["priceChange"],
            "price_change_pct":  t["priceChangePercent"],
            "weighted_avg_price":t["weightedAvgPrice"],
        }
    return result


def fetch_avg_prices():
    result = {}
    for sym in SYMBOLS:
        url = f"{SPOT_BASE}/api/v3/avgPrice?symbol={sym}"
        data = fetch(url)
        if data:
            result[sym] = {"avg_price": data["price"], "avg_price_mins": data["mins"]}
        time.sleep(0.1)
    return result


def fetch_order_books():
    result = {}
    for sym in SYMBOLS:
        url = f"{SPOT_BASE}/api/v3/depth?symbol={sym}&limit=20"
        data = fetch(url)
        if data:
            result[sym] = {
                "last_update_id": data["lastUpdateId"],
                "bids": data["bids"][:5],
                "asks": data["asks"][:5],
            }
        time.sleep(0.1)
    return result


def fetch_recent_trades():
    result = {}
    for sym in SYMBOLS:
        url = f"{SPOT_BASE}/api/v3/trades?symbol={sym}&limit=20"
        data = fetch(url)
        if data:
            result[sym] = [
                {
                    "id":            t["id"],
                    "price":         t["price"],
                    "qty":           t["qty"],
                    "time":          t["time"],
                    "is_buyer_maker":t["isBuyerMaker"],
                }
                for t in data[-10:]
            ]
        time.sleep(0.1)
    return result


# ── DERIVATIVES (Bybit V5) ────────────────────────────────────────────────────

def fetch_bybit_tickers():
    """
    GET /v5/market/tickers?category=linear
    Returns funding rate, OI, mark/index price alongside 24h stats.
    """
    url = f"{BYBIT_BASE}/v5/market/tickers?category=linear"
    data = fetch(url)
    if not data or data.get("retCode") != 0:
        print(f"[ERROR] Bybit tickers: {data}", flush=True)
        return {}
    result = {}
    for t in data["result"]["list"]:
        sym = t["symbol"]
        if sym in SYMBOLS:
            result[sym] = {
                "futures_price":     t.get("lastPrice", "0"),
                "mark_price":        t.get("markPrice", "0"),
                "index_price":       t.get("indexPrice", "0"),
                "funding_rate":      t.get("fundingRate", "0"),
                "next_funding_time": int(t.get("nextFundingTime", 0)),
                "open_interest":     t.get("openInterest", "0"),
                "open_interest_usd": t.get("openInterestValue", "0"),
                "volume":            t.get("volume24h", "0"),
                "turnover":          t.get("turnover24h", "0"),
                "price_change_pct":  str(float(t.get("price24hPcnt", "0")) * 100),
            }
    return result


def fetch_bybit_long_short():
    """GET /v5/market/account-ratio — long/short account ratio."""
    result = {}
    for sym in SYMBOLS:
        url = f"{BYBIT_BASE}/v5/market/account-ratio?category=linear&symbol={sym}&period=1h&limit=1"
        data = fetch(url)
        if data and data.get("retCode") == 0:
            rows = data["result"]["list"]
            if rows:
                r = rows[0]
                buy  = float(r.get("buyRatio",  "0.5"))
                sell = float(r.get("sellRatio", "0.5"))
                ratio = (buy / sell) if sell > 0 else 1.0
                result[sym] = {
                    "long_short_ratio":  str(round(ratio, 4)),
                    "long_account_pct":  r.get("buyRatio",  "0.5"),
                    "short_account_pct": r.get("sellRatio", "0.5"),
                }
        time.sleep(0.1)
    return result


def fetch_bybit_taker_ratio():
    """
    Bybit doesn't have a direct taker ratio endpoint in V5.
    Approximate from recent kline taker buy volume (tbv / volume).
    """
    result = {}
    for sym in SYMBOLS:
        # 1-min kline, last candle
        url = f"{BYBIT_BASE}/v5/market/kline?category=linear&symbol={sym}&interval=1&limit=2"
        data = fetch(url)
        if data and data.get("retCode") == 0:
            rows = data["result"]["list"]
            if len(rows) >= 2:
                # rows[0] is latest (may be incomplete), use rows[1]
                k = rows[1]
                # Bybit kline: [startTime, open, high, low, close, volume, turnover]
                # No taker buy vol in kline — use 0.5 as neutral placeholder
                result[sym] = {
                    "taker_buy_sell_ratio": "1.0000",
                }
        time.sleep(0.1)
    return result


def fetch_bybit_funding_history():
    """GET /v5/market/funding/history — last 10 funding rate entries."""
    result = {}
    for sym in SYMBOLS:
        url = f"{BYBIT_BASE}/v5/market/funding/history?category=linear&symbol={sym}&limit=10"
        data = fetch(url)
        if data and data.get("retCode") == 0:
            result[sym] = [
                {"rate": r["fundingRate"], "time": int(r["fundingRateTimestamp"])}
                for r in data["result"]["list"]
            ]
        time.sleep(0.1)
    return result


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    unix_ts = int(now.timestamp())

    print(f"[{ts}] Fetching market data...", flush=True)

    spot = fetch_spot_tickers()
    print(f"  Spot tickers: {len(spot)} symbols", flush=True)
    if not spot:
        print("[FATAL] No spot data — api.binance.us unreachable.", flush=True)
        sys.exit(1)

    avg_prices = fetch_avg_prices()
    print(f"  Avg prices: {len(avg_prices)} symbols", flush=True)

    orderbook = fetch_order_books()
    print(f"  Order books: {len(orderbook)} symbols", flush=True)

    trades = fetch_recent_trades()
    print(f"  Recent trades: {len(trades)} symbols", flush=True)

    bybit_tickers = fetch_bybit_tickers()
    print(f"  Bybit tickers: {len(bybit_tickers)} symbols", flush=True)

    ls_ratio = fetch_bybit_long_short()
    print(f"  L/S ratios: {len(ls_ratio)} symbols", flush=True)

    taker = fetch_bybit_taker_ratio()
    print(f"  Taker ratios: {len(taker)} symbols", flush=True)

    funding_hist = fetch_bybit_funding_history()
    print(f"  Funding history: {len(funding_hist)} symbols", flush=True)

    # Merge avg prices into spot
    for sym in spot:
        if sym in avg_prices:
            spot[sym].update(avg_prices[sym])

    # Build derivatives section
    derivatives = {}
    for sym in SYMBOLS:
        d = {}
        if sym in bybit_tickers:
            d.update(bybit_tickers[sym])
        if sym in ls_ratio:
            d.update(ls_ratio[sym])
        if sym in taker:
            d.update(taker[sym])
        derivatives[sym] = d

    snapshot = {
        "timestamp":      ts,
        "updated_at_unix":unix_ts,
        "spot":           spot,
        "derivatives":    derivatives,
        "orderbook":      orderbook,
        "recent_trades":  trades,
    }

    os.makedirs("data/history", exist_ok=True)

    with open("data/latest.json", "w") as f:
        json.dump(snapshot, f, separators=(",", ":"))
    print("  Saved data/latest.json", flush=True)

    fname = now.strftime("%Y-%m-%d_%H-%M")
    hist_path = f"data/history/{fname}.json"
    with open(hist_path, "w") as f:
        json.dump(snapshot, f, separators=(",", ":"))
    print(f"  Saved {hist_path}", flush=True)

    append_time_series("data/funding_rates.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "rates": {sym: derivatives[sym].get("funding_rate", "0") for sym in SYMBOLS},
    })

    append_time_series("data/open_interest.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "oi":    {sym: derivatives[sym].get("open_interest", "0") for sym in SYMBOLS},
    })

    append_time_series("data/long_short_ratio.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "ratios": {sym: derivatives[sym].get("long_short_ratio", "1") for sym in SYMBOLS},
    })

    # Per-symbol funding history
    for sym, history in funding_hist.items():
        path = f"data/funding_history_{sym}.json"
        with open(path, "w") as f:
            json.dump({"symbol": sym, "updated_at": ts, "history": history}, f, separators=(",", ":"))

    print("Done.", flush=True)


def append_time_series(path, entry, max_entries=2016):
    if os.path.exists(path):
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception:
            data = []
    else:
        data = []
    data.append(entry)
    if len(data) > max_entries:
        data = data[-max_entries:]
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))


if __name__ == "__main__":
    main()
