#!/usr/bin/env python3
"""
Fetch spot market data + CoinGecko market overview.
Spot prices/orderbook/trades: api.binance.us  (Binance US, no geo-block)
Market cap / ATH / supply:    api.coingecko.com (free, no key needed)
"""
import json
import os
import sys
import time
import traceback
import urllib.request
import urllib.error
from datetime import datetime, timezone

SYMBOLS   = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]
SPOT_BASE = "https://api.binance.us"
CG_BASE   = "https://api.coingecko.com/api/v3"

CG_IDS = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "XRPUSDT": "ripple",
    "BNBUSDT": "binancecoin",
}


def fetch(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
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
        print(f"[ERROR] HTTP {e.code} {e.reason} — {url}\n        body: {body[:120]}", flush=True)
        return None
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e} — {url}", flush=True)
        traceback.print_exc()
        return None


# ── Binance US spot ───────────────────────────────────────────────────────────

def fetch_spot_tickers():
    syms_param = "[" + ",".join(f'"{s}"' for s in SYMBOLS) + "]"
    url  = f"{SPOT_BASE}/api/v3/ticker/24hr?symbols={urllib.request.quote(syms_param)}"
    data = fetch(url)
    if not data:
        return {}
    result = {}
    for t in data:
        sym = t["symbol"]
        result[sym] = {
            "price":              t["lastPrice"],
            "open":               t["openPrice"],
            "high":               t["highPrice"],
            "low":                t["lowPrice"],
            "close":              t["lastPrice"],
            "volume":             t["volume"],
            "quote_volume":       t["quoteVolume"],
            "trades":             t["count"],
            "price_change":       t["priceChange"],
            "price_change_pct":   t["priceChangePercent"],
            "weighted_avg_price": t["weightedAvgPrice"],
        }
    return result


def fetch_avg_prices():
    result = {}
    for sym in SYMBOLS:
        url  = f"{SPOT_BASE}/api/v3/avgPrice?symbol={sym}"
        data = fetch(url)
        if data:
            result[sym] = {"avg_price": data["price"], "avg_price_mins": data["mins"]}
        time.sleep(0.1)
    return result


def fetch_order_books():
    result = {}
    for sym in SYMBOLS:
        url  = f"{SPOT_BASE}/api/v3/depth?symbol={sym}&limit=20"
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
        url  = f"{SPOT_BASE}/api/v3/trades?symbol={sym}&limit=20"
        data = fetch(url)
        if data:
            result[sym] = [
                {
                    "id":             t["id"],
                    "price":          t["price"],
                    "qty":            t["qty"],
                    "time":           t["time"],
                    "is_buyer_maker": t["isBuyerMaker"],
                }
                for t in data[-10:]
            ]
        time.sleep(0.1)
    return result


# ── CoinGecko market overview ─────────────────────────────────────────────────

def fetch_coingecko_markets():
    """
    GET /coins/markets — market cap, rank, ATH, circulating supply, etc.
    One request for all symbols.
    """
    ids = ",".join(CG_IDS.values())
    url = (
        f"{CG_BASE}/coins/markets"
        f"?vs_currency=usd"
        f"&ids={ids}"
        f"&price_change_percentage=1h,24h,7d"
        f"&sparkline=false"
    )
    data = fetch(url)
    if not data:
        return {}
    result = {}
    id_to_sym = {v: k for k, v in CG_IDS.items()}
    for coin in data:
        sym = id_to_sym.get(coin["id"])
        if not sym:
            continue
        result[sym] = {
            "market_cap":               coin.get("market_cap", 0),
            "market_cap_rank":          coin.get("market_cap_rank", 0),
            "fully_diluted_valuation":  coin.get("fully_diluted_valuation", 0),
            "circulating_supply":       coin.get("circulating_supply", 0),
            "total_supply":             coin.get("total_supply", 0),
            "max_supply":               coin.get("max_supply", 0),
            "ath":                      coin.get("ath", 0),
            "ath_change_pct":           coin.get("ath_change_percentage", 0),
            "ath_date":                 coin.get("ath_date", ""),
            "price_change_pct_1h":      coin.get("price_change_percentage_1h_in_currency", 0),
            "price_change_pct_7d":      coin.get("price_change_percentage_7d_in_currency", 0),
        }
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    now     = datetime.now(timezone.utc)
    ts      = now.strftime("%Y-%m-%dT%H:%M:%SZ")
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

    cg = fetch_coingecko_markets()
    print(f"  CoinGecko market data: {len(cg)} symbols", flush=True)

    # Merge avg prices and CoinGecko into spot
    for sym in spot:
        if sym in avg_prices:
            spot[sym].update(avg_prices[sym])
        if sym in cg:
            spot[sym].update(cg[sym])

    # Preserve existing derivatives if present — fetch_derivatives.py owns that section
    existing_derivatives = {sym: {} for sym in SYMBOLS}
    existing_klines = {}
    latest_path = "data/latest.json"
    if os.path.exists(latest_path):
        try:
            with open(latest_path) as f:
                prev = json.load(f)
            existing_derivatives = prev.get("derivatives", existing_derivatives)
            existing_klines      = prev.get("klines", {})
            print("  Preserved existing derivatives and klines from latest.json", flush=True)
        except Exception:
            pass

    snapshot = {
        "timestamp":       ts,
        "updated_at_unix": unix_ts,
        "spot":            spot,
        "derivatives":     existing_derivatives,
        "orderbook":       orderbook,
        "recent_trades":   trades,
    }
    if existing_klines:
        snapshot["klines"] = existing_klines

    os.makedirs("data/history", exist_ok=True)

    with open(latest_path, "w") as f:
        json.dump(snapshot, f, separators=(",", ":"))
    print("  Saved data/latest.json", flush=True)

    fname     = now.strftime("%Y-%m-%d_%H-%M")
    hist_path = f"data/history/{fname}.json"
    with open(hist_path, "w") as f:
        json.dump(snapshot, f, separators=(",", ":"))
    print(f"  Saved {hist_path}", flush=True)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()
