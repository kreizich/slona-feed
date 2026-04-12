#!/usr/bin/env python3
"""
Fetch futures derivatives data: funding rates, open interest, long/short ratios.
Appends to history JSON files.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]
FAPI_BASE = "https://fapi.binance.com"
MAX_ENTRIES = 2016  # ~7 days at 5-min intervals


def fetch(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "slona-feed/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} for {url}: {e.reason}")
        return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def append_ts(path, entry, max_entries=MAX_ENTRIES):
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


def fetch_funding_history():
    """Fetch recent funding rate history (last 10 entries per symbol)."""
    result = {}
    for sym in SYMBOLS:
        url = f"{FAPI_BASE}/fapi/v1/fundingRate?symbol={sym}&limit=10"
        data = fetch(url)
        if data:
            result[sym] = [
                {
                    "rate": d["fundingRate"],
                    "time": d["fundingTime"],
                }
                for d in data
            ]
        time.sleep(0.15)
    return result


def fetch_premium_index():
    """Fetch mark price, index price, next funding rate."""
    result = {}
    for sym in SYMBOLS:
        url = f"{FAPI_BASE}/fapi/v1/premiumIndex?symbol={sym}"
        data = fetch(url)
        if data:
            result[sym] = {
                "mark_price": data.get("markPrice", "0"),
                "index_price": data.get("indexPrice", "0"),
                "funding_rate": data.get("lastFundingRate", "0"),
                "next_funding_time": data.get("nextFundingTime", 0),
            }
        time.sleep(0.1)
    return result


def fetch_open_interest_hist():
    """Fetch open interest history (last 5m candles)."""
    result = {}
    for sym in SYMBOLS:
        url = f"{FAPI_BASE}/futures/data/openInterestHist?symbol={sym}&period=5m&limit=1"
        data = fetch(url)
        if data and len(data) > 0:
            d = data[0]
            result[sym] = {
                "sum_open_interest": d.get("sumOpenInterest", "0"),
                "sum_open_interest_value": d.get("sumOpenInterestValue", "0"),
                "timestamp": d.get("timestamp", 0),
            }
        time.sleep(0.1)
    return result


def fetch_open_interest_spot():
    """Fetch current open interest."""
    result = {}
    for sym in SYMBOLS:
        url = f"{FAPI_BASE}/fapi/v1/openInterest?symbol={sym}"
        data = fetch(url)
        if data:
            result[sym] = {
                "open_interest": data["openInterest"],
                "time": data["time"],
            }
        time.sleep(0.1)
    return result


def fetch_long_short():
    result = {}
    for sym in SYMBOLS:
        url = f"{FAPI_BASE}/futures/data/globalLongShortAccountRatio?symbol={sym}&period=5m&limit=1"
        data = fetch(url)
        if data and len(data) > 0:
            d = data[0]
            result[sym] = {
                "long_short_ratio": d.get("longShortRatio", "0"),
                "long_account": d.get("longAccount", "0"),
                "short_account": d.get("shortAccount", "0"),
                "timestamp": d.get("timestamp", 0),
            }
        time.sleep(0.1)
    return result


def fetch_top_trader_ratio():
    result = {}
    for sym in SYMBOLS:
        url = f"{FAPI_BASE}/futures/data/topLongShortAccountRatio?symbol={sym}&period=5m&limit=1"
        data = fetch(url)
        if data and len(data) > 0:
            d = data[0]
            result[sym] = {
                "long_short_ratio": d.get("longShortRatio", "0"),
                "long_account": d.get("longAccount", "0"),
                "short_account": d.get("shortAccount", "0"),
                "timestamp": d.get("timestamp", 0),
            }
        time.sleep(0.1)
    return result


def fetch_taker_ratio():
    result = {}
    for sym in SYMBOLS:
        url = f"{FAPI_BASE}/futures/data/takerlongshortRatio?symbol={sym}&period=5m&limit=1"
        data = fetch(url)
        if data and len(data) > 0:
            d = data[0]
            result[sym] = {
                "buy_sell_ratio": d.get("buySellRatio", "0"),
                "buy_vol": d.get("buyVol", "0"),
                "sell_vol": d.get("sellVol", "0"),
                "timestamp": d.get("timestamp", 0),
            }
        time.sleep(0.1)
    return result


def main():
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    unix_ts = int(now.timestamp())
    print(f"[{ts}] Fetching derivatives data...")

    os.makedirs("data", exist_ok=True)

    premium = fetch_premium_index()
    print(f"  Premium index: {len(premium)} symbols")

    oi = fetch_open_interest_spot()
    print(f"  Open interest: {len(oi)} symbols")

    oi_hist = fetch_open_interest_hist()
    print(f"  OI hist: {len(oi_hist)} symbols")

    ls = fetch_long_short()
    print(f"  L/S ratio: {len(ls)} symbols")

    top_trader = fetch_top_trader_ratio()
    print(f"  Top trader ratio: {len(top_trader)} symbols")

    taker = fetch_taker_ratio()
    print(f"  Taker ratio: {len(taker)} symbols")

    funding_hist = fetch_funding_history()
    print(f"  Funding history: {len(funding_hist)} symbols")

    # Append to time-series files
    append_ts("data/funding_rates.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "rates": {sym: premium[sym]["funding_rate"] for sym in premium},
        "next_funding": {sym: premium[sym]["next_funding_time"] for sym in premium},
    })

    append_ts("data/open_interest.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "oi": {sym: oi[sym]["open_interest"] for sym in oi},
        "oi_value": {sym: oi_hist[sym]["sum_open_interest_value"] for sym in oi_hist},
    })

    append_ts("data/long_short_ratio.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "global": {sym: ls[sym]["long_short_ratio"] for sym in ls},
        "top_trader": {sym: top_trader[sym]["long_short_ratio"] for sym in top_trader},
        "taker_ratio": {sym: taker[sym]["buy_sell_ratio"] for sym in taker},
    })

    # Save full funding history per symbol
    for sym, history in funding_hist.items():
        path = f"data/funding_history_{sym}.json"
        with open(path, "w") as f:
            json.dump({"symbol": sym, "updated_at": ts, "history": history}, f, separators=(",", ":"))

    # Patch derivatives into latest.json
    latest_path = "data/latest.json"
    if os.path.exists(latest_path):
        try:
            with open(latest_path) as f:
                latest = json.load(f)

            for sym in SYMBOLS:
                if sym not in latest.get("derivatives", {}):
                    latest.setdefault("derivatives", {})[sym] = {}
                d = latest["derivatives"].setdefault(sym, {})
                if sym in premium:
                    d.update(premium[sym])
                if sym in oi:
                    d["open_interest"] = oi[sym]["open_interest"]
                if sym in oi_hist:
                    d["open_interest_usd"] = oi_hist[sym]["sum_open_interest_value"]
                if sym in ls:
                    d["long_short_ratio"] = ls[sym]["long_short_ratio"]
                    d["long_account_pct"] = ls[sym]["long_account"]
                    d["short_account_pct"] = ls[sym]["short_account"]
                if sym in taker:
                    d["taker_buy_sell_ratio"] = taker[sym]["buy_sell_ratio"]

            latest["derivatives_updated_at"] = ts
            with open(latest_path, "w") as f:
                json.dump(latest, f, separators=(",", ":"))
            print("  Patched derivatives into data/latest.json")
        except Exception as e:
            print(f"  Warning: could not patch latest.json: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
