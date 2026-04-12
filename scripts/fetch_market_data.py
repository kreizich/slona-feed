#!/usr/bin/env python3
"""
Fetch spot + futures 24hr stats, order book, avg price, recent trades.
Saves to data/latest.json and data/history/YYYY-MM-DD_HH-MM.json
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]
SPOT_BASE = "https://api.binance.com"
FAPI_BASE = "https://fapi.binance.com"


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
            "price": t["lastPrice"],
            "open": t["openPrice"],
            "high": t["highPrice"],
            "low": t["lowPrice"],
            "close": t["lastPrice"],
            "volume": t["volume"],
            "quote_volume": t["quoteVolume"],
            "trades": t["count"],
            "price_change": t["priceChange"],
            "price_change_pct": t["priceChangePercent"],
            "weighted_avg_price": t["weightedAvgPrice"],
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
                    "id": t["id"],
                    "price": t["price"],
                    "qty": t["qty"],
                    "time": t["time"],
                    "is_buyer_maker": t["isBuyerMaker"],
                }
                for t in data[-10:]
            ]
        time.sleep(0.1)
    return result


def fetch_futures_tickers():
    syms_param = "[" + ",".join(f'"{s}"' for s in SYMBOLS) + "]"
    url = f"{FAPI_BASE}/fapi/v1/ticker/24hr?symbols={urllib.request.quote(syms_param)}"
    data = fetch(url)
    if not data:
        return {}
    result = {}
    for t in data:
        sym = t["symbol"]
        if sym in SYMBOLS:
            result[sym] = {
                "price": t["lastPrice"],
                "open": t["openPrice"],
                "high": t["highPrice"],
                "low": t["lowPrice"],
                "volume": t["volume"],
                "quote_volume": t["quoteVolume"],
                "price_change_pct": t["priceChangePercent"],
            }
    return result


def fetch_futures_funding_rates():
    result = {}
    for sym in SYMBOLS:
        url = f"{FAPI_BASE}/fapi/v1/premiumIndex?symbol={sym}"
        data = fetch(url)
        if data:
            result[sym] = {
                "funding_rate": data.get("lastFundingRate", "0"),
                "next_funding_time": data.get("nextFundingTime", 0),
                "mark_price": data.get("markPrice", "0"),
                "index_price": data.get("indexPrice", "0"),
            }
        time.sleep(0.1)
    return result


def fetch_open_interest():
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


def fetch_long_short_ratio():
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

    print(f"[{ts}] Fetching market data...")

    spot = fetch_spot_tickers()
    print(f"  Spot tickers: {len(spot)} symbols")

    avg_prices = fetch_avg_prices()
    print(f"  Avg prices: {len(avg_prices)} symbols")

    orderbook = fetch_order_books()
    print(f"  Order books: {len(orderbook)} symbols")

    trades = fetch_recent_trades()
    print(f"  Recent trades: {len(trades)} symbols")

    futures_tickers = fetch_futures_tickers()
    print(f"  Futures tickers: {len(futures_tickers)} symbols")

    funding = fetch_futures_funding_rates()
    print(f"  Funding rates: {len(funding)} symbols")

    oi = fetch_open_interest()
    print(f"  Open interest: {len(oi)} symbols")

    ls_ratio = fetch_long_short_ratio()
    print(f"  L/S ratios: {len(ls_ratio)} symbols")

    taker = fetch_taker_ratio()
    print(f"  Taker ratios: {len(taker)} symbols")

    # Merge spot with avg prices
    for sym in spot:
        if sym in avg_prices:
            spot[sym].update(avg_prices[sym])

    # Build derivatives section
    derivatives = {}
    for sym in SYMBOLS:
        derivatives[sym] = {}
        if sym in funding:
            derivatives[sym].update(funding[sym])
        if sym in oi:
            derivatives[sym].update(oi[sym])
        if sym in ls_ratio:
            derivatives[sym]["long_short_ratio"] = ls_ratio[sym]["long_short_ratio"]
            derivatives[sym]["long_account_pct"] = ls_ratio[sym]["long_account"]
            derivatives[sym]["short_account_pct"] = ls_ratio[sym]["short_account"]
        if sym in taker:
            derivatives[sym]["taker_buy_sell_ratio"] = taker[sym]["buy_sell_ratio"]
            derivatives[sym]["taker_buy_vol"] = taker[sym]["buy_vol"]
            derivatives[sym]["taker_sell_vol"] = taker[sym]["sell_vol"]
        if sym in futures_tickers:
            derivatives[sym]["futures_price"] = futures_tickers[sym]["price"]
            derivatives[sym]["futures_volume"] = futures_tickers[sym]["volume"]

    snapshot = {
        "timestamp": ts,
        "updated_at_unix": unix_ts,
        "spot": spot,
        "derivatives": derivatives,
        "orderbook": orderbook,
        "recent_trades": trades,
    }

    os.makedirs("data/history", exist_ok=True)

    # Write latest
    with open("data/latest.json", "w") as f:
        json.dump(snapshot, f, separators=(",", ":"))
    print("  Saved data/latest.json")

    # Write history snapshot
    fname = now.strftime("%Y-%m-%d_%H-%M")
    hist_path = f"data/history/{fname}.json"
    with open(hist_path, "w") as f:
        json.dump(snapshot, f, separators=(",", ":"))
    print(f"  Saved {hist_path}")

    # Append to funding_rates.json
    append_time_series("data/funding_rates.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "rates": {sym: funding[sym]["funding_rate"] for sym in funding},
    })

    # Append to open_interest.json
    append_time_series("data/open_interest.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "oi": {sym: oi[sym]["open_interest"] for sym in oi},
    })

    # Append to long_short_ratio.json
    append_time_series("data/long_short_ratio.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "ratios": {sym: ls_ratio[sym]["long_short_ratio"] for sym in ls_ratio},
    })

    print("Done.")


def append_time_series(path, entry, max_entries=2016):
    """Append entry to a JSON array file, keeping last max_entries."""
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
