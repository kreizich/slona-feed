#!/usr/bin/env python3
"""
Fetch futures/derivatives data from Bybit V5 API.
(Binance futures uses HTTP 451 from GitHub Actions — US geo-restriction)
"""
import json
import os
import sys
import time
import traceback
import urllib.request
import urllib.error
from datetime import datetime, timezone

SYMBOLS    = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]
BYBIT_BASE = "https://api.bybit.com"
MAX_ENTRIES = 2016  # ~7 days at 5-min intervals


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


def fetch_bybit_tickers():
    """Tickers include funding rate, OI, mark/index price."""
    url = f"{BYBIT_BASE}/v5/market/tickers?category=linear"
    data = fetch(url)
    if not data or data.get("retCode") != 0:
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
            }
    return result


def fetch_bybit_oi_history():
    """Open interest history (5-min snapshots)."""
    result = {}
    for sym in SYMBOLS:
        url = f"{BYBIT_BASE}/v5/market/open-interest?category=linear&symbol={sym}&intervalTime=5min&limit=1"
        data = fetch(url)
        if data and data.get("retCode") == 0:
            rows = data["result"]["list"]
            if rows:
                result[sym] = {
                    "open_interest": rows[0].get("openInterest", "0"),
                    "timestamp":     int(rows[0].get("timestamp", 0)),
                }
        time.sleep(0.1)
    return result


def fetch_bybit_long_short():
    """Long/short account ratio."""
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


def fetch_bybit_funding_history():
    """Last 10 funding rate entries per symbol."""
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


def main():
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    unix_ts = int(now.timestamp())
    print(f"[{ts}] Fetching derivatives data (Bybit)...", flush=True)

    os.makedirs("data", exist_ok=True)

    tickers = fetch_bybit_tickers()
    print(f"  Tickers: {len(tickers)} symbols", flush=True)

    if not tickers:
        print("[FATAL] No Bybit ticker data — api.bybit.com unreachable.", flush=True)
        sys.exit(1)

    oi_hist = fetch_bybit_oi_history()
    print(f"  OI history: {len(oi_hist)} symbols", flush=True)

    ls = fetch_bybit_long_short()
    print(f"  L/S ratio: {len(ls)} symbols", flush=True)

    funding_hist = fetch_bybit_funding_history()
    print(f"  Funding history: {len(funding_hist)} symbols", flush=True)

    # Append time-series
    append_ts("data/funding_rates.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "rates": {sym: tickers[sym]["funding_rate"] for sym in tickers},
        "next_funding": {sym: tickers[sym]["next_funding_time"] for sym in tickers},
    })

    append_ts("data/open_interest.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "oi":       {sym: tickers[sym]["open_interest"]     for sym in tickers},
        "oi_value": {sym: tickers[sym]["open_interest_usd"] for sym in tickers},
    })

    append_ts("data/long_short_ratio.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "global": {sym: ls[sym]["long_short_ratio"] for sym in ls},
    })

    # Per-symbol funding history files
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
                d = latest.setdefault("derivatives", {}).setdefault(sym, {})
                if sym in tickers:
                    d.update(tickers[sym])
                if sym in ls:
                    d.update(ls[sym])
            latest["derivatives_updated_at"] = ts
            with open(latest_path, "w") as f:
                json.dump(latest, f, separators=(",", ":"))
            print("  Patched derivatives into data/latest.json", flush=True)
        except Exception as e:
            print(f"  Warning: could not patch latest.json: {e}", flush=True)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()
