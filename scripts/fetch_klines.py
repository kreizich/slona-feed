#!/usr/bin/env python3
"""
Fetch multi-timeframe OHLCV klines.
Spot klines:       api.binance.us  (Binance US)
Derivatives klines: api.bybit.com  (Bybit V5)
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
SPOT_BASE  = "https://api.binance.us"
BYBIT_BASE = "https://api.bybit.com"

# interval -> (limit, binance_interval, bybit_interval)
INTERVALS = {
    "1m":  (60,  "1m",  "1"),
    "5m":  (60,  "5m",  "5"),
    "15m": (96,  "15m", "15"),
    "1h":  (48,  "1h",  "60"),
    "4h":  (30,  "4h",  "240"),
    "1d":  (30,  "1d",  "D"),
}


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


def parse_binance_klines(raw):
    return [
        {
            "t":   k[0],
            "o":   k[1],
            "h":   k[2],
            "l":   k[3],
            "c":   k[4],
            "v":   k[5],
            "T":   k[6],
            "qv":  k[7],
            "n":   k[8],
            "tbv": k[9],
            "tqv": k[10],
        }
        for k in raw
    ]


def parse_bybit_klines(raw):
    """
    Bybit V5 kline list: [startTime, open, high, low, close, volume, turnover]
    Returned newest-first, so reverse.
    """
    result = []
    for k in reversed(raw):
        result.append({
            "t":  int(k[0]),
            "o":  k[1],
            "h":  k[2],
            "l":  k[3],
            "c":  k[4],
            "v":  k[5],
            "qv": k[6],
        })
    return result


def fetch_spot_klines(symbol, interval, limit):
    url = f"{SPOT_BASE}/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    data = fetch(url)
    if data is not None:
        return parse_binance_klines(data)
    return []


def fetch_bybit_klines(symbol, bybit_interval, limit):
    url = f"{BYBIT_BASE}/v5/market/kline?category=linear&symbol={symbol}&interval={bybit_interval}&limit={limit}"
    data = fetch(url)
    if data and data.get("retCode") == 0:
        return parse_bybit_klines(data["result"]["list"])
    return []


def main():
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] Fetching klines...", flush=True)

    all_klines = {}
    total_candles = 0

    for sym in SYMBOLS:
        all_klines[sym] = {}
        for interval, (limit, binance_iv, bybit_iv) in INTERVALS.items():
            # Try Binance US first, fall back to Bybit
            klines = fetch_spot_klines(sym, binance_iv, limit)
            if not klines:
                print(f"  {sym} {interval}: Binance US failed, trying Bybit...", flush=True)
                klines = fetch_bybit_klines(sym, bybit_iv, limit)

            all_klines[sym][interval] = klines
            total_candles += len(klines)
            print(f"  {sym} {interval}: {len(klines)} candles", flush=True)

            os.makedirs(f"data/klines/{sym}", exist_ok=True)
            with open(f"data/klines/{sym}/{interval}.json", "w") as f:
                json.dump({
                    "symbol":     sym,
                    "interval":   interval,
                    "updated_at": ts,
                    "klines":     klines,
                }, f, separators=(",", ":"))

            time.sleep(0.12)

    if total_candles == 0:
        print("[FATAL] No kline data fetched from any source.", flush=True)
        sys.exit(1)

    # Patch klines into latest.json
    latest_path = "data/latest.json"
    if os.path.exists(latest_path):
        try:
            with open(latest_path) as f:
                latest = json.load(f)
            latest["klines"] = {
                sym: {iv: all_klines[sym][iv] for iv in ["15m", "1h", "4h", "1d"]}
                for sym in SYMBOLS
            }
            latest["klines_updated_at"] = ts
            with open(latest_path, "w") as f:
                json.dump(latest, f, separators=(",", ":"))
            print("  Patched klines into data/latest.json", flush=True)
        except Exception as e:
            print(f"  Warning: could not patch latest.json: {e}", flush=True)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()
