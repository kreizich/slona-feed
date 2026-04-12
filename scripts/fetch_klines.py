#!/usr/bin/env python3
"""
Fetch multi-timeframe OHLCV klines for tracked symbols.
Saves to data/klines/<symbol>/<interval>.json
Also patches klines section into data/latest.json
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

# interval -> (limit, source)
INTERVALS = {
    "1m":  (60,  "spot"),
    "5m":  (60,  "spot"),
    "15m": (96,  "spot"),
    "1h":  (48,  "spot"),
    "4h":  (30,  "spot"),
    "1d":  (30,  "spot"),
}


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


def parse_klines(raw):
    """Convert raw kline arrays to dicts."""
    result = []
    for k in raw:
        result.append({
            "t": k[0],   # open time ms
            "o": k[1],   # open
            "h": k[2],   # high
            "l": k[3],   # low
            "c": k[4],   # close
            "v": k[5],   # volume
            "T": k[6],   # close time ms
            "qv": k[7],  # quote volume
            "n": k[8],   # trades
            "tbv": k[9], # taker buy base vol
            "tqv": k[10],# taker buy quote vol
        })
    return result


def fetch_spot_klines(symbol, interval, limit):
    url = f"{SPOT_BASE}/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    data = fetch(url)
    if data:
        return parse_klines(data)
    return []


def fetch_index_klines(symbol, interval, limit):
    url = f"{FAPI_BASE}/fapi/v1/indexPriceKlines?pair={symbol}&interval={interval}&limit={limit}"
    data = fetch(url)
    if data:
        return [
            {"t": k[0], "o": k[1], "h": k[2], "l": k[3], "c": k[4], "T": k[6], "n": k[8]}
            for k in data
        ]
    return []


def main():
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] Fetching klines...")

    all_klines = {}

    for sym in SYMBOLS:
        all_klines[sym] = {}
        for interval, (limit, source) in INTERVALS.items():
            if source == "spot":
                klines = fetch_spot_klines(sym, interval, limit)
            else:
                klines = fetch_index_klines(sym, interval, limit)

            all_klines[sym][interval] = klines
            print(f"  {sym} {interval}: {len(klines)} candles")

            # Save per-symbol per-interval file
            os.makedirs(f"data/klines/{sym}", exist_ok=True)
            path = f"data/klines/{sym}/{interval}.json"
            with open(path, "w") as f:
                json.dump({
                    "symbol": sym,
                    "interval": interval,
                    "updated_at": ts,
                    "klines": klines,
                }, f, separators=(",", ":"))

            time.sleep(0.12)  # ~8 req/sec, well within limits

    # Patch klines into latest.json
    latest_path = "data/latest.json"
    if os.path.exists(latest_path):
        try:
            with open(latest_path) as f:
                latest = json.load(f)
            latest["klines"] = {
                sym: {
                    iv: all_klines[sym][iv]
                    for iv in ["15m", "1h", "4h", "1d"]
                }
                for sym in SYMBOLS
            }
            latest["klines_updated_at"] = ts
            with open(latest_path, "w") as f:
                json.dump(latest, f, separators=(",", ":"))
            print("  Patched klines into data/latest.json")
        except Exception as e:
            print(f"  Warning: could not patch latest.json: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
