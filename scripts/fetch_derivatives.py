#!/usr/bin/env python3
"""
Fetch futures/derivatives data.
Primary:  OKX public API  (api.okx.com) — no geo-block, no API key needed
Fallback: Gate.io futures API (api.gateio.ws)
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

# OKX uses "BTC-USDT-SWAP" format; ccy for statistics endpoints
OKX_INST = {
    "BTCUSDT": "BTC-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "SOLUSDT": "SOL-USDT-SWAP",
    "XRPUSDT": "XRP-USDT-SWAP",
    "BNBUSDT": "BNB-USDT-SWAP",
}
OKX_CCY = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "SOLUSDT": "SOL",
    "XRPUSDT": "XRP",
    "BNBUSDT": "BNB",
}

# Gate.io uses "BTC_USDT" format
GATE_SYM = {
    "BTCUSDT": "BTC_USDT",
    "ETHUSDT": "ETH_USDT",
    "SOLUSDT": "SOL_USDT",
    "XRPUSDT": "XRP_USDT",
    "BNBUSDT": "BNB_USDT",
}

OKX_BASE   = "https://www.okx.com"
GATE_BASE  = "https://api.gateio.ws/api/v4"
FAPI_BASE  = "https://fapi.binance.com"
MAX_ENTRIES = 2016


def fetch(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
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


# ── OKX ──────────────────────────────────────────────────────────────────────

def okx_tickers():
    """GET /api/v5/market/tickers?instType=SWAP — all perpetual swap tickers."""
    url = f"{OKX_BASE}/api/v5/market/tickers?instType=SWAP"
    data = fetch(url)
    if not data or data.get("code") != "0":
        print(f"[WARN] OKX tickers failed: {data}", flush=True)
        return {}
    result = {}
    for t in data["data"]:
        inst = t["instId"]
        sym  = next((s for s, i in OKX_INST.items() if i == inst), None)
        if not sym:
            continue
        # oiCcy = open interest in base currency (BTC, ETH, etc.) — NOT USD
        # Multiply by last price to get USD value
        oi_ccy     = t.get("oiCcy", "0")
        last_price = t.get("last", "0")
        try:
            oi_usd = str(round(float(oi_ccy) * float(last_price), 2))
        except (ValueError, TypeError):
            oi_usd = "0"
        result[sym] = {
            "futures_price":     t.get("last", "0"),
            "open":              t.get("open24h", "0"),
            "high":              t.get("high24h", "0"),
            "low":               t.get("low24h", "0"),
            "volume":            t.get("vol24h", "0"),
            "open_interest":     t.get("oi", "0"),
            "open_interest_usd": oi_usd,
        }
    return result


# ── Binance fapi OI (try first, may be geo-blocked) ───────────────────────────

def binance_fapi_oi():
    """
    GET /fapi/v1/openInterest — Binance futures open interest.
    May return HTTP 451 from GitHub Actions (geo-block); silently skip if so.
    Returns dict keyed by symbol with open_interest and open_interest_usd.
    """
    # First fetch all tickers for prices (needed to compute USD value)
    url_ticker = f"{FAPI_BASE}/fapi/v1/ticker/price"
    prices_data = fetch(url_ticker)
    prices = {}
    if prices_data and isinstance(prices_data, list):
        for p in prices_data:
            prices[p["symbol"]] = p.get("price", "0")

    result = {}
    for sym in SYMBOLS:
        url = f"{FAPI_BASE}/fapi/v1/openInterest?symbol={sym}"
        data = fetch(url)
        if not data:
            return {}   # likely geo-blocked; bail out entirely
        oi_contracts = data.get("openInterest", "0")
        price = prices.get(sym, "0")
        try:
            oi_usd = str(round(float(oi_contracts) * float(price), 2))
        except (ValueError, TypeError):
            oi_usd = "0"
        result[sym] = {
            "open_interest":     oi_contracts,
            "open_interest_usd": oi_usd,
        }
        time.sleep(0.1)
    return result


def okx_funding_rate(sym):
    """GET /api/v5/public/funding-rate — current funding rate."""
    inst = OKX_INST[sym]
    url  = f"{OKX_BASE}/api/v5/public/funding-rate?instId={inst}"
    data = fetch(url)
    if data and data.get("code") == "0" and data["data"]:
        d = data["data"][0]
        return {
            "funding_rate":      d.get("fundingRate", "0"),
            "next_funding_time": int(d.get("fundingTime", 0)),
        }
    return {}


def okx_mark_index(sym):
    """GET /api/v5/public/mark-price — mark price for SWAP."""
    inst = OKX_INST[sym]
    url  = f"{OKX_BASE}/api/v5/public/mark-price?instType=SWAP&instId={inst}"
    data = fetch(url)
    if data and data.get("code") == "0" and data.get("data"):
        d = data["data"][0]
        return {"mark_price": d.get("markPx", "0")}
    return {}


def okx_long_short(sym):
    """
    GET /api/v5/rubik/stat/contracts/long-short-account-ratio
    OKX response format: [["timestamp", "ratio"], ...]  (just 2 elements per row)
    The ratio is longAccounts / shortAccounts already computed.
    """
    ccy = OKX_CCY[sym]
    url = f"{OKX_BASE}/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy={ccy}&period=1H"
    data = fetch(url)
    if data and data.get("code") == "0" and data.get("data"):
        rows = data["data"]
        if not rows:
            return {}
        row = rows[-1]   # newest
        if len(row) < 2:
            return {}
        try:
            ratio = float(row[1])
            # Derive approximate long/short % from ratio: long = ratio/(1+ratio)
            long_pct  = ratio / (1.0 + ratio)
            short_pct = 1.0 - long_pct
            return {
                "long_short_ratio":  str(round(ratio, 4)),
                "long_account_pct":  str(round(long_pct, 4)),
                "short_account_pct": str(round(short_pct, 4)),
            }
        except (ValueError, ZeroDivisionError):
            return {}
    return {}


def okx_funding_history(sym):
    """GET /api/v5/public/funding-rate-history."""
    inst = OKX_INST[sym]
    url  = f"{OKX_BASE}/api/v5/public/funding-rate-history?instId={inst}&limit=10"
    data = fetch(url)
    if data and data.get("code") == "0":
        return [
            {"rate": d["fundingRate"], "time": int(d["fundingTime"])}
            for d in data["data"]
        ]
    return []


# ── Gate.io fallback ──────────────────────────────────────────────────────────

def gate_ticker(sym):
    """GET /futures/usdt/tickers?contract=BTC_USDT."""
    gsym = GATE_SYM[sym]
    url  = f"{GATE_BASE}/futures/usdt/tickers?contract={gsym}"
    data = fetch(url)
    if data and isinstance(data, list) and data:
        t = data[0]
        return {
            "futures_price": t.get("last", "0"),
            "volume":        t.get("volume_24h_quote", "0"),
            "open_interest": t.get("open_interest", "0"),
            "funding_rate":  t.get("funding_rate", "0"),
            "mark_price":    t.get("mark_price", "0"),
            "index_price":   t.get("index_price", "0"),
        }
    return {}


def gate_funding_history(sym):
    gsym = GATE_SYM[sym]
    url  = f"{GATE_BASE}/futures/usdt/funding_rate?contract={gsym}&limit=10"
    data = fetch(url)
    if data and isinstance(data, list):
        return [
            {"rate": str(d.get("r", "0")), "time": int(d.get("t", 0)) * 1000}
            for d in data
        ]
    return []


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    now      = datetime.now(timezone.utc)
    ts       = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    unix_ts  = int(now.timestamp())
    print(f"[{ts}] Fetching derivatives data...", flush=True)

    os.makedirs("data", exist_ok=True)

    # ── Try Binance fapi for OI first (best accuracy; may be geo-blocked) ──
    print("  Trying Binance fapi for open interest...", flush=True)
    fapi_oi = binance_fapi_oi()
    if fapi_oi:
        print(f"  Binance fapi OI: {len(fapi_oi)} symbols", flush=True)
    else:
        print("  Binance fapi unavailable — will use OKX OI (oiCcy * price)", flush=True)

    # ── OKX bulk ticker (1 request for all symbols) ──
    okx_tick = okx_tickers()
    print(f"  OKX tickers: {len(okx_tick)} symbols", flush=True)

    # Overlay Binance fapi OI if available (more accurate than OKX estimate)
    if fapi_oi:
        for sym, oi_data in fapi_oi.items():
            if sym in okx_tick:
                okx_tick[sym].update(oi_data)

    use_gate = len(okx_tick) == 0   # fall back to Gate.io if OKX bulk failed

    derivatives = {}
    funding_hist_all = {}

    for sym in SYMBOLS:
        d = {}

        if not use_gate and sym in okx_tick:
            d.update(okx_tick[sym])

            # Funding rate
            fr = okx_funding_rate(sym)
            d.update(fr)
            time.sleep(0.1)

            # Mark price
            mp = okx_mark_index(sym)
            d.update(mp)
            time.sleep(0.1)

            # Long/short ratio
            ls = okx_long_short(sym)
            d.update(ls)
            time.sleep(0.1)

            # Funding history
            fh = okx_funding_history(sym)
            if fh:
                funding_hist_all[sym] = fh
            time.sleep(0.1)

        else:
            # Gate.io fallback
            print(f"  [{sym}] Using Gate.io fallback", flush=True)
            gt = gate_ticker(sym)
            d.update(gt)
            time.sleep(0.1)

            fh = gate_funding_history(sym)
            if fh:
                funding_hist_all[sym] = fh
            time.sleep(0.1)

        derivatives[sym] = d
        rate = d.get("funding_rate", "?")
        oi   = d.get("open_interest", "?")
        print(f"  {sym}: funding={rate}  OI={oi}", flush=True)

    if not any(derivatives.values()):
        print("[FATAL] No derivatives data from OKX or Gate.io.", flush=True)
        sys.exit(1)

    # ── Time-series append ──
    append_ts("data/funding_rates.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "rates": {s: derivatives[s].get("funding_rate", "0") for s in SYMBOLS},
        "next_funding": {s: derivatives[s].get("next_funding_time", 0) for s in SYMBOLS},
    })

    append_ts("data/open_interest.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "oi":       {s: derivatives[s].get("open_interest", "0")     for s in SYMBOLS},
        "oi_value": {s: derivatives[s].get("open_interest_usd", "0") for s in SYMBOLS},
    })

    append_ts("data/long_short_ratio.json", {
        "timestamp": ts,
        "unix": unix_ts,
        "global": {s: derivatives[s].get("long_short_ratio", "1") for s in SYMBOLS},
    })

    for sym, history in funding_hist_all.items():
        with open(f"data/funding_history_{sym}.json", "w") as f:
            json.dump({"symbol": sym, "updated_at": ts, "history": history}, f, separators=(",", ":"))

    # ── Patch into latest.json ──
    latest_path = "data/latest.json"
    if os.path.exists(latest_path):
        try:
            with open(latest_path) as f:
                latest = json.load(f)
            for sym in SYMBOLS:
                latest.setdefault("derivatives", {}).setdefault(sym, {}).update(derivatives[sym])
            latest["derivatives_updated_at"] = ts
            with open(latest_path, "w") as f:
                json.dump(latest, f, separators=(",", ":"))
            print("  Patched derivatives into data/latest.json", flush=True)
        except Exception as e:
            print(f"  Warning: could not patch latest.json: {e}", flush=True)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()
