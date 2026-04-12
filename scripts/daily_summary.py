#!/usr/bin/env python3
"""
Generate daily OHLCV summary and clean up old history files (>90 days).
Runs at 00:05 UTC daily.
"""
import json
import os
import glob
import time
from datetime import datetime, timezone, timedelta

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]
HISTORY_DIR = "data/history"
DAILY_DIR = "data/daily"
KLINES_DIR = "data/klines"
RETENTION_DAYS = 90


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def cleanup_old_history():
    """Remove history snapshots older than RETENTION_DAYS."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    removed = 0
    pattern = os.path.join(HISTORY_DIR, "*.json")
    for path in glob.glob(pattern):
        fname = os.path.basename(path)
        # Format: YYYY-MM-DD_HH-MM.json
        try:
            date_str = fname[:16]  # YYYY-MM-DD_HH-MM
            dt = datetime.strptime(date_str, "%Y-%m-%d_%H-%M").replace(tzinfo=timezone.utc)
            if dt < cutoff:
                os.remove(path)
                removed += 1
        except ValueError:
            pass
    print(f"  Cleaned up {removed} old history files (>{RETENTION_DAYS} days)")


def cleanup_old_daily():
    """Remove daily summaries older than RETENTION_DAYS."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    removed = 0
    pattern = os.path.join(DAILY_DIR, "*.json")
    for path in glob.glob(pattern):
        fname = os.path.basename(path)
        try:
            date_str = fname[:10]  # YYYY-MM-DD
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if dt < cutoff:
                os.remove(path)
                removed += 1
        except ValueError:
            pass
    print(f"  Cleaned up {removed} old daily summary files")


def generate_daily_summary():
    """Build daily OHLCV aggregates from 1d kline data."""
    now = datetime.now(timezone.utc)
    # Use yesterday's date
    yesterday = now - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    summary = {
        "date": date_str,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbols": {},
    }

    for sym in SYMBOLS:
        klines_path = os.path.join(KLINES_DIR, sym, "1d.json")
        data = load_json(klines_path)
        if not data or "klines" not in data:
            print(f"  No 1d klines for {sym}, skipping")
            continue

        klines = data["klines"]
        if not klines:
            continue

        # Find yesterday's candle
        yesterday_ms_start = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        yesterday_ms_end = yesterday_ms_start + 86400000

        candle = None
        for k in klines:
            if k["t"] >= yesterday_ms_start and k["t"] < yesterday_ms_end:
                candle = k
                break

        if not candle:
            # Use last complete candle
            if len(klines) >= 2:
                candle = klines[-2]
            else:
                candle = klines[-1]

        open_p = float(candle["o"])
        close_p = float(candle["c"])
        change_pct = ((close_p - open_p) / open_p * 100) if open_p > 0 else 0

        summary["symbols"][sym] = {
            "open": candle["o"],
            "high": candle["h"],
            "low": candle["l"],
            "close": candle["c"],
            "volume": candle["v"],
            "quote_volume": candle["qv"],
            "trades": candle["n"],
            "taker_buy_volume": candle["tbv"],
            "change_pct": round(change_pct, 4),
            "candle_time": candle["t"],
        }
        print(f"  {sym}: O={candle['o']} H={candle['h']} L={candle['l']} C={candle['c']} ({change_pct:.2f}%)")

    os.makedirs(DAILY_DIR, exist_ok=True)
    daily_path = os.path.join(DAILY_DIR, f"{date_str}.json")
    save_json(daily_path, summary)
    print(f"  Saved {daily_path}")

    return summary


def update_daily_index():
    """Maintain an index of all daily summaries."""
    pattern = os.path.join(DAILY_DIR, "*.json")
    files = sorted(glob.glob(pattern))
    entries = []
    for path in files:
        fname = os.path.basename(path)
        if fname == "index.json":
            continue
        date_str = fname[:10]
        data = load_json(path)
        if data and "symbols" in data:
            entry = {"date": date_str}
            for sym in SYMBOLS:
                if sym in data["symbols"]:
                    entry[f"{sym}_close"] = data["symbols"][sym]["close"]
                    entry[f"{sym}_change_pct"] = data["symbols"][sym]["change_pct"]
            entries.append(entry)

    index_path = os.path.join(DAILY_DIR, "index.json")
    save_json(index_path, {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": len(entries),
        "entries": entries[-90:],  # keep last 90 days
    })
    print(f"  Updated daily index with {len(entries)} days")


def trim_time_series(path, max_entries=2016):
    """Trim a time-series JSON array to max_entries."""
    data = load_json(path)
    if data and isinstance(data, list) and len(data) > max_entries:
        trimmed = data[-max_entries:]
        save_json(path, trimmed)
        print(f"  Trimmed {path}: {len(data)} -> {len(trimmed)} entries")


def main():
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] Running daily summary and cleanup...")

    os.makedirs(HISTORY_DIR, exist_ok=True)
    os.makedirs(DAILY_DIR, exist_ok=True)

    print("Generating daily summary...")
    generate_daily_summary()

    print("Updating daily index...")
    update_daily_index()

    print("Trimming time-series files...")
    for tsfile in ["data/funding_rates.json", "data/open_interest.json", "data/long_short_ratio.json"]:
        if os.path.exists(tsfile):
            trim_time_series(tsfile, max_entries=2016)

    print("Cleaning up old history files...")
    cleanup_old_history()
    cleanup_old_daily()

    print("Done.")


if __name__ == "__main__":
    main()
