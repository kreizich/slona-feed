# Slona Feed — Crypto Market Data Pipeline

A production-ready GitHub-hosted pipeline that automatically fetches, stores, and exposes Binance market data every 5 minutes via GitHub Actions.

## Live Dashboard

**Dashboard:** https://kreizich.github.io/slona-feed/  
**Latest JSON:** https://kreizich.github.io/slona-feed/data/latest.json

---

## Tracked Symbols

`BTCUSDT` · `ETHUSDT` · `SOLUSDT` · `XRPUSDT` · `BNBUSDT`

---

## Data Sources

All data is fetched from Binance public APIs. **No API key required.**

### Spot API (`api.binance.com`)

| Endpoint | Data | Update Freq |
|---|---|---|
| `GET /api/v3/ticker/24hr` | Price, open, high, low, volume, trades | 5 min |
| `GET /api/v3/klines` | OHLCV candles (1m, 5m, 15m, 1h, 4h, 1d) | 15 min |
| `GET /api/v3/depth` | Order book top 20 bids/asks | 5 min |
| `GET /api/v3/trades` | Recent 20 trades per symbol | 5 min |
| `GET /api/v3/avgPrice` | 5-min weighted average price | 5 min |

### Futures API (`fapi.binance.com`)

| Endpoint | Data | Update Freq |
|---|---|---|
| `GET /fapi/v1/ticker/24hr` | Futures 24h stats | 5 min |
| `GET /fapi/v1/premiumIndex` | Funding rate, mark price, index price | 5 min |
| `GET /fapi/v1/openInterest` | Current open interest | 5 min |
| `GET /futures/data/openInterestHist` | OI with USD value | 5 min |
| `GET /futures/data/globalLongShortAccountRatio` | Long/short account ratio | 5 min |
| `GET /futures/data/topLongShortAccountRatio` | Top trader long/short ratio | 5 min |
| `GET /futures/data/takerlongshortRatio` | Taker buy/sell volume ratio | 5 min |
| `GET /fapi/v1/fundingRate` | Funding rate history | 5 min |
| `GET /fapi/v1/indexPriceKlines` | Index price OHLCV | 15 min |

---

## Data Storage

```
data/
├── latest.json                    # Always current snapshot (overwrites each run)
├── funding_rates.json             # Appended funding rate time-series (last ~7 days)
├── open_interest.json             # Appended open interest time-series (last ~7 days)
├── long_short_ratio.json          # Appended L/S ratio time-series (last ~7 days)
├── funding_history_BTCUSDT.json   # Per-symbol funding history
├── history/
│   └── YYYY-MM-DD_HH-MM.json     # Full snapshots (kept 90 days)
├── daily/
│   ├── YYYY-MM-DD.json           # Daily OHLCV summary
│   └── index.json                # Index of all daily summaries
└── klines/
    └── <SYMBOL>/
        ├── 1m.json
        ├── 5m.json
        ├── 15m.json
        ├── 1h.json
        ├── 4h.json
        └── 1d.json
```

---

## `data/latest.json` Structure

```json
{
  "timestamp": "2024-01-15T12:00:00Z",
  "updated_at_unix": 1705320000,
  "spot": {
    "BTCUSDT": {
      "price": "42000.00",
      "open": "41500.00",
      "high": "42500.00",
      "low": "41000.00",
      "close": "42000.00",
      "volume": "12345.678",
      "quote_volume": "519000000",
      "trades": 450000,
      "price_change": "500.00",
      "price_change_pct": "1.205",
      "weighted_avg_price": "41850.00",
      "avg_price": "41900.00",
      "avg_price_mins": 5
    }
  },
  "derivatives": {
    "BTCUSDT": {
      "funding_rate": "0.00010000",
      "next_funding_time": 1705334400000,
      "mark_price": "42005.00",
      "index_price": "42000.00",
      "open_interest": "80000.000",
      "open_interest_usd": "3360000000",
      "long_short_ratio": "1.2500",
      "long_account_pct": "0.5556",
      "short_account_pct": "0.4444",
      "taker_buy_sell_ratio": "1.0523",
      "futures_price": "42010.00",
      "futures_volume": "95000.000"
    }
  },
  "orderbook": {
    "BTCUSDT": {
      "last_update_id": 123456789,
      "bids": [["42000.00", "0.500"], ["41999.00", "1.200"]],
      "asks": [["42001.00", "0.300"], ["42002.00", "0.800"]]
    }
  },
  "klines": {
    "BTCUSDT": {
      "15m": [ { "t": 1705320000000, "o": "41900", "h": "42100", "l": "41850", "c": "42000", "v": "500" } ],
      "1h":  [ ... ],
      "4h":  [ ... ],
      "1d":  [ ... ]
    }
  }
}
```

---

## GitHub Actions Workflows

| Workflow | Schedule | Purpose |
|---|---|---|
| `fetch_market_data.yml` | Every 5 min | Spot prices, order book, trades, funding → `latest.json` + history |
| `fetch_klines.yml` | Every 15 min | Multi-timeframe OHLCV for all symbols |
| `fetch_derivatives.yml` | Every 5 min | Funding rates, OI, L/S ratio, taker ratio |
| `daily_summary.yml` | 00:05 UTC daily | Daily OHLCV aggregates + cleanup of files >90 days |

All workflows commit directly to the repository using `github-actions[bot]`.

---

## GitHub Pages Setup

1. Go to **Settings → Pages**
2. Source: **Deploy from branch**
3. Branch: `main` (or `master`), folder: `/ (root)`
4. Save — the site will be live at `https://kreizich.github.io/slona-feed/`

The dashboard fetches `data/latest.json` every 30 seconds automatically.

---

## Rate Limits

Binance enforces IP-based rate limits:

| API | Limit | Notes |
|---|---|---|
| Spot API | 6,000 weight/min | Ticker (1 weight), klines (2 weight), depth (variable), trades (25 weight) |
| Futures API | 2,400 weight/min | Most endpoints 1-5 weight |
| Funding rate history | 500 req/5min/IP | Shared across endpoints |
| L/S ratio endpoints | 1000 req/5min/IP | `globalLongShortAccountRatio`, `takerlongshortRatio` |

**Per run consumption (5-symbol fetch):**
- `fetch_market_data.yml`: ~60 weight (well under limits)
- `fetch_klines.yml`: ~60 weight (6 intervals × 5 symbols × 2 weight)
- `fetch_derivatives.yml`: ~50 weight

GitHub Actions runners each have a fresh IP, so rate limits reset between runs.

---

## Local Development

```bash
# Install nothing — only stdlib used
python scripts/fetch_market_data.py
python scripts/fetch_klines.py
python scripts/fetch_derivatives.py
python scripts/daily_summary.py
```

---

## Using the JSON Feed

```js
// Fetch latest prices
const res = await fetch("https://kreizich.github.io/slona-feed/data/latest.json");
const data = await res.json();
console.log(data.spot.BTCUSDT.price);          // "42000.00"
console.log(data.derivatives.BTCUSDT.funding_rate); // "0.00010000"
```

```python
import urllib.request, json
url = "https://kreizich.github.io/slona-feed/data/latest.json"
data = json.loads(urllib.request.urlopen(url).read())
print(data["spot"]["BTCUSDT"]["price"])
```
