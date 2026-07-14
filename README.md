# CryptoTick — Automated Intraday Cryptocurrency Data Pipeline

## Overview

**CryptoTick** is an automated data collection and processing pipeline for building a continuously growing historical archive of tick-level cryptocurrency trade data from the HitBTC exchange. It downloads raw trades (price, quantity, side, timestamp) for a fixed set of coins, keeps the archive updated daily via GitHub Actions, and provides tools to aggregate raw trades into OHLCV bars and realized volatility measures for use in event studies and other financial research.

This project was built to support ETH-focused event study research (cumulative abnormal returns, synthetic control analysis, and realized volatility estimation), but the underlying data pipeline is coin-agnostic and can be extended to any symbol available on HitBTC.

## Coins tracked

- ETH (Ethereum)
- BTC (Bitcoin)
- BCH (Bitcoin Cash)
- BSV (Bitcoin SV)
- LTC (Litecoin)
- XMR (Monero)
- ZEC (Zcash)
- DASH (Dash)

## Data source

All trade data is pulled from **HitBTC's public REST API v3** (`/api/3/public/trades/{symbol}`), which provides tick-level trade history with no authentication required. See [HitBTC API docs](https://api.hitbtc.com/) for reference.

## Project structure
├── .github/workflows/
│ └── update_hitbtc_data.yml # GitHub Actions workflow: runs the daily update job
├── data/
│ └── IntradayData_hitbtc_SG/ # One CSV per coin, appended to daily
│ ├── Intra_eth_full.csv
│ ├── Intra_btc_full.csv
│ └── ...
├── download_hitbtc_trades_single_csv.py # One-time historical backfill script
├── update_hitbtc_trades_daily.py # Incremental daily update script
└── README.md


## How it works

1. **Historical backfill** (`download_hitbtc_trades_single_csv.py`): run once to populate the full historical archive for each coin over a specified date range, paginating through HitBTC's API in daily windows and appending results into a single CSV per coin.

2. **Daily update** (`update_hitbtc_trades_daily.py`): reads the last stored timestamp in each coin's CSV, fetches only new trades since that point, and appends them — keeping the archive current without re-downloading historical data.

3. **Automation** (`.github/workflows/update_hitbtc_data.yml`): a GitHub Actions workflow runs the daily update script automatically every day at 02:00 UTC (also triggerable manually), and commits any new data back to the repository.

## Data schema

Each coin's CSV file contains one row per trade, with the following columns:

| Column | Description |
|---|---|
| `id` | Unique trade ID assigned by HitBTC |
| `price` | Execution price of the trade |
| `qty` | Quantity traded |
| `side` | `buy` or `sell` |
| `timestamp` | UTC timestamp of the trade (millisecond precision) |
| `coin` | Lowercase coin ticker (e.g., `eth`) |

## From raw trades to analysis-ready data

The raw tick data in this repository is the foundation for further downstream processing, including:

- **OHLCV bar construction**: aggregating trades into fixed time buckets (1-minute, hourly, daily) by taking first/max/min/last price and summed quantity per bucket.
- **Realized volatility (RV), Bipower Variation (BPV), and Jump Variation (JV)**: standard financial econometrics measures computed from intraday log returns.
- **Event studies**: using the resulting return series to estimate abnormal returns and cumulative abnormal returns (CAR) around specific market events.

## Setup

```bash
pip install requests pandas
python download_hitbtc_trades_single_csv.py   # initial backfill
```

To enable automated daily updates, push this repository to GitHub with the included workflow file in `.github/workflows/`. Ensure the repository has **Actions enabled** and that workflow permissions allow **read and write** access (Settings → Actions → General → Workflow permissions).

## Notes and limitations

- HitBTC's public API enforces rate limits; the scripts include small delays and automatic retry-on-429 handling.
- Some smaller-cap coins may not have a direct USDT trading pair on HitBTC — verify available symbols via HitBTC's `/api/3/public/symbol` endpoint if a coin returns no data.
- As the dataset grows, consider migrating large CSV files to Git LFS or an external storage solution (e.g., Parquet archives) to keep the repository lightweight.
