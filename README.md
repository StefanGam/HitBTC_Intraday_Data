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
