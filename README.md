# CryptoTick: HitBTC Intraday Data

This repository builds and publishes tick-level intraday cryptocurrency trade data from HitBTC.

## Coverage

- Coins: ETH, BTC, BCH, BSV, LTC, XMR, ZEC, DASH
- Storage format: Parquet
- Partitioning: one file per coin per day
- Source: HitBTC public REST API v3

## Update logic

- First run: a full historical backfill is downloaded locally.
- Later runs: the workflow downloads only missing days.
- Daily GitHub Releases publish incremental datasets.
- Monthly GitHub Releases publish full-history snapshots for citation and archiving.

## Repository contents

- `hitbtc_pipeline.py`: downloader and updater
- `.github/workflows/daily_release.yml`: daily incremental release workflow
- `.github/workflows/monthly_full_history.yml`: monthly full-history release workflow
- `data/manifest/`: tiny text files tracking completed dates per coin
- `logs/`: pipeline logs

## How to run locally

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the first historical download:

```bash
python hitbtc_pipeline.py backfill
```

Run a later incremental update:

```bash
python hitbtc_pipeline.py update
```

## Citation

Please cite the Zenodo DOI for the latest full-history release once Zenodo is connected and the first archived release has been created.

## Zenodo

After connecting this repository to Zenodo, each GitHub Release can be archived automatically on Zenodo. The recommended citable records are the monthly full-history releases.
