"""
CryptoTick HitBTC Pipeline
===========================
Memory-safe pipeline for:
1) first run historical backfill,
2) later daily incremental updates,
3) storage as one Parquet file per coin per day,
4) tiny manifest files to track completed days.

Why this framing:
- We never keep years of data in memory.
- We only request one coin-day at a time.
- We immediately write each day to disk as Parquet.
- On later runs, we skip already completed days using manifest files.
"""

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
import os
import sys
import logging
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.hitbtc.com/api/3/public/trades/{symbol}"
SYMBOL_LIST_URL = "https://api.hitbtc.com/api/3/public/symbol"

COINS = ["ETH", "BTC", "BCH", "BSV", "LTC", "XMR", "ZEC", "DASH"]
QUOTE_CANDIDATES = ["USDT", "USD"]
START_DATE = datetime(2019, 11, 1, tzinfo=timezone.utc)

OUTPUT_DIR = "data/HitBTC_Intraday_Data"
MANIFEST_DIR = "data/manifest"
LOG_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(MANIFEST_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

MAX_LIMIT = 1000
REQUEST_SLEEP = 0.25
MAX_RETRIES = 5

SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("price", pa.float64()),
    ("qty", pa.float64()),
    ("side", pa.string()),
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("coin", pa.string()),
])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "hitbtc_pipeline.log")),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("hitbtc_pipeline")


def manifest_path(coin):
    return os.path.join(MANIFEST_DIR, f"{coin.lower()}.txt")


def get_existing_days(coin):
    path = manifest_path(coin)
    if not os.path.exists(path):
        return set()
    with open(path, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    days = set()
    for line in lines:
        try:
            days.add(datetime.strptime(line, "%Y-%m-%d").date())
        except ValueError:
            continue
    return days


def day_done(coin, done_set):
    def check(day):
        return day.date() in done_set
    return check


def mark_day_done(coin, day, done_set):
    path = manifest_path(coin)
    if day.date() not in done_set:
        with open(path, "a") as f:
            f.write(f"{day.date()}\n")
        done_set.add(day.date())


def resolve_symbol(coin, session):
    try:
        resp = session.get(SYMBOL_LIST_URL, timeout=15)
        resp.raise_for_status()
        available = set(resp.json().keys())
    except Exception as e:
        log.warning(f"Could not fetch symbol list ({e}); assuming USDT pair exists.")
        return f"{coin}USDT"

    for quote in QUOTE_CANDIDATES:
        candidate = f"{coin}{quote}"
        if candidate in available:
            return candidate

    log.error(f"No USDT/USD pair found for {coin} on HitBTC. Skipping.")
    return None


def fetch_trades_for_window(symbol, from_ts_ms, till_ts_ms, session):
    all_trades = []
    current_from = from_ts_ms
    retries = 0

    while True:
        params = {
            "sort": "ASC",
            "by": "timestamp",
            "from": current_from,
            "till": till_ts_ms,
            "limit": MAX_LIMIT,
        }
        try:
            resp = session.get(BASE_URL.format(symbol=symbol), params=params, timeout=30)
        except requests.RequestException as e:
            retries += 1
            if retries > MAX_RETRIES:
                log.error(f"{symbol}: giving up after {MAX_RETRIES} retries ({e})")
                return all_trades, False
            wait = 2 ** retries
            log.warning(f"{symbol}: network error ({e}), retrying in {wait}s...")
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            retries += 1
            if retries > MAX_RETRIES:
                log.error(f"{symbol}: rate limited too many times, giving up after {MAX_RETRIES} retries")
                return all_trades, False
            wait = 2 ** retries
            log.warning(f"{symbol}: rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            log.error(f"{symbol}: HTTP {resp.status_code} at {current_from}: {resp.text[:200]}")
            return all_trades, False

        batch = resp.json()
        if not batch:
            break

        all_trades.extend(batch)
        retries = 0

        last_ts_ms = int(pd.Timestamp(batch[-1]["timestamp"]).timestamp() * 1000)
        if len(batch) < MAX_LIMIT or last_ts_ms >= till_ts_ms:
            break

        current_from = last_ts_ms + 1
        time.sleep(REQUEST_SLEEP)

    return all_trades, True


def trades_to_table(trades, coin):
    df = pd.DataFrame(trades)
    df["price"] = df["price"].astype(float)
    df["qty"] = df["qty"].astype(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["coin"] = coin.lower()
    df = df[["id", "price", "qty", "side", "timestamp", "coin"]]
    df = df.drop_duplicates(subset="id").sort_values("timestamp").reset_index(drop=True)
    return pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)


def write_day_file(table, coin, day):
    coin_dir = os.path.join(OUTPUT_DIR, coin.lower())
    os.makedirs(coin_dir, exist_ok=True)
    filepath = os.path.join(coin_dir, f"{coin.lower()}_{day.date()}.parquet")
    pq.write_table(table, filepath, compression="snappy")
    return filepath


def daterange(start, end):
    current = start
    while current < end:
        yield current
        current += timedelta(days=1)


def download_one_day(coin, symbol, day, session, done_set, is_done_check):
    if is_done_check(day):
        log.info(f"{coin}: {day.date()} already completed, skipping")
        return 0

    from_ms = int(day.timestamp() * 1000)
    till_ms = int((day + timedelta(days=1)).timestamp() * 1000)
    trades, complete = fetch_trades_for_window(symbol, from_ms, till_ms, session)

    if not trades:
        if complete:
            log.info(f"{coin}: no trades for {day.date()}")
        else:
            log.warning(f"{coin}: fetch incomplete for {day.date()}, will retry later")
            return 0
        mark_day_done(coin, day, done_set)
        return 0

    table = trades_to_table(trades, coin)
    filepath = write_day_file(table, coin, day)
    if complete:
        mark_day_done(coin, day, done_set)
        log.info(f"{coin}: saved {len(trades)} trades for {day.date()} -> {filepath}")
    else:
        log.warning(f"{coin}: partial fetch for {day.date()} (wrote {len(trades)} trades), will retry later -> {filepath}")
    time.sleep(REQUEST_SLEEP)
    return len(trades) if complete else 0


def run_backfill(start_date=START_DATE, end_date=None):
    end_date = end_date or datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    session = requests.Session()

    for coin in COINS:
        symbol = resolve_symbol(coin, session)
        if symbol is None:
            continue

        done_set = get_existing_days(coin)
        is_done_check = day_done(coin, done_set)

        log.info(f"=== Backfilling {symbol} from {start_date.date()} to {end_date.date()} ===")
        total = 0
        for day in daterange(start_date, end_date):
            total += download_one_day(coin, symbol, day, session, done_set, is_done_check)
        log.info(f"=== {coin}: backfill complete, {total} new trades ===")


def run_daily_update():
    session = requests.Session()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    for coin in COINS:
        symbol = resolve_symbol(coin, session)
        if symbol is None:
            continue

        done_set = get_existing_days(coin)
        is_done_check = day_done(coin, done_set)

        existing_days = sorted(done_set)
        if existing_days:
            resume_from = datetime.combine(
                existing_days[-1] + timedelta(days=1), datetime.min.time()
            ).replace(tzinfo=timezone.utc)
        else:
            resume_from = START_DATE

        if resume_from > yesterday:
            log.info(f"{coin}: already up to date")
            continue

        log.info(f"=== Updating {symbol} from {resume_from.date()} through {yesterday.date()} ===")
        total = 0
        for day in daterange(resume_from, yesterday + timedelta(days=1)):
            total += download_one_day(coin, symbol, day, session, done_set, is_done_check)
        log.info(f"=== {coin}: update complete, {total} new trades ===")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "backfill"

    if mode == "backfill":
        run_backfill()
    elif mode == "update":
        run_daily_update()
    else:
        print("Usage: python hitbtc_pipeline.py [backfill|update]")
        sys.exit(1)
