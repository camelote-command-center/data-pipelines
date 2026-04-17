"""Candle updater — fetches missing 1m/5m/15m OHLCV candles for 7 crypto assets.

Sources:
  - Binance klines API (BTC, ETH, SOL, BNB, DOGE, XRP) — free, no auth
  - OKX market candles API (HYPE) — free, no auth

Target:
  - Backtesting Supabase project, public.candles table
  - ON CONFLICT (asset, timeframe, open_time) DO NOTHING

Schedule: cron '5 0,12 * * *' (00:05 and 12:05 UTC)

Logs each run to camelote_data.public.acquisition_logs and updates
dataset metadata via shared/freshness.py.
"""

import os
import sys
import time
import traceback
import requests
from datetime import datetime, timezone

# Allow imports from shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.freshness import update_dataset_meta

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BACKTESTING_DB_HOST = os.environ.get(
    "BACKTESTING_DB_HOST", "db.uwjhtxansbwqsydsihso.supabase.co"
)
BACKTESTING_DB_PASSWORD = os.environ.get("BACKTESTING_DB_PASSWORD")

CAMELOTE_DATA_DB_URL = os.environ.get("CAMELOTE_DATA_DB_URL")
CAMELOTE_SUPABASE_URL = os.environ.get("CAMELOTE_SUPABASE_URL")
CAMELOTE_SUPABASE_KEY = os.environ.get("CAMELOTE_SUPABASE_KEY")
DATASET_ID = os.environ.get("CANDLE_UPDATER_DATASET_ID")

# WebShare HTTP proxy — needed because GitHub Actions runners are in
# US datacenters and Binance returns HTTP 451 (geo-blocked).
PROXY_USER = os.environ.get("WEBSHARE_PROXY_USER")
PROXY_PASS = os.environ.get("WEBSHARE_PROXY_PASS")
PROXY_URL = (
    f"http://{PROXY_USER}:{PROXY_PASS}@p.webshare.io:80"
    if PROXY_USER and PROXY_PASS else None
)

BINANCE_URL = "https://api.binance.com/api/v3/klines"

# All assets to ingest (Binance symbol -> DB asset name)
# HYPE is on OKX, not Binance
ASSETS = {
    "BTC":  "BTCUSDT",
    "ETH":  "ETHUSDT",
    "SOL":  "SOLUSDT",
    "BNB":  "BNBUSDT",
    "DOGE": "DOGEUSDT",
    "XRP":  "XRPUSDT",
    "HYPE": None,  # OKX, not Binance
}

OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
OKX_HISTORY_URL = "https://www.okx.com/api/v5/market/history-candles"

TIMEFRAMES = {
    "1m":  {"binance": "1m",  "ms": 60_000},
    "5m":  {"binance": "5m",  "ms": 300_000},
    "15m": {"binance": "15m", "ms": 900_000},
}

# All (asset, timeframe) pairs to keep fresh
INGEST_PAIRS = [(a, t) for a in ASSETS for t in TIMEFRAMES]

BINANCE_LIMIT = 1000      # max candles per request
BATCH_SIZE = 1000          # rows per DB insert
MIN_DELAY = 0.12           # seconds between Binance requests

# ---------------------------------------------------------------------------
# DB helpers (psycopg2 for bulk insert performance)
# ---------------------------------------------------------------------------

_conn = None


def get_conn():
    """Get psycopg2 connection to Backtesting candles DB."""
    global _conn
    if _conn is None or _conn.closed:
        import psycopg2
        _conn = psycopg2.connect(
            host=BACKTESTING_DB_HOST, port=5432, dbname="postgres",
            user="postgres", password=BACKTESTING_DB_PASSWORD,
        )
        _conn.autocommit = True
    return _conn


def get_latest_ms(asset, timeframe):
    """Return MAX(open_time) as epoch-ms, or None."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(open_time) FROM candles WHERE asset=%s AND timeframe=%s",
        (asset, timeframe),
    )
    row = cur.fetchone()
    cur.close()
    if row and row[0]:
        return int(row[0].timestamp() * 1000)
    return None


def bulk_insert(rows):
    """Insert candle rows via psycopg2. ON CONFLICT DO NOTHING. Returns inserted count.

    Note: is_green, body_size, upper_wick, lower_wick, candle_range are
    GENERATED ALWAYS columns — do NOT include them in INSERT.
    """
    if not rows:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO candles (
            asset, timeframe, open_time, close_time,
            open, high, low, close, volume,
            hour_utc, dow_utc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (asset, timeframe, open_time) DO NOTHING
    """
    from psycopg2.extras import execute_batch
    execute_batch(cur, sql, rows, page_size=BATCH_SIZE)
    inserted = cur.rowcount  # approximate — ON CONFLICT may reduce this
    cur.close()
    return inserted


# ---------------------------------------------------------------------------
# Binance fetcher with adaptive rate limiting
# ---------------------------------------------------------------------------

def fetch_binance(session, symbol, interval, start_ms):
    """Fetch up to 1000 candles from Binance. Returns (raw_klines, weight_used)."""
    for attempt in range(5):
        try:
            r = session.get(BINANCE_URL, params={
                "symbol": symbol,
                "interval": interval,
                "startTime": start_ms,
                "limit": BINANCE_LIMIT,
            }, timeout=30)

            weight = int(r.headers.get("X-MBX-USED-WEIGHT-1M", "0"))

            if r.status_code == 429:
                print(f"    [RATE LIMIT] 429 from Binance, sleeping 60s...")
                time.sleep(60)
                continue

            if r.status_code >= 400:
                print(f"    [WARN] Binance HTTP {r.status_code}: {r.text[:200]}")
                time.sleep(2 ** attempt)
                continue

            return r.json(), weight

        except requests.exceptions.RequestException as e:
            print(f"    [WARN] Binance request failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt * 2)

    return [], 0


def adaptive_sleep(weight):
    """Sleep based on Binance rate limit weight."""
    if weight > 1000:
        time.sleep(2.0)
    elif weight > 800:
        time.sleep(0.5)
    elif weight > 500:
        time.sleep(0.2)
    else:
        time.sleep(MIN_DELAY)


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform_binance(raw, asset, timeframe):
    """Convert Binance kline array to tuple for psycopg2 insert."""
    open_ms = raw[0]
    close_ms = raw[6]
    o = float(raw[1])
    h = float(raw[2])
    lo = float(raw[3])
    c = float(raw[4])
    vol = float(raw[5])

    open_dt = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)
    close_dt = datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc)
    hour_utc = open_dt.hour
    dow_utc = (open_dt.weekday() + 1) % 7  # Python Mon=0 -> PG Sun=0

    return (
        asset, timeframe,
        open_dt, close_dt,
        o, h, lo, c, vol,
        hour_utc, dow_utc,
    )


def transform_okx(raw, asset, timeframe):
    """Convert OKX kline array to tuple for psycopg2 insert."""
    open_ms = int(raw[0])
    o = float(raw[1])
    h = float(raw[2])
    lo = float(raw[3])
    c = float(raw[4])
    vol = float(raw[5])

    ms_interval = TIMEFRAMES[timeframe]["ms"]
    open_dt = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)
    close_dt = datetime.fromtimestamp((open_ms + ms_interval - 1) / 1000, tz=timezone.utc)
    hour_utc = open_dt.hour
    dow_utc = (open_dt.weekday() + 1) % 7

    return (
        asset, timeframe,
        open_dt, close_dt,
        o, h, lo, c, vol,
        hour_utc, dow_utc,
    )


# ---------------------------------------------------------------------------
# Ingest one pair
# ---------------------------------------------------------------------------

def ingest_pair(session, asset, timeframe):
    """Fetch all missing candles for one (asset, timeframe). Returns count."""
    symbol = ASSETS[asset]
    tf = TIMEFRAMES[timeframe]
    ms_step = tf["ms"]

    latest_ms = get_latest_ms(asset, timeframe)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if latest_ms is None:
        start_ms = int(datetime(2023, 2, 15, tzinfo=timezone.utc).timestamp() * 1000)
        print(f"  {asset}/{timeframe}: no data, starting from 2023-02-15")
    else:
        start_ms = latest_ms + ms_step
        gap_hours = (now_ms - latest_ms) / 3_600_000
        if gap_hours < 0.5:
            print(f"  {asset}/{timeframe}: up to date (gap {gap_hours:.1f}h)")
            return 0
        print(f"  {asset}/{timeframe}: gap {gap_hours:.1f}h, fetching from "
              f"{datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")

    # Route to OKX for HYPE, Binance for everything else
    if symbol is None:
        return _ingest_okx(session, asset, timeframe, start_ms, now_ms, ms_step)
    return _ingest_binance(session, asset, timeframe, symbol, start_ms, now_ms, ms_step)


def _ingest_binance(session, asset, timeframe, symbol, start_ms, now_ms, ms_step):
    """Fetch from Binance klines API."""
    interval = TIMEFRAMES[timeframe]["binance"]
    total = 0
    cursor = start_ms
    buffer = []

    while cursor < now_ms:
        raw, weight = fetch_binance(session, symbol, interval, cursor)
        if not raw:
            break

        for k in raw:
            buffer.append(transform_binance(k, asset, timeframe))

        if len(buffer) >= BATCH_SIZE:
            bulk_insert(buffer)
            total += len(buffer)
            buffer = []

        last_ms = raw[-1][0]
        cursor = last_ms + ms_step

        adaptive_sleep(weight)

        if total > 0 and total % 10000 < BINANCE_LIMIT:
            dt = datetime.fromtimestamp(cursor/1000, tz=timezone.utc)
            print(f"    ... {total:,} candles, at {dt.strftime('%Y-%m-%d %H:%M')}")

    if buffer:
        bulk_insert(buffer)
        total += len(buffer)

    return total


def _ingest_okx(session, asset, timeframe, start_ms, now_ms, ms_step):
    """Fetch from OKX candles API (for HYPE and any non-Binance assets)."""
    inst_id = f"{asset}-USDT"
    total = 0
    buffer = []
    seen = set()

    for url in [OKX_CANDLES_URL, OKX_HISTORY_URL]:
        after_ts = None
        max_pages = 100

        for _ in range(max_pages):
            params = {"instId": inst_id, "bar": timeframe, "limit": "100"}
            if after_ts:
                params["after"] = str(after_ts)

            try:
                r = session.get(url, params=params, timeout=15)
            except Exception as e:
                print(f"    [WARN] OKX error: {e}")
                time.sleep(2)
                continue

            if r.status_code == 429:
                time.sleep(10)
                continue

            data = r.json().get("data", [])
            if not data:
                break

            new_count = 0
            oldest_in_batch = None
            for k in data:
                ts = int(k[0])
                if ts not in seen and ts >= start_ms:
                    seen.add(ts)
                    buffer.append(transform_okx(k, asset, timeframe))
                    new_count += 1
                if oldest_in_batch is None or ts < oldest_in_batch:
                    oldest_in_batch = ts

            if new_count == 0 or (oldest_in_batch and oldest_in_batch < start_ms):
                break

            after_ts = int(data[-1][0])

            if len(buffer) >= BATCH_SIZE:
                bulk_insert(buffer)
                total += len(buffer)
                buffer = []

            time.sleep(0.15)

    if buffer:
        bulk_insert(buffer)
        total += len(buffer)

    return total


# ---------------------------------------------------------------------------
# Acquisition logging (camelote_data)
# ---------------------------------------------------------------------------

_camelote_conn = None


def _get_camelote_conn():
    """Get psycopg2 connection to camelote_data for acquisition_logs."""
    global _camelote_conn
    if not CAMELOTE_DATA_DB_URL:
        return None
    if _camelote_conn is None or _camelote_conn.closed:
        import psycopg2
        try:
            _camelote_conn = psycopg2.connect(CAMELOTE_DATA_DB_URL)
            _camelote_conn.autocommit = True
        except Exception as e:
            print(f"  [WARN] Could not connect to camelote_data: {e}")
            return None
    return _camelote_conn


def start_acquisition_log():
    """Insert a 'running' acquisition_logs row. Returns the row UUID or None."""
    if not DATASET_ID:
        print("  [WARN] CANDLE_UPDATER_DATASET_ID not set, skipping acquisition log")
        return None
    conn = _get_camelote_conn()
    if not conn:
        print("  [WARN] CAMELOTE_DATA_DB_URL not set, skipping acquisition log")
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO acquisition_logs (dataset_id, status, triggered_by)
               VALUES (%s, 'running', 'github_actions')
               RETURNING id""",
            (DATASET_ID,),
        )
        row_id = cur.fetchone()[0]
        cur.close()
        print(f"  Acquisition log started: {row_id}")
        return str(row_id)
    except Exception as e:
        print(f"  [WARN] Could not start acquisition log: {e}")
        return None


def complete_acquisition_log(log_id, status, records_fetched, records_new, error_message=None, duration_seconds=None):
    """Update the acquisition_logs row with final stats."""
    if not log_id:
        return
    conn = _get_camelote_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """UPDATE acquisition_logs
               SET status = %s,
                   completed_at = now(),
                   duration_seconds = %s,
                   records_fetched = %s,
                   records_new = %s,
                   error_message = %s
               WHERE id = %s""",
            (status, duration_seconds, records_fetched, records_new, error_message, log_id),
        )
        cur.close()
        print(f"  Acquisition log completed: {status}")
    except Exception as e:
        print(f"  [WARN] Could not complete acquisition log: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not BACKTESTING_DB_PASSWORD:
        print("ERROR: BACKTESTING_DB_PASSWORD is required")
        sys.exit(1)

    start_time = time.time()
    print("=" * 60)
    print("  CANDLE UPDATE — All assets x 1m/5m/15m")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    session = requests.Session()
    session.headers["User-Agent"] = "candle-updater/2.0"
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
        print(f"  Using WebShare proxy (user: {PROXY_USER})")
    else:
        print("  No proxy configured, connecting directly")

    log_id = start_acquisition_log()

    results = {}
    errors = []
    total_inserted = 0

    for asset, timeframe in INGEST_PAIRS:
        label = f"{asset}/{timeframe}"
        try:
            count = ingest_pair(session, asset, timeframe)
            results[label] = count
            if count > 0:
                total_inserted += count
                print(f"  Ingested {count:,} candles for {label}")
        except Exception as e:
            results[label] = f"ERROR: {e}"
            errors.append(label)
            traceback.print_exc()

    elapsed = int(time.time() - start_time)

    # Summary
    print("\n" + "=" * 60)
    print("  UPDATE SUMMARY")
    for label, count in sorted(results.items()):
        if isinstance(count, int):
            status = f"{count:>8,} candles" if count > 0 else "up to date"
        else:
            status = count
        print(f"    {label:12s} {status}")
    print(f"\n  Total: {total_inserted:,} candles in {elapsed}s")
    if errors:
        print(f"  Errors: {', '.join(errors)}")
    print("=" * 60)

    # Acquisition log
    acq_status = "failed" if errors else "success"
    if errors and total_inserted > 0:
        acq_status = "partial"
    complete_acquisition_log(
        log_id,
        status=acq_status,
        records_fetched=total_inserted,
        records_new=total_inserted,
        error_message=", ".join(errors) if errors else None,
        duration_seconds=elapsed,
    )

    # Dataset metadata update
    update_dataset_meta(
        url=CAMELOTE_SUPABASE_URL,
        key=CAMELOTE_SUPABASE_KEY,
        dataset_code="crypto-candles",
        record_count=total_inserted,
        status="active" if not errors else "error",
        last_error=", ".join(errors) if errors else None,
    )

    # Close DB connections
    global _conn, _camelote_conn
    if _conn and not _conn.closed:
        _conn.close()
    if _camelote_conn and not _camelote_conn.closed:
        _camelote_conn.close()

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
