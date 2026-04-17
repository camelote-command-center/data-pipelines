# Candle Updater

Fetches OHLCV candle data from crypto exchanges and writes to `public.candles` in the Backtesting Supabase project.

## Sources

| Asset | Exchange | Endpoint |
|-------|----------|----------|
| BTC, ETH, SOL, BNB, DOGE, XRP | Binance | `api/v3/klines` |
| HYPE | OKX | `api/v5/market/candles` + `market/history-candles` |

## Timeframes

1m, 5m, 15m (21 asset/timeframe pairs total)

## Schedule

Every 12 hours via GitHub Actions: `cron: '5 0,12 * * *'`

## Target

- **Project**: Backtesting (`uwjhtxansbwqsydsihso`)
- **Table**: `public.candles`
- **Write mode**: `INSERT ... ON CONFLICT (asset, timeframe, open_time) DO NOTHING`

## Owner

xGhozt Bot

## Run locally

```bash
cp .env.example .env
# Fill in credentials
python pipelines/candle-updater/parser.py
```

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BACKTESTING_DB_PASSWORD` | Yes | Backtesting project DB password |
| `BACKTESTING_DB_HOST` | No | DB host (defaults to Backtesting project) |
| `CAMELOTE_DATA_DB_URL` | No | Postgres connection string for acquisition_logs |
| `CAMELOTE_SUPABASE_URL` | No | Camelote Data URL for dataset metadata |
| `CAMELOTE_SUPABASE_KEY` | No | Camelote Data service key |
| `CANDLE_UPDATER_DATASET_ID` | No | Dataset UUID for acquisition_logs |

## Notes

- Uses psycopg2 for bulk inserts (performance-critical with millions of rows)
- Binance rate limiting: reads `X-MBX-USED-WEIGHT-1M` header, adapts sleep
- OKX paginates backwards from newest; uses both `/candles` and `/history-candles`
- Safe for parallel runs (ON CONFLICT DO NOTHING)
