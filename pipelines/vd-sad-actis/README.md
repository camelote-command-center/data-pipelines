# vd-sad-actis

Scraper for Canton de Vaud building permits from the ACTIS platform (CAMAC system).

Fetches HTML pages from the ACTIS REST API, parses structured permit data, and upserts into `bronze.sad_national`.

## Setup

```bash
npm install
```

Set environment variables:

```bash
export SUPABASE_URL="https://xxx.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="eyJ..."
```

## Usage

### 1. Probe the valid ID range

```bash
npx tsx probe-range.ts
```

Tests a set of candidate IDs to find where valid CAMAC records start and end. Updates `state.json` with `range_min` and `range_max`.

### 2. Backfill permits

```bash
# Use state.json defaults (last_processed_id + 1, chunk of 5000)
npx tsx fetch-and-parse.ts

# Start at a specific ID with a custom chunk size
npx tsx fetch-and-parse.ts 220000 10000
```

Rate-limited to 1 request/second. Progress is saved to `state.json` after every 100-record batch, so the script can be safely interrupted and resumed.

## Target table

`bronze.sad_national` with `UNIQUE(source_id, canton)` where `canton = 'VD'` and `source_system = 'actis_vd'`.
