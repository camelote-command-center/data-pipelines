# Camelote Data Pipelines

Clean, automated data pipelines for Camelote startups. API-based only (no browser, no proxies needed). Each pipeline runs as a GitHub Actions workflow on a schedule.

## Structure

```
data-pipelines/
  .github/workflows/    # GitHub Actions workflow definitions
  pipelines/            # One folder per pipeline
    zefix/              # Swiss company registry import
  shared/               # Reusable utilities
    supabase_client.py  # Batch upsert with retries
  requirements.txt
```

## Pipelines

| Pipeline | Source | Schedule | Target Table |
|----------|--------|----------|--------------|
| `zefix` | Basel Open Data CSV (26 cantons) | 1st of month, 04:00 UTC | `zefix_companies` |

## Setup

### 1. Add GitHub Secrets

Go to **Settings > Secrets and variables > Actions** and add:

| Secret | Value |
|--------|-------|
| `CAMELOTE_DATA_SUPABASE_URL` | `https://dxugbpeacnorjunpljih.supabase.co` |
| `CAMELOTE_DATA_SUPABASE_SERVICE_KEY` | Your service_role key from Supabase dashboard |

### 2. Manual trigger

Go to **Actions** tab > select the workflow > click **Run workflow**.

## Adding a new pipeline

1. Create `pipelines/<name>/import.py`
2. Use `from shared.supabase_client import batch_upsert`
3. Add a workflow in `.github/workflows/<name>.yml`
4. Update `requirements.txt` if new dependencies are needed
