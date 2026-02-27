"""
Shared Supabase client for all data pipelines.

Reads connection details from environment variables:
  SUPABASE_URL          - e.g. https://xxxx.supabase.co
  SUPABASE_SERVICE_KEY  - service_role key (not anon)

Usage:
    from shared.supabase_client import batch_upsert

    count = batch_upsert("zefix_companies", records, conflict_column="uid")
"""

import os
import sys
import time
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds: 2, 4, 8


def _get_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }


def _upsert_single_batch(table, records, conflict_column):
    """Upsert one batch with retry logic. Returns number of rows upserted."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conflict_column}"
    headers = _get_headers()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(url, headers=headers, json=records, timeout=30)
            if r.status_code in (200, 201):
                return len(records)
            elif r.status_code == 429:
                # Rate limited - always retry
                wait = RETRY_BACKOFF_BASE ** attempt
                print(f"    Rate limited, retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            elif r.status_code >= 500:
                # Server error - retry
                wait = RETRY_BACKOFF_BASE ** attempt
                print(f"    Server error {r.status_code}, retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            else:
                # Client error (4xx) - don't retry
                print(f"    ERROR {r.status_code}: {r.text[:300]}")
                return 0
        except requests.exceptions.Timeout:
            wait = RETRY_BACKOFF_BASE ** attempt
            print(f"    Timeout, retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
        except requests.exceptions.RequestException as e:
            wait = RETRY_BACKOFF_BASE ** attempt
            print(f"    Request error: {e}, retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)

    print(f"    FAILED after {MAX_RETRIES} attempts")
    return 0


def batch_upsert(table, records, conflict_column, batch_size=500):
    """
    Upsert records into a Supabase table in batches.

    Args:
        table:            Target table name (e.g. "zefix_companies")
        records:          List of dicts to upsert
        conflict_column:  Column for ON CONFLICT (e.g. "uid")
        batch_size:       Records per batch (default 500)

    Returns:
        Total number of rows successfully upserted.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        sys.exit(1)

    if not records:
        return 0

    total_upserted = 0
    total_batches = (len(records) + batch_size - 1) // batch_size

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        batch_num = i // batch_size + 1
        count = _upsert_single_batch(table, batch, conflict_column)
        total_upserted += count

        status = "OK" if count > 0 else "FAIL"
        print(f"    Batch {batch_num}/{total_batches}: {count}/{len(batch)} rows [{status}]")

        time.sleep(0.3)  # rate limiting between batches

    return total_upserted
