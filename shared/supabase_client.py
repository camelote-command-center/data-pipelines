"""
Shared Supabase client for all data pipelines.

Usage:
    from shared.supabase_client import batch_upsert

    count = batch_upsert(
        url="https://xxxx.supabase.co",
        key="eyJ...",
        table="zefix_companies",
        records=records,
        conflict_column="uid",
        schema="bronze",       # optional, defaults to "public"
    )
"""

import time
import requests

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds: 2, 4, 8


def _build_headers(key, schema="public"):
    """Build Supabase REST headers with correct schema profile."""
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    # For non-public schemas, PostgREST needs Content-Profile on writes
    if schema and schema != "public":
        headers["Content-Profile"] = schema
    return headers


def _upsert_single_batch(url, key, table, records, conflict_column, schema="public"):
    """Upsert one batch with retry logic. Returns number of rows upserted."""
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?on_conflict={conflict_column}"
    headers = _build_headers(key, schema)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(endpoint, headers=headers, json=records, timeout=30)
            if r.status_code in (200, 201):
                return len(records)
            elif r.status_code == 429:
                wait = RETRY_BACKOFF_BASE ** attempt
                print(f"    Rate limited, retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            elif r.status_code >= 500:
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


def batch_upsert(url, key, table, records, conflict_column, schema="public", batch_size=500):
    """
    Upsert records into a Supabase table in batches.

    Args:
        url:              Supabase project URL (e.g. "https://xxxx.supabase.co")
        key:              Supabase service_role key
        table:            Target table name (e.g. "zefix_companies")
        records:          List of dicts to upsert
        conflict_column:  Column for ON CONFLICT (e.g. "uid")
        schema:           Target schema (default "public"). Non-public schemas
                          use Content-Profile header for PostgREST routing.
        batch_size:       Records per batch (default 500)

    Returns:
        Total number of rows successfully upserted.
    """
    if not url or not key:
        print("ERROR: url and key are required")
        return 0

    if not records:
        return 0

    total_upserted = 0
    total_batches = (len(records) + batch_size - 1) // batch_size

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        batch_num = i // batch_size + 1
        count = _upsert_single_batch(url, key, table, batch, conflict_column, schema)
        total_upserted += count

        status = "OK" if count > 0 else "FAIL"
        print(f"    Batch {batch_num}/{total_batches}: {count}/{len(batch)} rows [{status}]")

        time.sleep(0.3)

    return total_upserted
