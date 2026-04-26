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

    # Dual-write to lamap_db (primary) + re-LLM (mirror):
    from shared.supabase_client import dual_batch_upsert

    count = dual_batch_upsert(
        records=records,
        table_lamap="SAD",
        table_rellm="ge_sad",
        conflict_column="type,numero,numero_complementaire",
    )
"""

import os
import sys
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
                if attempt == 1:
                    print(f"    Response: {r.text[:500]}")
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


def dual_batch_upsert(
    records,
    *,
    table_lamap,
    table_rellm,
    conflict_column,
    schema_lamap=None,
    schema_rellm=None,
    batch_size=500,
):
    """
    Two-target UPSERT: writes to lamap_db (PRIMARY) then mirrors to re-LLM (SECONDARY).

    Behavior:
      - PRIMARY (lamap_db) write goes through batch_upsert unchanged. Its
        return value is what the caller gets back, so the existing "0 rows ⇒
        sys.exit(1)" pattern in callers still works.
      - SECONDARY (re-LLM) write is best-effort: any exception is logged to
        stderr and swallowed. Missing re-LLM credentials log a SKIPPED notice
        and skip silently. The mirror MUST NEVER fail the parent pipeline.

    Credentials are read from environment so callers stay terse:
        LAMAP_SUPABASE_URL, LAMAP_SUPABASE_SERVICE_KEY      (PRIMARY)
        RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY (SECONDARY)
        LAMAP_SCHEMA   (default "bronze")
        RE_LLM_SCHEMA  (default "bronze_ch")

    Args:
        records:          List of dicts (same payload sent to both targets).
        table_lamap:      Table name on lamap_db (e.g. "SAD" — quoted by PostgREST).
        table_rellm:      Table name on re-LLM (e.g. "ge_sad").
        conflict_column:  ON CONFLICT key. Must exist as a UNIQUE constraint
                          on BOTH targets (this is the caller's responsibility
                          to verify before adopting the helper).
        schema_lamap:     Override LAMAP_SCHEMA env (rare).
        schema_rellm:     Override RE_LLM_SCHEMA env (rare).
        batch_size:       Forwarded to both batch_upsert calls.

    Returns:
        int — rows upserted into the PRIMARY (lamap_db) target.
    """
    lamap_url = os.environ.get("LAMAP_SUPABASE_URL", "")
    lamap_key = os.environ.get("LAMAP_SUPABASE_SERVICE_KEY", "")
    rellm_url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    rellm_key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    schema_lamap = schema_lamap or os.environ.get("LAMAP_SCHEMA", "bronze")
    schema_rellm = schema_rellm or os.environ.get("RE_LLM_SCHEMA", "bronze_ch")

    print(f"  [PRIMARY] lamap_db {schema_lamap}.{table_lamap} ← {len(records):,} rows")
    primary_count = batch_upsert(
        url=lamap_url,
        key=lamap_key,
        table=table_lamap,
        records=records,
        conflict_column=conflict_column,
        schema=schema_lamap,
        batch_size=batch_size,
    )

    if not rellm_url or not rellm_key:
        print(
            "  [re-LLM dual-write SKIPPED] RE_LLM_SUPABASE_URL or "
            "RE_LLM_SUPABASE_SERVICE_ROLE_KEY not set in env",
            file=sys.stderr,
        )
        return primary_count

    print(f"  [SECONDARY] re-LLM {schema_rellm}.{table_rellm} ← {len(records):,} rows")
    try:
        mirror_count = batch_upsert(
            url=rellm_url,
            key=rellm_key,
            table=table_rellm,
            records=records,
            conflict_column=conflict_column,
            schema=schema_rellm,
            batch_size=batch_size,
        )
        print(
            f"  [re-LLM dual-write OK] {mirror_count:,} rows mirrored to "
            f"{schema_rellm}.{table_rellm}"
        )
    except Exception as e:
        print(f"[re-LLM dual-write FAILED] {e}", file=sys.stderr)

    return primary_count
