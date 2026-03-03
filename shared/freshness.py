"""
Shared freshness-check utilities for data pipelines.

Avoids re-downloading and re-uploading millions of identical rows by:
  1. Checking source HTTP Last-Modified / Content-Length before downloading
  2. Comparing with `last_acquired_at` from the datasets table on camelote_data
  3. Updating metadata (last_acquired_at, record_count) after successful imports

Usage:
    from shared.freshness import source_modified_since, get_dataset_meta, update_dataset_meta

Environment variables:
    CAMELOTE_SUPABASE_URL  - Camelote command-center Supabase URL
    CAMELOTE_SUPABASE_KEY  - Camelote service_role or anon key
"""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests


# ──────────────────────────────────────────────────────────────
# Source freshness (HTTP HEAD)
# ──────────────────────────────────────────────────────────────

def get_source_last_modified(url: str, timeout: int = 15) -> datetime | None:
    """
    HTTP HEAD to get Last-Modified datetime from a remote source.
    Returns timezone-aware UTC datetime or None.
    """
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        lm = r.headers.get("Last-Modified")
        if lm:
            dt = parsedate_to_datetime(lm)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
    except Exception as e:
        print(f"  Warning: HEAD request failed for {url}: {e}")
    return None


def source_modified_since(url: str, since: datetime, timeout: int = 15) -> bool:
    """
    Check whether a remote source has been modified since `since`.
    Returns True if source is newer or if we can't determine (fail-open).
    """
    source_dt = get_source_last_modified(url, timeout)
    if source_dt is None:
        print("  Could not determine source Last-Modified, proceeding with import")
        return True

    # Ensure both are tz-aware for comparison
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)

    if source_dt <= since:
        print(f"  Source Last-Modified: {source_dt.isoformat()}")
        print(f"  Last acquired:       {since.isoformat()}")
        print(f"  Source has NOT changed since last import")
        return False

    print(f"  Source Last-Modified: {source_dt.isoformat()}")
    print(f"  Last acquired:       {since.isoformat()}")
    print(f"  Source HAS changed, proceeding with import")
    return True


# ──────────────────────────────────────────────────────────────
# Dataset metadata (camelote_data)
# ──────────────────────────────────────────────────────────────

def get_dataset_meta(url: str, key: str, dataset_code: str) -> dict | None:
    """
    Fetch dataset metadata from the command-center datasets table.
    Returns dict with last_acquired_at (datetime), record_count (int), etc.
    Returns None on failure.
    """
    if not url or not key:
        return None

    endpoint = (
        f"{url.rstrip('/')}/rest/v1/datasets"
        f"?code=eq.{dataset_code}"
        f"&select=code,last_acquired_at,record_count,status"
    )
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
    }
    try:
        r = requests.get(endpoint, headers=headers, timeout=15)
        if r.status_code == 200:
            rows = r.json()
            if rows:
                row = rows[0]
                la = row.get("last_acquired_at")
                if la:
                    row["last_acquired_at"] = datetime.fromisoformat(
                        la.replace("Z", "+00:00")
                    )
                return row
    except Exception as e:
        print(f"  Warning: could not fetch dataset metadata: {e}")
    return None


def update_dataset_meta(
    url: str,
    key: str,
    dataset_code: str,
    record_count: int | None = None,
    status: str | None = None,
    last_error: str | None = None,
) -> bool:
    """
    Update dataset metadata after a pipeline run.
    Always sets last_acquired_at = now(). Optionally updates record_count and status.
    Returns True on success.
    """
    if not url or not key:
        return False

    endpoint = (
        f"{url.rstrip('/')}/rest/v1/datasets"
        f"?code=eq.{dataset_code}"
    )
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    payload: dict = {
        "last_acquired_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if record_count is not None:
        payload["record_count"] = record_count
    if status is not None:
        payload["status"] = status
    if last_error is not None:
        payload["last_error"] = last_error

    try:
        r = requests.patch(endpoint, headers=headers, json=payload, timeout=15)
        if r.status_code in (200, 204):
            print(f"  Updated dataset metadata for '{dataset_code}'")
            return True
        else:
            print(f"  Warning: could not update dataset metadata: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"  Warning: could not update dataset metadata: {e}")
    return False
