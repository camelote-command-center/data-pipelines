#!/usr/bin/env python3
"""
BFS Population by Commune — Import Pipeline

Downloads commune-level population data from the Swiss Federal Statistical
Office (BFS) via opendata.swiss and upserts into bronze.bfs_population
on lamap_db.

Source:  opendata.swiss / BFS
         Dataset: Commune-level population (Wohnbevoelkerung nach Gemeinde)
         CSV download via opendata.swiss CKAN API

Target:  bronze.bfs_population
Conflict: year,bfs_commune_number

DATA SAFETY:
    - UPSERT only. Never truncates or deletes.
    - Row count should only go UP or stay the same.

Environment variables:
    LAMAP_SUPABASE_URL          - Lamap Supabase project URL (required)
    LAMAP_SUPABASE_SERVICE_KEY  - service_role key (required)
    LAMAP_SCHEMA                - target schema (default: bronze)
    CAMELOTE_SUPABASE_URL       - Command center URL (optional, for metadata)
    CAMELOTE_SUPABASE_KEY       - Command center key (optional)
"""

import csv
import io
import os
import sys
import time

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.freshness import get_dataset_meta, update_dataset_meta


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

# Direct BFS ASSET URLs (PRIMARY) — BFS population by commune
# Asset IDs change over time; we try several from newest to oldest.
BFS_ASSET_IDS = [
    "36451447",  # 2025 provisional population by commune
    "34447410",  # 2024 provisional population by commune
    "32007762",  # older asset (may be gone)
]
BFS_ASSET_URL_TEMPLATE = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/{}/master"

# opendata.swiss dataset search (FALLBACK) — searches for BFS population data
OPENDATA_SEARCH_URL = (
    "https://opendata.swiss/api/3/action/package_search"
    "?q=bevoelkerung+gemeinde+bfs&rows=10"
)

# Reject URLs from cantonal portals (they return partial/unrelated data)
REJECTED_DOMAINS = {"data.zg.ch", "data.bs.ch", "data.bl.ch", "data.be.ch", "data.zh.ch"}

TABLE = "bfs_population"
CONFLICT_COLUMN = "year,bfs_commune_number"
BATCH_SIZE = 500
DATASET_CODE = "ch_bfs_population"

# Column mapping: CSV header variations → table columns
# BFS CSVs may use German, French, or English headers.
COLUMN_MAP = {
    # Year
    "jahr": "year",
    "annee": "year",
    "year": "year",
    "stichtag": "year",
    # BFS commune number
    "gemeinde_nummer": "bfs_commune_number",
    "gemeindenummer": "bfs_commune_number",
    "gemeinde-nr.": "bfs_commune_number",
    "bfs_nr": "bfs_commune_number",
    "bfs_commune_number": "bfs_commune_number",
    "no_commune": "bfs_commune_number",
    "commune_id": "bfs_commune_number",
    # Commune name
    "gemeindename": "commune_name",
    "gemeinde": "commune_name",
    "commune": "commune_name",
    "commune_name": "commune_name",
    "nom_commune": "commune_name",
    # Canton
    "kanton": "canton_code",
    "kantonskuerzel": "canton_code",
    "canton": "canton_code",
    "canton_code": "canton_code",
    "kt": "canton_code",
    # Population
    "bevoelkerung": "total_population",
    "total": "total_population",
    "total_population": "total_population",
    "einwohner": "total_population",
    "population_totale": "total_population",
    "wohnbevoelkerung": "total_population",
    # Swiss nationals
    "schweizer": "swiss_nationals",
    "swiss_nationals": "swiss_nationals",
    "schweizer_innen": "swiss_nationals",
    "suisses": "swiss_nationals",
    "ch": "swiss_nationals",
    # Foreign nationals
    "auslaender": "foreign_nationals",
    "auslaender_innen": "foreign_nationals",
    "foreign_nationals": "foreign_nationals",
    "etrangers": "foreign_nationals",
    "ausl": "foreign_nationals",
}


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _resource_name_str(name) -> str:
    """Extract a plain string from a resource name that may be a multilingual dict."""
    if isinstance(name, dict):
        return name.get("de") or name.get("en") or name.get("fr") or next(iter(name.values()), "")
    if isinstance(name, str):
        return name
    return ""


def get_row_count(url: str, key: str, schema: str) -> int | None:
    """Get current row count via PostgREST HEAD request."""
    endpoint = f"{url.rstrip('/')}/rest/v1/{TABLE}?select=count"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Prefer": "count=exact",
    }
    if schema and schema != "public":
        headers["Accept-Profile"] = schema
    try:
        r = requests.head(endpoint, headers=headers, timeout=30)
        cr = r.headers.get("content-range", "")
        if "/" in cr:
            return int(cr.split("/")[1])
    except Exception as e:
        print(f"  Warning: could not get row count: {e}")
    return None


def _is_rejected_url(url: str) -> bool:
    """Reject URLs from cantonal portals that return partial/unrelated data."""
    from urllib.parse import urlparse
    domain = urlparse(url).hostname or ""
    return domain in REJECTED_DOMAINS


def find_csv_url_opendata() -> str | None:
    """Search opendata.swiss for the BFS population CSV download URL."""
    try:
        r = requests.get(OPENDATA_SEARCH_URL, timeout=30)
        r.raise_for_status()
        data = r.json()
        results = data.get("result", {}).get("results", [])

        for dataset in results:
            org = (dataset.get("organization", {}) or {}).get("name", "")
            for resource in dataset.get("resources", []):
                fmt = (resource.get("format") or "").lower()
                url = resource.get("url", "")
                name = _resource_name_str(resource.get("name")).lower()
                if fmt in ("csv", "text/csv") and (
                    "gemeinde" in name or "commune" in name or "population" in name
                    or "bevoelkerung" in name
                ):
                    if _is_rejected_url(url):
                        print(f"  Skipped (cantonal portal): {url}")
                        continue
                    print(f"  Found CSV: {_resource_name_str(resource.get('name'))} (org: {org})")
                    return url

        # Broader search: any CSV resource (still filtering cantonal portals)
        for dataset in results:
            for resource in dataset.get("resources", []):
                fmt = (resource.get("format") or "").lower()
                url = resource.get("url", "")
                if fmt in ("csv", "text/csv") and not _is_rejected_url(url):
                    print(f"  Found CSV (broad match): {_resource_name_str(resource.get('name'))}")
                    return url

    except Exception as e:
        print(f"  Warning: opendata.swiss search failed: {e}")
    return None


def download_csv(csv_url: str) -> str:
    """Download CSV content as text."""
    print(f"  Downloading: {csv_url}")
    r = requests.get(csv_url, timeout=120)
    r.raise_for_status()
    # Handle BOM
    text = r.content.decode("utf-8-sig")
    print(f"  Downloaded {len(text):,} characters")
    return text


def detect_delimiter(text: str) -> str:
    """Detect CSV delimiter (semicolon or comma)."""
    first_line = text.split("\n")[0]
    if first_line.count(";") > first_line.count(","):
        return ";"
    return ","


def parse_csv_to_records(text: str) -> list[dict]:
    """Parse CSV text into table records using column mapping."""
    delimiter = detect_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

    # Map headers
    header_map = {}
    if reader.fieldnames:
        for h in reader.fieldnames:
            key = h.strip().lower().replace(" ", "_").replace("-", "_")
            if key in COLUMN_MAP:
                header_map[h] = COLUMN_MAP[key]

    if not header_map:
        print(f"  WARNING: Could not map any CSV headers")
        print(f"  Available headers: {reader.fieldnames}")
        return []

    print(f"  Mapped columns: {header_map}")

    records = []
    for row in reader:
        record = {}
        for csv_col, table_col in header_map.items():
            val = row.get(csv_col, "").strip()
            record[table_col] = val if val else None

        # Validate required fields
        if not record.get("bfs_commune_number") or not record.get("year"):
            continue

        # Type conversions
        try:
            record["bfs_commune_number"] = int(record["bfs_commune_number"])
        except (ValueError, TypeError):
            continue

        # Handle year — might be a date string like "2024-12-31"
        year_val = record.get("year", "")
        if year_val:
            try:
                if "-" in str(year_val):
                    record["year"] = int(str(year_val).split("-")[0])
                else:
                    record["year"] = int(year_val)
            except (ValueError, TypeError):
                continue

        for col in ("total_population", "swiss_nationals", "foreign_nationals"):
            if record.get(col) is not None:
                try:
                    record[col] = int(record[col])
                except (ValueError, TypeError):
                    record[col] = None

        records.append(record)

    return records


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    lamap_url = os.environ.get("LAMAP_SUPABASE_URL", "")
    lamap_key = os.environ.get("LAMAP_SUPABASE_SERVICE_KEY", "")
    lamap_schema = os.environ.get("LAMAP_SCHEMA", "bronze")
    camelote_url = os.environ.get("CAMELOTE_SUPABASE_URL", "")
    camelote_key = os.environ.get("CAMELOTE_SUPABASE_KEY", "")

    if not lamap_url or not lamap_key:
        print("ERROR: LAMAP_SUPABASE_URL and LAMAP_SUPABASE_SERVICE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  BFS Population by Commune Pipeline")
    print(f"  Target: {lamap_schema}.{TABLE}")
    print("=" * 60)

    # ── Row count BEFORE ──
    rows_before = get_row_count(lamap_url, lamap_key, lamap_schema)
    print(f"\n  Rows before: {rows_before:,}" if rows_before is not None else "\n  Rows before: unknown")

    # ── Download CSV ──
    start = time.time()
    text = None
    csv_url = None

    # Strategy 1: BFS DAM API (primary — direct asset download)
    print("\n  Trying BFS DAM API assets...")
    for asset_id in BFS_ASSET_IDS:
        asset_url = BFS_ASSET_URL_TEMPLATE.format(asset_id)
        print(f"  Trying asset {asset_id}...")
        try:
            text = download_csv(asset_url)
            csv_url = asset_url
            print(f"  Success: asset {asset_id}")
            break
        except requests.exceptions.HTTPError:
            print(f"  Asset {asset_id} not available, trying next...")
            continue

    # Strategy 2: opendata.swiss search (fallback)
    if not text:
        print("\n  BFS DAM assets unavailable, searching opendata.swiss...")
        csv_url = find_csv_url_opendata()
        if csv_url:
            try:
                text = download_csv(csv_url)
            except requests.exceptions.HTTPError as e:
                print(f"  Warning: opendata.swiss URL failed ({e})")

    if not text:
        print("  ERROR: Could not download population data from any source")
        sys.exit(1)

    # ── Parse ──
    print("\n  Parsing CSV...")
    records = parse_csv_to_records(text)
    print(f"  Parsed {len(records):,} records")

    if not records:
        print("  ERROR: No records parsed from CSV")
        sys.exit(1)

    # Normalise keys
    all_keys = set()
    for rec in records:
        all_keys |= rec.keys()
    for rec in records:
        for k in all_keys:
            rec.setdefault(k, None)

    # ── Upsert ──
    print(f"\n  Upserting {len(records):,} records...")
    upserted = batch_upsert(
        url=lamap_url,
        key=lamap_key,
        table=TABLE,
        records=records,
        conflict_column=CONFLICT_COLUMN,
        schema=lamap_schema,
        batch_size=BATCH_SIZE,
    )

    elapsed = time.time() - start

    # ── Row count AFTER ──
    rows_after = get_row_count(lamap_url, lamap_key, lamap_schema)

    # ── Summary ──
    print(f"\n{'=' * 60}")
    print("  IMPORT COMPLETE")
    print(f"  Source:          {csv_url}")
    print(f"  Records parsed:  {len(records):,}")
    print(f"  Rows upserted:   {upserted:,}")
    print(f"  Rows before:     {rows_before:,}" if rows_before is not None else "  Rows before:     unknown")
    print(f"  Rows after:      {rows_after:,}" if rows_after is not None else "  Rows after:      unknown")
    if rows_before is not None and rows_after is not None:
        delta = rows_after - rows_before
        print(f"  Net new:         {delta:,}")
    print(f"  Duration:        {elapsed:.1f}s")
    print("=" * 60)

    if upserted == 0:
        print("  FAILED: Zero rows upserted!")
        sys.exit(1)

    # ── Update dataset metadata ──
    update_dataset_meta(
        camelote_url, camelote_key, DATASET_CODE,
        record_count=rows_after,
        status="active",
    )


if __name__ == "__main__":
    main()
