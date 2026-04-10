#!/usr/bin/env python3
"""
BFS Vacancy Rates — Import Pipeline

Downloads vacancy rate data (Leerwohnungszaehlung) from the Swiss Federal
Statistical Office (BFS) via opendata.swiss and upserts into
bronze.bfs_vacancy_rates on lamap_db.

Source:  opendata.swiss / BFS
         Dataset: Leerwohnungszaehlung (Empty dwelling count)
         CSV download via opendata.swiss CKAN API

Target:  bronze.bfs_vacancy_rates
Conflict: year,canton_code

Currently ~48 rows at canton level. BFS publishes annually (usually September).

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

OPENDATA_SEARCH_URL = (
    "https://opendata.swiss/api/3/action/package_search"
    "?q=leerwohnungsziffer+leerwohnung+bfs&rows=10"
)

TABLE = "bfs_vacancy_rates"
CONFLICT_COLUMN = "year,canton_code"
BATCH_SIZE = 500
DATASET_CODE = "ch_bfs_vacancy_rates"

# Column mapping
COLUMN_MAP = {
    # Year
    "jahr": "year",
    "annee": "year",
    "year": "year",
    "stichtag": "year",
    # Canton
    "kanton": "canton_code",
    "kantonskuerzel": "canton_code",
    "canton": "canton_code",
    "canton_code": "canton_code",
    "kt": "canton_code",
    "kanton_kuerzel": "canton_code",
    # Commune (for commune-level data if available)
    "gemeinde_nummer": "bfs_commune_number",
    "gemeindenummer": "bfs_commune_number",
    "bfs_nr": "bfs_commune_number",
    "bfs_commune_number": "bfs_commune_number",
    "no_commune": "bfs_commune_number",
    # Commune name
    "gemeindename": "commune_name",
    "gemeinde": "commune_name",
    "commune": "commune_name",
    "commune_name": "commune_name",
    # Dwelling counts
    "wohnungen_total": "total_dwellings",
    "total_dwellings": "total_dwellings",
    "gesamtzahl": "total_dwellings",
    "wohnungsbestand": "total_dwellings",
    "total_logements": "total_dwellings",
    # Vacant
    "leerwohnungen": "vacant_dwellings",
    "leer": "vacant_dwellings",
    "vacant_dwellings": "vacant_dwellings",
    "leerstehend": "vacant_dwellings",
    "logements_vacants": "vacant_dwellings",
    # Vacancy rate
    "leerwohnungsziffer": "vacancy_rate_pct",
    "vacancy_rate_pct": "vacancy_rate_pct",
    "quote": "vacancy_rate_pct",
    "taux_vacance": "vacancy_rate_pct",
    "leerwohnungsquote": "vacancy_rate_pct",
}


# Expected columns in a valid BFS vacancy CSV (at least some of these must be present)
EXPECTED_VACANCY_HEADERS = {
    "leerwohnungsziffer", "leerwohnungsquote", "vacancy_rate_pct",
    "taux_vacance", "quote", "leerwohnungen", "logements_vacants",
    "vacant_dwellings", "leerstehend",
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


def _validate_vacancy_csv(csv_url: str) -> bool:
    """Download the first few KB of a CSV and check it has vacancy-related columns."""
    try:
        r = requests.get(csv_url, timeout=30, stream=True)
        r.raise_for_status()
        # Read just the first 4KB to check headers
        chunk = next(r.iter_content(4096, decode_unicode=True), "")
        r.close()
        if not chunk:
            return False
        first_line = chunk.split("\n")[0].lower()
        # Normalise for matching
        normalised = first_line.replace(" ", "_").replace("-", "_").replace('"', '')
        return any(h in normalised for h in EXPECTED_VACANCY_HEADERS)
    except Exception as e:
        print(f"  Warning: could not validate CSV at {csv_url}: {e}")
        return False


def find_csv_url_opendata() -> str | None:
    """Search opendata.swiss for the BFS vacancy CSV download URL."""
    try:
        r = requests.get(OPENDATA_SEARCH_URL, timeout=30)
        r.raise_for_status()
        data = r.json()
        results = data.get("result", {}).get("results", [])

        csv_candidates = []
        for dataset in results:
            dataset_title = _resource_name_str(dataset.get("title")).lower() if isinstance(dataset.get("title"), dict) else (dataset.get("title") or "").lower()
            for resource in dataset.get("resources", []):
                fmt = (resource.get("format") or "").lower()
                if fmt in ("csv", "text/csv"):
                    name = _resource_name_str(resource.get("name")).lower()
                    url = resource.get("url", "")
                    csv_candidates.append((name, dataset_title, url))

        # Try each candidate, validating it has vacancy columns
        for name, dataset_title, url in csv_candidates:
            print(f"  Checking CSV: {name} (dataset: {dataset_title})")
            if _validate_vacancy_csv(url):
                print(f"  Validated CSV with vacancy columns: {name}")
                return url
            else:
                print(f"  Skipped (no vacancy columns): {name}")

    except Exception as e:
        print(f"  Warning: opendata.swiss search failed: {e}")
    return None


def download_csv(csv_url: str) -> str:
    print(f"  Downloading: {csv_url}")
    r = requests.get(csv_url, timeout=120)
    r.raise_for_status()
    text = r.content.decode("utf-8-sig")
    print(f"  Downloaded {len(text):,} characters")
    return text


def detect_delimiter(text: str) -> str:
    first_line = text.split("\n")[0]
    if first_line.count(";") > first_line.count(","):
        return ";"
    return ","


def parse_csv_to_records(text: str) -> list[dict]:
    delimiter = detect_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

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

        if not record.get("canton_code") or not record.get("year"):
            continue

        # Type conversions
        year_val = record.get("year", "")
        try:
            if "-" in str(year_val):
                record["year"] = int(str(year_val).split("-")[0])
            else:
                record["year"] = int(year_val)
        except (ValueError, TypeError):
            continue

        for col in ("total_dwellings", "vacant_dwellings", "bfs_commune_number"):
            if record.get(col) is not None:
                try:
                    record[col] = int(record[col])
                except (ValueError, TypeError):
                    record[col] = None

        if record.get("vacancy_rate_pct") is not None:
            try:
                record["vacancy_rate_pct"] = float(record["vacancy_rate_pct"])
            except (ValueError, TypeError):
                record["vacancy_rate_pct"] = None

        # Add source
        record["source"] = "BFS Leerwohnungszaehlung"
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
    print("  BFS Vacancy Rates Pipeline")
    print(f"  Target: {lamap_schema}.{TABLE}")
    print("=" * 60)

    # ── Row count BEFORE ──
    rows_before = get_row_count(lamap_url, lamap_key, lamap_schema)
    print(f"\n  Rows before: {rows_before:,}" if rows_before is not None else "\n  Rows before: unknown")

    # ── Find CSV URL ──
    print("\n  Searching opendata.swiss for BFS vacancy data...")
    csv_url = find_csv_url_opendata()
    if not csv_url:
        print("  ERROR: Could not find vacancy data on opendata.swiss")
        print("  Try setting a direct URL in BFS_FALLBACK_URL")
        sys.exit(1)

    # ── Download & Parse ──
    start = time.time()
    text = download_csv(csv_url)

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

    update_dataset_meta(
        camelote_url, camelote_key, DATASET_CODE,
        record_count=rows_after,
        status="active",
    )


if __name__ == "__main__":
    main()
