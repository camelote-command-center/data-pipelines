#!/usr/bin/env python3
"""
BFS Average Rents — Import Pipeline

Downloads average rent data (Mietpreisstrukturerhebung) from the Swiss
Federal Statistical Office (BFS) via opendata.swiss and upserts into
bronze.bfs_average_rents on lamap_db.

Source:  opendata.swiss / BFS
         Dataset: Average rents by canton and room count
         CSV download via opendata.swiss CKAN API

Target:  bronze.bfs_average_rents
Conflict: year,canton_code,rooms

NOTE: This table is currently EMPTY. This pipeline will perform the first load.

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
    "?q=mietpreis+miete+kanton+bfs&rows=10"
)

TABLE = "bfs_average_rents"
CONFLICT_COLUMN = "year,canton_code,rooms"
BATCH_SIZE = 500
DATASET_CODE = "ch_bfs_average_rents"

# Column mapping
COLUMN_MAP = {
    # Year
    "jahr": "year",
    "annee": "year",
    "year": "year",
    "stichtag": "year",
    "periode": "year",
    # Canton code
    "kanton": "canton_code",
    "kantonskuerzel": "canton_code",
    "canton": "canton_code",
    "canton_code": "canton_code",
    "kt": "canton_code",
    "kanton_kuerzel": "canton_code",
    # Canton name
    "kantonsname": "canton_name",
    "kanton_name": "canton_name",
    "canton_name": "canton_name",
    "nom_canton": "canton_name",
    # Rooms
    "zimmeranzahl": "rooms",
    "zimmer": "rooms",
    "rooms": "rooms",
    "nombre_pieces": "rooms",
    "anzahl_zimmer": "rooms",
    "wohnungsgroesse": "rooms",
    "pieces": "rooms",
    # Average rent
    "durchschnittsmiete": "average_rent_chf",
    "miete": "average_rent_chf",
    "average_rent_chf": "average_rent_chf",
    "mittlerer_mietpreis": "average_rent_chf",
    "mietpreis": "average_rent_chf",
    "loyer_moyen": "average_rent_chf",
    "nettomiete": "average_rent_chf",
    "bruttomiete": "average_rent_chf",
    # Rent per m2
    "miete_pro_m2": "rent_per_m2_chf",
    "rent_per_m2_chf": "rent_per_m2_chf",
    "mietpreis_m2": "rent_per_m2_chf",
    "preis_pro_m2": "rent_per_m2_chf",
    "loyer_m2": "rent_per_m2_chf",
}


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

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


def find_csv_url_opendata() -> str | None:
    """Search opendata.swiss for the BFS rent data CSV download URL."""
    try:
        r = requests.get(OPENDATA_SEARCH_URL, timeout=30)
        r.raise_for_status()
        data = r.json()
        results = data.get("result", {}).get("results", [])

        for dataset in results:
            for resource in dataset.get("resources", []):
                fmt = (resource.get("format") or "").lower()
                if fmt in ("csv", "text/csv"):
                    print(f"  Found CSV: {resource.get('name')}")
                    return resource.get("url")
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


def parse_csv_to_records(text: str, source_file: str) -> list[dict]:
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

        # Rooms: might be text like "1 pièce", "2 pièces", etc.
        rooms_val = record.get("rooms")
        if rooms_val is not None:
            try:
                # Extract first digit(s) from string
                digits = "".join(c for c in str(rooms_val) if c.isdigit())
                record["rooms"] = int(digits) if digits else None
            except (ValueError, TypeError):
                record["rooms"] = None

        if record.get("rooms") is None:
            continue  # rooms is part of the conflict key

        for col in ("average_rent_chf", "rent_per_m2_chf"):
            if record.get(col) is not None:
                try:
                    record[col] = float(record[col])
                except (ValueError, TypeError):
                    record[col] = None

        record["source_file"] = source_file
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
    print("  BFS Average Rents Pipeline")
    print(f"  Target: {lamap_schema}.{TABLE}")
    print("  NOTE: First load — table is currently empty")
    print("=" * 60)

    # ── Row count BEFORE ──
    rows_before = get_row_count(lamap_url, lamap_key, lamap_schema)
    print(f"\n  Rows before: {rows_before:,}" if rows_before is not None else "\n  Rows before: unknown")

    # ── Find CSV URL ──
    print("\n  Searching opendata.swiss for BFS rent data...")
    csv_url = find_csv_url_opendata()
    if not csv_url:
        print("  ERROR: Could not find rent data on opendata.swiss")
        print("  Try setting a direct URL for the BFS Mietpreisstrukturerhebung CSV")
        sys.exit(1)

    # ── Download & Parse ──
    start = time.time()
    text = download_csv(csv_url)

    # Use the CSV URL as source_file identifier
    source_file = csv_url.split("/")[-1] if "/" in csv_url else csv_url

    print("\n  Parsing CSV...")
    records = parse_csv_to_records(text, source_file)
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
