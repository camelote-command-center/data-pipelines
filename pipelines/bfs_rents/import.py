#!/usr/bin/env python3
"""
BFS Average Rents — Import Pipeline

Downloads average rent data (Mietpreisstrukturerhebung) from the Swiss
Federal Statistical Office (BFS) via the BFS DAM API and upserts into
bronze.bfs_average_rents on lamap_db.

Source:  BFS DAM API
         Asset 24129085: "Durchschnittlicher Mietpreis in Franken
         nach Zimmerzahl und Kanton" (all 26 cantons + CH total)
         Excel file parsed with openpyxl

Target:  bronze.bfs_average_rents
Conflict: year,canton_code,rooms

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

import io
import os
import sys
import time
import tempfile

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.freshness import update_dataset_meta


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

# BFS DAM API — direct download of the Excel file
# Asset 24129085: Durchschnittlicher Mietpreis nach Zimmerzahl und Kanton
# Covers years 2000, 2003, 2010–2021, all 26 cantons + Switzerland total
BFS_ASSET_URL = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/24129085/master"

TABLE = "bfs_average_rents"
CONFLICT_COLUMN = "year,canton_code,rooms"
BATCH_SIZE = 500
DATASET_CODE = "ch_bfs_average_rents"

# Canton name (German) → ISO canton code
CANTON_MAP = {
    "Schweiz": "CH",
    "Zürich": "ZH",
    "Bern": "BE",
    "Luzern": "LU",
    "Uri": "UR",
    "Schwyz": "SZ",
    "Obwalden": "OW",
    "Nidwalden": "NW",
    "Glarus": "GL",
    "Zug": "ZG",
    "Freiburg": "FR",
    "Solothurn": "SO",
    "Basel-Stadt": "BS",
    "Basel-Landschaft": "BL",
    "Schaffhausen": "SH",
    "Appenzell A.Rh.": "AR",
    "Appenzell I.Rh.": "AI",
    "St.Gallen": "SG",
    "Graubünden": "GR",
    "Aargau": "AG",
    "Thurgau": "TG",
    "Tessin": "TI",
    "Waadt": "VD",
    "Wallis": "VS",
    "Neuenburg": "NE",
    "Genf": "GE",
    "Jura": "JU",
}

# Room count columns in the Excel file (0-indexed from column B):
# Col B(idx 1): Total, Col D(idx 3): 1 room, Col F(idx 5): 2 rooms, ...
# Each pair: (rent_col, confidence_col)
ROOM_COLUMNS = [
    (2, 3, 0),   # Total: col B=rent, col C=CI  → rooms=0
    (4, 5, 1),   # 1 room: col D=rent, col E=CI  → rooms=1
    (6, 7, 2),   # 2 rooms: col F=rent, col G=CI → rooms=2
    (8, 9, 3),   # 3 rooms: col H=rent, col I=CI → rooms=3
    (10, 11, 4), # 4 rooms: col J=rent, col K=CI → rooms=4
    (12, 13, 5), # 5 rooms: col L=rent, col M=CI → rooms=5
    (14, 15, 6), # 6+ rooms: col N=rent, col O=CI → rooms=6
]


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


def download_excel(url: str) -> bytes:
    """Download the BFS Excel file from the DAM API."""
    print(f"  Downloading: {url}")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    content_type = r.headers.get("content-type", "")
    print(f"  Content-Type: {content_type}")
    print(f"  Downloaded {len(r.content):,} bytes")
    return r.content


def safe_int(val) -> int | None:
    """Convert value to int, returning None for non-numeric values like 'X'."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def safe_float(val) -> float | None:
    """Convert value to float, returning None for non-numeric values."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def discover_table_columns(url: str, key: str, schema: str) -> set[str] | None:
    """Discover available columns in the target table via PostgREST.

    Makes a GET request with limit=0 to get column names from the response
    without fetching actual data.
    """
    endpoint = f"{url.rstrip('/')}/rest/v1/{TABLE}?limit=0"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
    }
    if schema and schema != "public":
        headers["Accept-Profile"] = schema
    try:
        r = requests.get(endpoint, headers=headers, timeout=15)
        if r.status_code == 200:
            # PostgREST returns [] for empty tables but with correct columns
            # in the response definition. We need a different approach:
            # Use the OpenAPI definition.
            pass
    except Exception:
        pass

    # Alternative: try a select with a known column to verify connectivity,
    # then we'll filter records by trial.
    return None


def parse_excel_to_records(excel_bytes: bytes) -> list[dict]:
    """Parse the BFS rent Excel file into flat records.

    Each sheet = one year. Each row = one canton.
    Columns alternate between rent values and confidence intervals
    for each room count (Total, 1, 2, 3, 4, 5, 6+).

    Output records have: year, canton_code, rooms, average_rent_chf,
    canton_name, source_file
    """
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(excel_bytes), data_only=True)
    records = []
    skipped = 0

    for sheet_name in wb.sheetnames:
        # Sheet name is the year
        try:
            year = int(sheet_name)
        except ValueError:
            print(f"  Skipping sheet: {sheet_name}")
            continue

        ws = wb[sheet_name]

        # Data rows start at row 6, canton name in column A
        for row_idx in range(6, ws.max_row + 1):
            canton_name = ws.cell(row=row_idx, column=1).value
            if not canton_name or not isinstance(canton_name, str):
                continue

            canton_name = canton_name.strip()
            canton_code = CANTON_MAP.get(canton_name)
            if not canton_code:
                # Not a canton row (footnote, empty, etc.)
                continue

            # Extract rent data for each room count
            for rent_col, ci_col, rooms in ROOM_COLUMNS:
                rent_val = ws.cell(row=row_idx, column=rent_col).value

                avg_rent = safe_float(rent_val)

                if avg_rent is None:
                    # BFS uses 'X' for suppressed data (insufficient sample)
                    skipped += 1
                    continue

                records.append({
                    "year": year,
                    "canton_code": canton_code,
                    "rooms": rooms,
                    "average_rent_chf": avg_rent,
                    "source_file": f"bfs_dam_asset_24129085_{sheet_name}",
                })

    print(f"  Parsed {len(records):,} records from {len(wb.sheetnames)} sheets")
    if skipped:
        print(f"  Skipped {skipped} suppressed values (X)")

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
    print(f"  Source: BFS DAM API (asset 24129085)")
    print("=" * 60)

    # ── Row count BEFORE ──
    rows_before = get_row_count(lamap_url, lamap_key, lamap_schema)
    print(f"\n  Rows before: {rows_before:,}" if rows_before is not None else "\n  Rows before: unknown")

    # ── Download Excel ──
    start = time.time()
    print("\n  Downloading BFS rent data (Excel)...")
    excel_bytes = download_excel(BFS_ASSET_URL)

    # ── Parse ──
    print("\n  Parsing Excel...")
    records = parse_excel_to_records(excel_bytes)

    if not records:
        print("  ERROR: No records parsed from Excel")
        sys.exit(1)

    # ── Discover valid columns via probe ──
    # PostgREST rejects unknown columns, so we probe with one record
    # to find which columns exist in the table.
    print("\n  Probing table columns...")
    probe = records[0].copy()
    all_record_keys = set(probe.keys())
    valid_keys = set()

    endpoint = f"{lamap_url.rstrip('/')}/rest/v1/{TABLE}?on_conflict={CONFLICT_COLUMN}"
    headers = {
        "apikey": lamap_key,
        "Authorization": f"Bearer {lamap_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    if lamap_schema and lamap_schema != "public":
        headers["Content-Profile"] = lamap_schema

    # Try with all columns first
    import json as _json
    r = requests.post(endpoint, headers=headers, json=[probe], timeout=15)
    if r.status_code in (200, 201):
        valid_keys = all_record_keys
        print(f"  All columns accepted: {sorted(valid_keys)}")
    else:
        # Strip columns one by one based on error
        bad_cols = set()
        for attempt in range(5):  # max 5 rounds of stripping
            stripped_probe = {k: v for k, v in probe.items() if k not in bad_cols}
            r = requests.post(endpoint, headers=headers, json=[stripped_probe], timeout=15)
            if r.status_code in (200, 201):
                valid_keys = set(stripped_probe.keys())
                break
            # Parse error to find bad column
            try:
                err = r.json()
                msg = err.get("message", "")
                # "Could not find the 'xxx' column of 'table' in the schema cache"
                if "Could not find the" in msg and "column" in msg:
                    bad_col = msg.split("'")[1]
                    bad_cols.add(bad_col)
                    print(f"  Column '{bad_col}' not in table, removing")
                else:
                    print(f"  Probe error: {msg}")
                    break
            except Exception:
                print(f"  Probe failed: {r.status_code} {r.text[:200]}")
                break

        if not valid_keys:
            # Fallback: use only the conflict columns + average_rent_chf
            valid_keys = {"year", "canton_code", "rooms", "average_rent_chf"}
            print(f"  Falling back to core columns: {sorted(valid_keys)}")
        else:
            print(f"  Valid columns: {sorted(valid_keys)}")

    # Filter records to valid columns only
    if valid_keys != all_record_keys:
        dropped = all_record_keys - valid_keys
        print(f"  Dropping columns not in table: {sorted(dropped)}")
        records = [{k: v for k, v in rec.items() if k in valid_keys} for rec in records]

    # Normalise keys (all records must have same keys for PostgREST)
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
    print(f"  Source:          BFS DAM API asset 24129085")
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
