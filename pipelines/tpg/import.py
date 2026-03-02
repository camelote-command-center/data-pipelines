#!/usr/bin/env python3
"""
TPG — Import Pipeline (standalone fetcher)

Fetches 2 TPG datasets and upserts into lamap_db:
  1. ge_tpg_arrets   — Bus/tram stops  (point)   — TPG Opendatasoft API
  2. ge_tpg_lignes   — Transit lines   (polyline) — SITG ArcGIS REST API
     (TPG's own API does not publish ligne data, so we fall back to SITG.)

STANDALONE: Does NOT use shared/sitg_arcgis.py.
All fetching logic is self-contained in this file.

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.
    - Row count should only go UP or stay the same.

Environment variables:
    LAMAP_SUPABASE_URL          - Lamap Supabase project URL (required)
    LAMAP_SUPABASE_SERVICE_KEY  - service_role key (required)
    LAMAP_SCHEMA                - target schema (default: bronze)
"""

import json
import os
import re
import sys
import time

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

TPG_ARRETS_API = "https://opendata.tpg.ch/api/explore/v2.1/catalog/datasets/arrets/records"
SITG_LIGNES_URL = "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/tpg_lignes/FeatureServer/0"

MAX_RETRIES = 3
RETRY_BACKOFF = 2


# ──────────────────────────────────────────────────────────────
# Standalone fetchers
# ──────────────────────────────────────────────────────────────

def fetch_tpg_arrets() -> list[dict]:
    """
    Fetch all TPG arrêts from Opendatasoft v2.1 API.
    Pagination: offset + limit (max 100).
    """
    all_records = []
    offset = 0
    limit = 100

    print("  Fetching from TPG Opendatasoft API...")

    while True:
        params = {"limit": limit, "offset": offset}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(TPG_ARRETS_API, params=params, timeout=30)
                if r.status_code == 200:
                    break
                elif r.status_code >= 500:
                    wait = RETRY_BACKOFF ** attempt
                    print(f"    Server error {r.status_code}, retrying in {wait}s")
                    time.sleep(wait)
                else:
                    print(f"    ERROR {r.status_code}: {r.text[:300]}")
                    return all_records
            except requests.exceptions.RequestException as e:
                wait = RETRY_BACKOFF ** attempt
                print(f"    Request error: {e}, retrying in {wait}s")
                time.sleep(wait)
        else:
            print(f"    FAILED after {MAX_RETRIES} attempts at offset {offset}")
            return all_records

        data = r.json()
        results = data.get("results", [])

        for rec in results:
            row = {
                "arretcodelong": rec.get("arretcodelong"),
                "nomarret": rec.get("nomarret"),
                "commune": rec.get("commune"),
                "pays": rec.get("pays"),
                "codedidoc": rec.get("codedidoc"),
                "actif": rec.get("actif"),
            }

            # Extract coordinates
            coord = rec.get("coordonnees")
            if coord and isinstance(coord, dict):
                row["lon"] = coord.get("lon")
                row["lat"] = coord.get("lat")
                # Build GeoJSON Point for geometry column
                if row["lon"] is not None and row["lat"] is not None:
                    row["geometry"] = json.dumps({
                        "type": "Point",
                        "coordinates": [row["lon"], row["lat"]],
                    })
            else:
                row["lon"] = None
                row["lat"] = None

            all_records.append(row)

        total = data.get("total_count", 0)
        offset += limit

        if offset % 500 == 0 or offset >= total:
            print(f"    Fetched {min(offset, total):,} / {total:,}")

        if offset >= total:
            break

        time.sleep(0.2)

    print(f"  Total arrêts fetched: {len(all_records):,}")
    return all_records


def _key_to_snake_case(key: str) -> str:
    """Convert ArcGIS field name to snake_case (mirrors JS keyToSnakeCase)."""
    s = re.sub(r"[^\w\s]", "_", key)
    s = re.sub(r"\s+", "_", s)
    return s.lower()


def fetch_tpg_lignes() -> list[dict]:
    """
    Fetch all TPG lignes from SITG ArcGIS REST API.
    Standalone implementation (does not use shared/sitg_arcgis.py).
    """
    all_records = []
    page_size = 2000
    offset = 0

    print("  Fetching from SITG ArcGIS API (tpg_lignes)...")

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "outSR": 4326,
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(
                    f"{SITG_LIGNES_URL}/query",
                    params=params,
                    timeout=60,
                )
                if r.status_code == 200:
                    break
                wait = RETRY_BACKOFF ** attempt
                print(f"    HTTP {r.status_code}, retrying in {wait}s")
                time.sleep(wait)
            except requests.exceptions.RequestException as e:
                wait = RETRY_BACKOFF ** attempt
                print(f"    Request error: {e}, retrying in {wait}s")
                time.sleep(wait)
        else:
            print(f"    FAILED after {MAX_RETRIES} attempts at offset {offset}")
            return all_records

        data = r.json()

        if "error" in data:
            print(f"    ArcGIS error: {data['error']}")
            return all_records

        features = data.get("features", [])
        if not features:
            break

        for feat in features:
            attrs = feat.get("attributes", {})
            geom = feat.get("geometry")

            # Convert keys to snake_case
            row = {}
            for k, v in attrs.items():
                snake_key = _key_to_snake_case(k)
                row[snake_key] = v

            # Add geometry as GeoJSON
            if geom:
                row["geometry"] = json.dumps(geom)

            all_records.append(row)

        offset += len(features)
        print(f"    Fetched {offset:,} features...")

        if not data.get("exceededTransferLimit", False):
            break

        time.sleep(0.3)

    print(f"  Total lignes fetched: {len(all_records):,}")
    return all_records


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def get_row_count(url: str, key: str, schema: str, table: str) -> int | None:
    """Get current row count via PostgREST HEAD request."""
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?select=count"
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


def get_table_columns(url: str, key: str, schema: str, table: str) -> set[str]:
    """Discover existing columns in a table via PostgREST."""
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?limit=1"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
    }
    if schema and schema != "public":
        headers["Accept-Profile"] = schema
    try:
        r = requests.get(endpoint, headers=headers, timeout=15)
        if r.status_code == 200:
            rows = r.json()
            if rows:
                return set(rows[0].keys())
    except Exception as e:
        print(f"  Warning: could not discover table columns: {e}")
    return set()


def has_column(url: str, key: str, schema: str, table: str, column: str) -> bool:
    """Check if a column exists in the target table via PostgREST."""
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?select={column}&limit=0"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
    }
    if schema and schema != "public":
        headers["Accept-Profile"] = schema
    try:
        r = requests.get(endpoint, headers=headers, timeout=10)
        return r.status_code == 200
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────
# Process a dataset
# ──────────────────────────────────────────────────────────────

FIELD_RENAMES = {
    "ge_tpg_lignes": {
        "shape__length": "shape_len",
    },
}

EXCLUDE_FIELDS = {"iteration", "globalid"}


def process_dataset(
    name: str,
    table: str,
    records: list[dict],
    conflict_column: str,
    field_renames: dict[str, str],
    dest_url: str,
    dest_key: str,
    dest_schema: str,
) -> bool:
    """Upsert one dataset. Returns True if successful."""
    print(f"\n{'━' * 60}")
    print(f"  [{name}] → {dest_schema}.{table}")
    print(f"  Records fetched: {len(records):,}")
    print(f"{'━' * 60}")

    if not records:
        print("  No records. Skipping.")
        return True

    # Apply field renames
    if field_renames:
        for r in records:
            for old_name, new_name in field_renames.items():
                if old_name in r:
                    r[new_name] = r.pop(old_name)

    # Remove excluded fields
    for r in records:
        for f in EXCLUDE_FIELDS:
            r.pop(f, None)

    # Discover table columns and filter unknown
    known_cols = get_table_columns(dest_url, dest_key, dest_schema, table)
    if known_cols:
        before_keys = set(records[0].keys()) if records else set()
        allowed = known_cols - EXCLUDE_FIELDS
        records = [{k: v for k, v in r.items() if k in allowed} for r in records]
        after_keys = set(records[0].keys()) if records else set()
        dropped = before_keys - after_keys
        if dropped:
            print(f"  Dropped unknown columns: {', '.join(sorted(dropped))}")

    # Check geometry column
    geom_exists = has_column(dest_url, dest_key, dest_schema, table, "geometry")
    if geom_exists and any("geometry" in r for r in records[:1]):
        print("  Geometry: included")
    else:
        records = [{k: v for k, v in r.items() if k != "geometry"} for r in records]
        print("  Geometry: column not found in table, stripping")

    # Normalise keys (PostgREST requires identical keys in batch)
    all_keys = set()
    for r in records:
        all_keys |= r.keys()
    for r in records:
        for k in all_keys:
            r.setdefault(k, None)

    # Row count BEFORE
    rows_before = get_row_count(dest_url, dest_key, dest_schema, table)
    print(f"  Rows before: {rows_before or 'unknown'}")

    # Upsert
    upserted = batch_upsert(
        url=dest_url,
        key=dest_key,
        table=table,
        records=records,
        conflict_column=conflict_column,
        schema=dest_schema,
        batch_size=500,
    )

    # Row count AFTER
    rows_after = get_row_count(dest_url, dest_key, dest_schema, table)

    print(f"\n  Results:")
    print(f"    Upserted:     {upserted:,}")
    print(f"    Rows before:  {rows_before or 'unknown'}")
    print(f"    Rows after:   {rows_after or 'unknown'}")

    if rows_before is not None and rows_after is not None:
        delta = rows_after - rows_before
        print(f"    Net new:      {delta:,}")
        if rows_after < rows_before:
            print("    WARNING: Row count DECREASED!")

    if upserted == 0:
        print("    ERROR: Zero rows upserted!")
        return False

    return True


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    # ── Required: Lamap ──
    lamap_url = os.environ.get("LAMAP_SUPABASE_URL", "")
    lamap_key = os.environ.get("LAMAP_SUPABASE_SERVICE_KEY", "")
    lamap_schema = os.environ.get("LAMAP_SCHEMA", "bronze")

    if not lamap_url or not lamap_key:
        print("ERROR: LAMAP_SUPABASE_URL and LAMAP_SUPABASE_SERVICE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  TPG Pipeline")
    print("  Datasets: 2 (arrets + lignes)")
    print("=" * 60)

    all_ok = True

    # ── 1. TPG Arrêts (from TPG Opendatasoft API) ──
    print(f"\n{'━' * 60}")
    print("  Fetching: TPG Arrêts")
    print(f"  URL: {TPG_ARRETS_API}")
    print(f"{'━' * 60}")

    arrets = fetch_tpg_arrets()

    ok = process_dataset(
        name="TPG Arrêts",
        table="ge_tpg_arrets",
        records=arrets,
        conflict_column="arretcodelong",
        field_renames={},
        dest_url=lamap_url,
        dest_key=lamap_key,
        dest_schema=lamap_schema,
    )
    if not ok:
        all_ok = False

    # ── 2. TPG Lignes (from SITG ArcGIS — TPG API doesn't publish this) ──
    print(f"\n{'━' * 60}")
    print("  Fetching: TPG Lignes")
    print(f"  URL: {SITG_LIGNES_URL}")
    print(f"{'━' * 60}")

    lignes = fetch_tpg_lignes()

    ok = process_dataset(
        name="TPG Lignes",
        table="ge_tpg_lignes",
        records=lignes,
        conflict_column="objectid",
        field_renames=FIELD_RENAMES.get("ge_tpg_lignes", {}),
        dest_url=lamap_url,
        dest_key=lamap_key,
        dest_schema=lamap_schema,
    )
    if not ok:
        all_ok = False

    # ── Final status ──
    print("\n" + "=" * 60)
    print("  IMPORT COMPLETE")
    print("=" * 60)

    if not all_ok:
        print("  FAILED: Some datasets had errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
