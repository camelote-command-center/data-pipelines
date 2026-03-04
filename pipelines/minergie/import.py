#!/usr/bin/env python3
"""
Minergie — Import Pipeline

Fetches Minergie certified buildings in Switzerland from the geo.admin.ch
REST API and upserts into bronze."minergie" on lamap_db.

Source:  geo.admin.ch MapServer identify endpoint
         Layer: ch.bfe.minergiegebaeude
         ~32K-40K buildings with point geometry

Strategy:
    1. Query the identify endpoint with a Switzerland-wide bounding box
    2. Request WGS84 output (sr=4326) with GeoJSON geometry
    3. Paginate with limit=200, incrementing offset
    4. UPSERT on feature_id (unique Minergie building identifier)

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.
    - Row count should only go UP or stay the same.

Environment variables:
    LAMAP_SUPABASE_URL          - Lamap Supabase project URL (required)
    LAMAP_SUPABASE_SERVICE_KEY  - service_role key (required)
    LAMAP_SCHEMA                - target schema (default: bronze)
    CAMELOTE_SUPABASE_URL       - Camelote command-center URL (optional)
    CAMELOTE_SUPABASE_KEY       - Camelote service_role key (optional)
"""

import json
import os
import sys
import time

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.freshness import get_dataset_meta, update_dataset_meta


# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------

IDENTIFY_URL = "https://api3.geo.admin.ch/rest/services/api/MapServer/identify"
LAYER = "ch.bfe.minergiegebaeude"
TABLE = "minergie"
CONFLICT_COLUMN = "feature_id"
BATCH_SIZE = 500
PAGE_SIZE = 200
DATASET_CODE = "ext_minergie"

# Switzerland bounding box in LV03 (EPSG:21781)
# Used for the spatial query; output is in WGS84
BBOX_LV03 = "485000,74000,834000,296000"


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

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


def table_exists(url: str, key: str, schema: str, table: str) -> bool:
    """Check if a table exists via PostgREST."""
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?limit=0"
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


# ------------------------------------------------------------------
# Data fetching
# ------------------------------------------------------------------

def fetch_all_minergie() -> list[dict]:
    """
    Fetch all Minergie buildings from geo.admin.ch identify endpoint.
    Returns list of dicts ready for upsert.
    """
    all_records = []
    offset = 0

    while True:
        params = {
            "geometry": BBOX_LV03,
            "geometryType": "esriGeometryEnvelope",
            "layers": f"all:{LAYER}",
            "mapExtent": BBOX_LV03,
            "imageDisplay": "1,1,96",
            "tolerance": 0,
            "sr": "21781",           # Input SR (bounding box)
            "returnGeometry": "true",
            "limit": PAGE_SIZE,
            "offset": offset,
            "geometryFormat": "geojson",
        }

        for attempt in range(1, 4):
            try:
                r = requests.get(IDENTIFY_URL, params=params, timeout=120)
                r.raise_for_status()
                data = r.json()
                break
            except Exception as e:
                wait = 10 * attempt
                print(f"    Request error ({e}), retrying in {wait}s...")
                time.sleep(wait)
                data = {}

        results = data.get("results", [])
        if not results:
            break

        for feat in results:
            record = transform_feature(feat)
            if record:
                all_records.append(record)

        if offset % 5000 == 0 or len(results) < PAGE_SIZE:
            print(f"  Offset {offset}: {len(all_records):,} records fetched")

        if len(results) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

        # Small delay between pages
        time.sleep(0.3)

    return all_records


def transform_feature(feat: dict) -> dict | None:
    """Transform a geo.admin.ch feature to a database record."""
    feature_id = feat.get("featureId")
    if feature_id is None:
        return None

    # Properties are under 'properties' key (GeoJSON format)
    props = feat.get("properties", {}) or feat.get("attributes", {}) or {}

    # Geometry
    geom = feat.get("geometry")
    geometry_json = None
    if geom:
        # The geometry from the API is in LV03 coordinates when sr=21781
        # We need to convert to WGS84 for storage
        # Since we can't easily convert LV03 to WGS84 in pure Python,
        # we'll re-request individual features with sr=4326 in a batch
        # OR we can use the approximate Swiss conversion formula
        coords = geom.get("coordinates", [])
        if coords and len(coords) >= 2:
            # Convert LV03 to WGS84
            lon, lat = lv03_to_wgs84(coords[0], coords[1])
            geometry_json = json.dumps({
                "type": "Point",
                "coordinates": [round(lon, 6), round(lat, 6)],
            })

    record = {
        "feature_id": feature_id,
        "certificate": props.get("certificate"),
        "standard": props.get("standard"),
        "canton": props.get("canton"),
        "ebf": props.get("ebf"),
        "buildinginfo_de": props.get("buildinginfo_de"),
        "buildinginfo_fr": props.get("buildinginfo_fr"),
        "buildinginfo_it": props.get("buildinginfo_it"),
        "buildinginfo_en": props.get("buildinginfo_en"),
        "http_de": props.get("http_de"),
        "http_fr": props.get("http_fr"),
        "http_it": props.get("http_it"),
        "http_en": props.get("http_en"),
        "label": props.get("label"),
    }

    if geometry_json:
        record["geometry"] = geometry_json

    return record


def lv03_to_wgs84(y: float, x: float) -> tuple[float, float]:
    """
    Convert Swiss LV03 (EPSG:21781) coordinates to WGS84.

    Uses the approximate formulas from swisstopo.
    Input:  y (easting), x (northing) in LV03
    Output: (longitude, latitude) in WGS84

    Reference: https://www.swisstopo.admin.ch/en/maps-data-online/calculation-services/navref.html
    """
    # Auxiliary values (differences from Bern in 1000 km)
    y_aux = (y - 600000) / 1000000
    x_aux = (x - 200000) / 1000000

    # Longitude
    lon = (
        2.6779094
        + 4.728982 * y_aux
        + 0.791484 * y_aux * x_aux
        + 0.1306 * y_aux * x_aux ** 2
        - 0.0436 * y_aux ** 3
    )

    # Latitude
    lat = (
        16.9023892
        + 3.238272 * x_aux
        - 0.270978 * y_aux ** 2
        - 0.002528 * x_aux ** 2
        - 0.0447 * y_aux ** 2 * x_aux
        - 0.0140 * x_aux ** 3
    )

    # Convert to degrees (from 10000" units)
    lon = lon * 100 / 36
    lat = lat * 100 / 36

    return lon, lat


# ------------------------------------------------------------------
# Table creation
# ------------------------------------------------------------------

def create_minergie_table(url: str, key: str, schema: str) -> bool:
    """
    Create the minergie table if it doesn't exist.
    Uses PostgREST RPC or direct SQL via Supabase management API.
    Since we can't run DDL via PostgREST, we'll rely on the table
    being pre-created. This function logs a warning if it's missing.
    """
    print("  WARNING: bronze.minergie table does not exist!")
    print("  Please create it with the following SQL:")
    print(f"""
    CREATE TABLE IF NOT EXISTS {schema}."minergie" (
        id BIGSERIAL PRIMARY KEY,
        feature_id INTEGER UNIQUE NOT NULL,
        certificate TEXT,
        standard TEXT,
        canton TEXT,
        ebf INTEGER,
        buildinginfo_de TEXT,
        buildinginfo_fr TEXT,
        buildinginfo_it TEXT,
        buildinginfo_en TEXT,
        http_de TEXT,
        http_fr TEXT,
        http_it TEXT,
        http_en TEXT,
        label INTEGER,
        geometry JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    """)
    return False


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

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
    print("  Minergie (Certified Buildings) Pipeline")
    print(f"  Source: geo.admin.ch — {LAYER}")
    print(f"  Target: {lamap_schema}.{TABLE}")
    print("=" * 60)

    # -- Previous metadata --
    meta = get_dataset_meta(camelote_url, camelote_key, DATASET_CODE)
    if meta and meta.get("last_acquired_at"):
        print(f"\n  Last acquired: {meta['last_acquired_at'].isoformat()}")
        print(f"  Previous record count: {meta.get('record_count', 'unknown')}")

    # -- Check table exists --
    if not table_exists(lamap_url, lamap_key, lamap_schema, TABLE):
        create_minergie_table(lamap_url, lamap_key, lamap_schema)
        sys.exit(1)

    # -- Row count BEFORE --
    rows_before = get_row_count(lamap_url, lamap_key, lamap_schema, TABLE)
    print(
        f"  Rows before: {rows_before:,}" if rows_before is not None
        else "  Rows before: unknown"
    )

    # -- Fetch all Minergie buildings --
    start_time = time.time()
    print(f"\n{'━' * 60}")
    print("  Fetching Minergie buildings from geo.admin.ch...")
    print(f"{'━' * 60}")

    all_records = fetch_all_minergie()

    fetch_elapsed = time.time() - start_time
    print(f"\n  Fetch complete: {len(all_records):,} records in {fetch_elapsed:.0f}s")

    if not all_records:
        print("  ERROR: No records fetched!")
        sys.exit(1)

    # -- Discover table columns and filter --
    table_cols = get_table_columns(lamap_url, lamap_key, lamap_schema, TABLE)
    exclude = {"id", "created_at", "updated_at"}
    if table_cols:
        allowed = table_cols - exclude
        filtered = []
        dropped = set()
        for rec in all_records:
            f = {}
            for k, v in rec.items():
                if k in allowed:
                    f[k] = v
                else:
                    dropped.add(k)
            filtered.append(f)
        if dropped:
            print(f"  Dropped unknown columns: {', '.join(sorted(dropped))}")
        all_records = filtered

    # -- Deduplicate by feature_id --
    seen = {}
    for rec in all_records:
        fid = rec.get("feature_id")
        if fid is not None:
            seen[fid] = rec  # last wins
    if len(seen) < len(all_records):
        print(f"  Deduplicated: {len(all_records):,} → {len(seen):,} unique feature_ids")
    all_records = list(seen.values())

    # -- Normalise keys --
    all_keys = set()
    for r in all_records:
        all_keys |= r.keys()
    for r in all_records:
        for k in all_keys:
            r.setdefault(k, None)

    # -- Upsert --
    print(f"\n  Upserting {len(all_records):,} records (batch size {BATCH_SIZE})...")

    total_upserted = batch_upsert(
        url=lamap_url,
        key=lamap_key,
        table=TABLE,
        records=all_records,
        conflict_column=CONFLICT_COLUMN,
        schema=lamap_schema,
        batch_size=BATCH_SIZE,
    )

    # -- Row count AFTER --
    rows_after = get_row_count(lamap_url, lamap_key, lamap_schema, TABLE)

    # -- Summary --
    elapsed = time.time() - start_time

    print(f"\n{'=' * 60}")
    print("  IMPORT COMPLETE")
    print(f"  Records fetched:  {len(all_records):,}")
    print(f"  Rows upserted:    {total_upserted:,}")
    print(
        f"  Rows before:      {rows_before:,}" if rows_before is not None
        else "  Rows before:      unknown"
    )
    print(
        f"  Rows after:       {rows_after:,}" if rows_after is not None
        else "  Rows after:       unknown"
    )
    if rows_before is not None and rows_after is not None:
        delta = rows_after - rows_before
        print(f"  Net new:          {delta:,}")
        if rows_after < rows_before:
            print("  WARNING: Row count DECREASED!")
    print(f"  Duration:         {elapsed / 60:.1f} min")
    print("=" * 60)

    if total_upserted == 0:
        print("  FAILED: Zero rows upserted!")
        sys.exit(1)

    # -- Update dataset metadata --
    update_dataset_meta(
        camelote_url, camelote_key, DATASET_CODE,
        record_count=rows_after,
        status="active",
    )


if __name__ == "__main__":
    main()
