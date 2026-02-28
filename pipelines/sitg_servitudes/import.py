#!/usr/bin/env python3
"""
SITG Servitudes (RFO_TOUTES_SERVITUDES) — Import Pipeline

Fetches land-registry easement/servitude data from the SITG ArcGIS REST API
and upserts into bronze."RFO_TOUTES_SERVITUDES".

This is a clean Python rewrite of LamapParser/parsers/legalConstraints.js.
It preserves the same API endpoint, field mapping, and table structure, with
the addition of WGS84 geometry (polygons).

CHANGES FROM THE JS VERSION:
    - UPSERT on objectid instead of iteration-based full replace.
    - Geometry is fetched and stored (WGS84 / EPSG:4326).
    - Multi-destination support (Lamap required, Yooneet optional).
    - No delete, no truncate. Row count only goes UP or stays the same.

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.
    - Row count should only go UP or stay the same.

Source:  https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/RFO_TOUTES_SERVITUDES/FeatureServer/0
Table:   bronze."RFO_TOUTES_SERVITUDES"
Key:     objectid
Geom:    Polygon (WGS84)

Environment variables:
    LAMAP_SUPABASE_URL          - Lamap Supabase project URL (required)
    LAMAP_SUPABASE_SERVICE_KEY  - service_role key (required)
    LAMAP_SCHEMA                - target schema (default: bronze)
    YOONEET_SUPABASE_URL        - Yooneet Supabase project URL (optional)
    YOONEET_SUPABASE_SERVICE_KEY - Yooneet service_role key (optional)
"""

import os
import sys

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.sitg_arcgis import fetch_all_features

# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

API_URL = (
    "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted"
    "/RFO_TOUTES_SERVITUDES/FeatureServer/0"
)
TABLE_NAME = "RFO_TOUTES_SERVITUDES"
CONFLICT_COLUMN = "objectid"

# Fields that exist in the JS-era table but we no longer use
EXCLUDE_FIELDS = {"iteration"}


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
# Destination config
# ──────────────────────────────────────────────────────────────

def build_destinations() -> list[dict]:
    """Build list of Supabase destinations from env vars."""
    destinations = []

    # Lamap (required)
    lamap_url = os.environ.get("LAMAP_SUPABASE_URL", "")
    lamap_key = os.environ.get("LAMAP_SUPABASE_SERVICE_KEY", "")
    lamap_schema = os.environ.get("LAMAP_SCHEMA", "bronze")

    if lamap_url and lamap_key:
        destinations.append({
            "name": "lamap_db",
            "url": lamap_url,
            "key": lamap_key,
            "schema": lamap_schema,
        })
    else:
        print("ERROR: LAMAP_SUPABASE_URL and LAMAP_SUPABASE_SERVICE_KEY are required")
        sys.exit(1)

    # Yooneet (optional)
    yooneet_url = os.environ.get("YOONEET_SUPABASE_URL", "")
    yooneet_key = os.environ.get("YOONEET_SUPABASE_SERVICE_KEY", "")
    if yooneet_url and yooneet_key:
        destinations.append({
            "name": "yooneet",
            "url": yooneet_url,
            "key": yooneet_key,
            "schema": "bronze",
        })

    return destinations


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    destinations = build_destinations()

    print("=" * 60)
    print("  SITG Servitudes Import Pipeline")
    print(f"  Source: SITG ArcGIS REST API")
    print(f"  Table:  {TABLE_NAME}")
    print(f"  Key:    {CONFLICT_COLUMN}")
    print(f"  Destinations: {', '.join(d['name'] for d in destinations)}")
    print("=" * 60)

    # ── Step 1: Fetch all features from ArcGIS ────────────────
    print("\n  Fetching features from SITG ArcGIS API...")
    records = fetch_all_features(API_URL, include_geometry=True)

    if not records:
        print("  No records fetched. Exiting.")
        return

    # Remove excluded fields (e.g. 'iteration' from JS era)
    for r in records:
        for f in EXCLUDE_FIELDS:
            r.pop(f, None)

    print(f"\n  Total records to upsert: {len(records):,}")

    # ── Step 2: Upsert to each destination ────────────────────
    primary_ok = False  # lamap_db (first dest) must succeed

    for i, dest in enumerate(destinations):
        is_primary = i == 0  # lamap_db is always first

        print(f"\n{'─' * 60}")
        print(f"  Destination: {dest['name']} ({dest['schema']}.{TABLE_NAME})")
        print(f"  {'[REQUIRED]' if is_primary else '[OPTIONAL]'}")
        print(f"{'─' * 60}")

        # Row count BEFORE
        rows_before = get_row_count(dest["url"], dest["key"], dest["schema"], TABLE_NAME)
        print(f"  Rows before: {rows_before or 'unknown'}")

        # Check if geometry column exists
        geom_exists = has_column(dest["url"], dest["key"], dest["schema"], TABLE_NAME, "geometry")

        if geom_exists:
            print("  Geometry column: found, including geometry data")
            upsert_records = records
        else:
            print("  Geometry column: NOT FOUND, stripping geometry from records")
            print(f"  (To add it: ALTER TABLE {dest['schema']}.\"{TABLE_NAME}\" ADD COLUMN geometry text;)")
            upsert_records = [{k: v for k, v in r.items() if k != "geometry"} for r in records]

        # Upsert
        upserted = batch_upsert(
            url=dest["url"],
            key=dest["key"],
            table=TABLE_NAME,
            records=upsert_records,
            conflict_column=CONFLICT_COLUMN,
            schema=dest["schema"],
            batch_size=500,
        )

        # Row count AFTER
        rows_after = get_row_count(dest["url"], dest["key"], dest["schema"], TABLE_NAME)

        print(f"\n  Results for {dest['name']}:")
        print(f"    Upserted:     {upserted:,}")
        print(f"    Rows before:  {rows_before or 'unknown'}")
        print(f"    Rows after:   {rows_after or 'unknown'}")

        if rows_before is not None and rows_after is not None:
            delta = rows_after - rows_before
            print(f"    Net new:      {delta:,}")
            if rows_after < rows_before:
                print("    WARNING: Row count DECREASED! This should never happen.")

        if upserted == 0:
            print(f"    {'ERROR' if is_primary else 'WARNING'}: Zero rows upserted!")
        elif is_primary:
            primary_ok = True

    print("\n" + "=" * 60)
    print("  IMPORT COMPLETE")
    print("=" * 60)

    if not primary_ok:
        print("  FATAL: Primary destination (lamap_db) failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
