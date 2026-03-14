#!/usr/bin/env python3
"""
SITG Urban Programs & Construction Potential — Import Pipeline

Fetches 3 SITG datasets from ArcGIS REST API and upserts into lamap_db:
  1. SIT_PROG_DENS          — Villas-district densification programs  (polygons)
  2. OLS_LOGEMENT_SUBV       — Subsidized housing                     (polygons)
  3. SIT_SURELEVATION_BATIMENT — Buildings that can be raised          (polygons)

This is a clean Python rewrite of
LamapParser/parsers/urbanProgramsConstructionPotential.js.
It preserves the same API endpoints, field mappings, and table structures,
with the addition of WGS84 geometry.

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.
    - Row count should only go UP or stay the same.

Environment variables:
    LAMAP_SUPABASE_URL          - Lamap Supabase project URL (required)
    LAMAP_SUPABASE_SERVICE_KEY  - service_role key (required)
    LAMAP_SCHEMA                - target schema (default: bronze)
"""

import os
import sys

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.sitg_arcgis import fetch_all_features

# ──────────────────────────────────────────────────────────────
# Dataset configs — matches the JS urls + DATABASE_NAMES exactly
# ──────────────────────────────────────────────────────────────

DATASETS = [
    {
        "key": "villas_district_densification_program",
        "table": "SIT_PROG_DENS",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/sit_prog_dens/FeatureServer/0",
        "conflict_column": "objectid",
    },
    {
        "key": "subsidized_housing",
        "table": "OLS_LOGEMENT_SUBV",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/ols_logement_subv/FeatureServer/0",
        "conflict_column": "objectid",
    },
    {
        "key": "buildings_that_can_be_raised",
        "table": "SIT_SURELEVATION_BATIMENT",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/sit_surelevation_batiment/FeatureServer/0",
        "conflict_column": "objectid",
    },
]

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
# Main
# ──────────────────────────────────────────────────────────────

def main():
    dest_url = os.environ.get("LAMAP_SUPABASE_URL", "")
    dest_key = os.environ.get("LAMAP_SUPABASE_SERVICE_KEY", "")
    dest_schema = os.environ.get("LAMAP_SCHEMA", "bronze")

    if not dest_url or not dest_key:
        print("ERROR: LAMAP_SUPABASE_URL and LAMAP_SUPABASE_SERVICE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  SITG Urban Programs & Construction Potential Pipeline")
    print(f"  Destination: lamap_db ({dest_schema})")
    print(f"  Datasets: {len(DATASETS)}")
    print("=" * 60)

    any_failure = False

    for ds in DATASETS:
        table = ds["table"]
        conflict = ds["conflict_column"]

        print(f"\n{'━' * 60}")
        print(f"  Dataset: {ds['key']}")
        print(f"  Table:   {dest_schema}.{table}")
        print(f"  API:     .../{table}/FeatureServer/0")
        print(f"{'━' * 60}")

        # ── Fetch from ArcGIS ──
        print(f"\n  Fetching features...")
        try:
            records = fetch_all_features(ds["url"], include_geometry=True)
        except Exception as e:
            print(f"  FETCH ERROR: {e}")
            any_failure = True
            continue

        if not records:
            print("  No records fetched. Skipping.")
            continue

        # Remove excluded fields
        for r in records:
            for f in EXCLUDE_FIELDS:
                r.pop(f, None)

        print(f"  Records to upsert: {len(records):,}")

        # ── Row count BEFORE ──
        rows_before = get_row_count(dest_url, dest_key, dest_schema, table)
        print(f"  Rows before: {rows_before or 'unknown'}")

        # ── Check geometry column ──
        geom_exists = has_column(dest_url, dest_key, dest_schema, table, "geometry")
        if geom_exists:
            print("  Geometry column: found")
            upsert_records = records
        else:
            print("  Geometry column: NOT FOUND, stripping geometry")
            print(f"  (Add it: ALTER TABLE {dest_schema}.\"{table}\" ADD COLUMN geometry text;)")
            upsert_records = [{k: v for k, v in r.items() if k != "geometry"} for r in records]

        # ── Upsert ──
        upserted = batch_upsert(
            url=dest_url,
            key=dest_key,
            table=table,
            records=upsert_records,
            conflict_column=conflict,
            schema=dest_schema,
            batch_size=500,
        )

        # ── Row count AFTER ──
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
            any_failure = True

    print("\n" + "=" * 60)
    print("  IMPORT COMPLETE")
    print("=" * 60)

    if any_failure:
        sys.exit(1)


if __name__ == "__main__":
    main()
