#!/usr/bin/env python3
"""
SITG Authorizations — Import Pipeline (config-driven, pure ArcGIS)

Fetches 2 SITG datasets via ArcGIS REST API and upserts into lamap_db (+ optional Yooneet):
  1. SIT_AUTOR_DOSSIER  — Building permit files    (point)
  2. SIT_AUTOR_OBJET    — Building permit objects   (point)

All datasets use the SITG ArcGIS REST API (Hosted FeatureServer).
No CSV downloads, no filesystem operations.

CONFIG-DRIVEN: Adding a new SITG table is just adding a dict to DATASETS.

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.
    - Row count should only go UP or stay the same.

Environment variables:
    LAMAP_SUPABASE_URL          - Lamap Supabase project URL (required)
    LAMAP_SUPABASE_SERVICE_KEY  - service_role key (required)
    LAMAP_SCHEMA                - target schema (default: bronze)
    YOONEET_SUPABASE_URL        - Yooneet Supabase project URL (optional)
    YOONEET_SUPABASE_SERVICE_KEY - service_role key (optional)
    YOONEET_SCHEMA              - target schema (default: bronze)
"""

import os
import sys

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.sitg_arcgis import fetch_all_features

# ──────────────────────────────────────────────────────────────
# Dataset configs
# ──────────────────────────────────────────────────────────────

DATASETS = [
    {
        "name": "Autorisations dossiers",
        "code": "ge_sit_autor_dossier",
        "table": "SIT_AUTOR_DOSSIER",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/sit_autor_dossier/FeatureServer/0",
        "conflict_column": "objectid",
    },
    {
        "name": "Autorisations objets",
        "code": "ge_sit_autor_objet",
        "table": "SIT_AUTOR_OBJET",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/sit_autor_objet/FeatureServer/0",
        "conflict_column": "objectid",
    },
]

# Fields that exist in the JS-era table but we no longer manage
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


def apply_field_renames(records: list[dict], renames: dict[str, str]) -> list[dict]:
    """Rename fields in all records according to the mapping."""
    if not renames:
        return records
    for r in records:
        for old_name, new_name in renames.items():
            if old_name in r:
                r[new_name] = r.pop(old_name)
    return records


def filter_to_known_columns(
    records: list[dict], known_columns: set[str], exclude: set[str]
) -> list[dict]:
    """Keep only columns that exist in the target table, minus excluded."""
    if not known_columns:
        for r in records:
            for f in exclude:
                r.pop(f, None)
        return records

    allowed = known_columns - exclude
    return [{k: v for k, v in r.items() if k in allowed} for r in records]


# ──────────────────────────────────────────────────────────────
# Process a single destination
# ──────────────────────────────────────────────────────────────

def process_destination(
    dest_name: str,
    dest_url: str,
    dest_key: str,
    dest_schema: str,
    datasets_with_records: list[tuple[dict, list[dict]]],
) -> bool:
    """Upsert all datasets into one destination. Returns True if all succeeded."""
    print(f"\n{'=' * 60}")
    print(f"  Destination: {dest_name} ({dest_schema})")
    print(f"{'=' * 60}")

    all_ok = True

    for ds, records in datasets_with_records:
        table = ds["table"]
        conflict = ds["conflict_column"]
        renames = ds.get("field_renames", {})

        print(f"\n{'━' * 60}")
        print(f"  [{ds['name']}] → {dest_schema}.{table}")
        print(f"  Records fetched: {len(records):,}")
        print(f"{'━' * 60}")

        if not records:
            print("  No records. Skipping.")
            continue

        # Make a copy so renames/filtering don't affect other destinations
        work_records = [dict(r) for r in records]

        # Apply field renames
        work_records = apply_field_renames(work_records, renames)

        # Remove excluded fields
        for r in work_records:
            for f in EXCLUDE_FIELDS:
                r.pop(f, None)

        # Discover table columns and filter out unknown ones
        known_cols = get_table_columns(dest_url, dest_key, dest_schema, table)
        if known_cols:
            before_keys = set()
            for r in work_records[:1]:
                before_keys = set(r.keys())
            work_records = filter_to_known_columns(work_records, known_cols, EXCLUDE_FIELDS)
            after_keys = set()
            for r in work_records[:1]:
                after_keys = set(r.keys())
            dropped = before_keys - after_keys
            if dropped:
                print(f"  Dropped unknown columns: {', '.join(sorted(dropped))}")

        # Check geometry column
        geom_exists = has_column(dest_url, dest_key, dest_schema, table, "geometry")
        if geom_exists and any("geometry" in r for r in work_records[:1]):
            print("  Geometry: included")
        else:
            work_records = [{k: v for k, v in r.items() if k != "geometry"} for r in work_records]
            print("  Geometry: column not found in table, stripping")

        # Normalise keys: PostgREST requires all objects in a batch to have
        # identical keys.  Some ArcGIS features may lack optional fields
        # (e.g. geometry on features with NULL shape).
        all_keys = set()
        for r in work_records:
            all_keys |= r.keys()
        for r in work_records:
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
            records=work_records,
            conflict_column=conflict,
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
            all_ok = False

    return all_ok


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

    # ── Optional: Yooneet ──
    yooneet_url = os.environ.get("YOONEET_SUPABASE_URL", "")
    yooneet_key = os.environ.get("YOONEET_SUPABASE_SERVICE_KEY", "")
    yooneet_schema = os.environ.get("YOONEET_SCHEMA", "bronze")

    print("=" * 60)
    print("  SITG Authorizations Pipeline")
    print(f"  Datasets: {len(DATASETS)}")
    print("=" * 60)

    # ── Fetch all datasets ──
    datasets_with_records: list[tuple[dict, list[dict]]] = []

    for ds in DATASETS:
        print(f"\n{'━' * 60}")
        print(f"  Fetching: {ds['name']}")
        print(f"  URL:      {ds['url'][:80]}...")
        print(f"{'━' * 60}")

        try:
            records = fetch_all_features(ds["url"], include_geometry=True)
        except Exception as e:
            print(f"  FETCH ERROR: {e}")
            records = []

        datasets_with_records.append((ds, records))

    # ── Upsert to Lamap (required) ──
    lamap_ok = process_destination(
        "lamap_db", lamap_url, lamap_key, lamap_schema, datasets_with_records
    )

    # ── Upsert to Yooneet (optional) ──
    if yooneet_url and yooneet_key:
        yooneet_ok = process_destination(
            "yooneet", yooneet_url, yooneet_key, yooneet_schema, datasets_with_records
        )
        if not yooneet_ok:
            print("\n  WARNING: Yooneet had failures (optional destination)")
    else:
        print("\n  Yooneet: not configured, skipping")

    # ── Final status ──
    print("\n" + "=" * 60)
    print("  IMPORT COMPLETE")
    print("=" * 60)

    if not lamap_ok:
        print("  FAILED: Lamap had errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
