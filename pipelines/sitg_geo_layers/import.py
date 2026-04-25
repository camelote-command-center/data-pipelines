#!/usr/bin/env python3
"""
SITG Geo Layers — Import Pipeline (config-driven, pure ArcGIS)

Fetches 7 SITG geo-enriched datasets via ArcGIS REST API and upserts into re-llm bronze_ch:
  1. ge_sitg_classement_geo          — Protected buildings      (polygon)
  2. ge_sitg_bruit_routier_geo       — Road noise measurements  (polyline)
  3. ge_sitg_solaire_geo             — Solar potential           (polygon)
  4. ge_sitg_girec_geo               — GIREC sub-sectors         (polygon)
  5. ge_sitg_zones_amenag_geo        — Zoning areas              (polygon)
  6. ge_sitg_rdppf_zones_dev_geo     — Development zones         (polygon)
  7. ge_sitg_rdppf_dsopb_geo         — RDPPF DSOPB               (polygon)

All datasets use the SITG ArcGIS REST API (Hosted FeatureServer).

GEOMETRY HANDLING:
    The shared/sitg_arcgis.py fetcher returns raw ArcGIS geometry JSON.
    All 7 target tables use PostGIS geometry columns (USER-DEFINED type),
    which require GeoJSON format. This pipeline converts:
      - ArcGIS polygon  {"rings": [...]}  -> GeoJSON {"type":"Polygon", ...}
      - ArcGIS polyline {"paths": [...]}  -> GeoJSON {"type":"MultiLineString", ...}

CONFIG-DRIVEN: Adding a new SITG geo table is just adding a dict to DATASETS.

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.
    - Row count should only go UP or stay the same.

Environment variables:
    RE_LLM_SUPABASE_URL              - re-llm Supabase project URL (required)
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY - service_role key (required)
    RE_LLM_SCHEMA                    - target schema (default: bronze_ch)
"""

import json
import os
import sys

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.sitg_arcgis import fetch_all_features
from shared.freshness import get_dataset_meta, update_dataset_meta

# ──────────────────────────────────────────────────────────────
# Dataset configs
# ──────────────────────────────────────────────────────────────

DATASETS = [
    {
        "name": "Batiments proteges",
        "code": "ge_geo_patrimoine_classe",
        "table": "ge_sitg_classement_geo",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/DPS_CLASSEMENT/FeatureServer/0",
        "conflict_column": "objectid",
        "geom_type": "polygon",
        "field_renames": {
            "shape__length": "shape_length",
            "shape__area": "shape_area",
        },
        "batch_size": 500,
    },
    {
        "name": "Bruit routier",
        "code": "ge_rdppf_dsopb",
        "table": "ge_sitg_bruit_routier_geo",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/BRUIT_ROUTIER_MESURE_AUX_FACADES_DES_BATIMENTS/FeatureServer/0",
        "conflict_column": "objectid",
        "geom_type": "polyline",
        "field_renames": {
            "shape_fid": "objectid",
            "cal_lrj": "max_lr_day",
            "cal_lrn": "max_lr_night",
            "shape__length": "shape_length",
        },
        "batch_size": 50,  # Polyline geometry payloads are large
    },
    {
        "name": "Potentiel solaire",
        "code": "ge_ocen_solaire",
        "table": "ge_sitg_solaire_geo",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/OCEN_SOLAIRE_PV_BATIMENT/FeatureServer/0",
        "conflict_column": "objectid",
        "geom_type": "polygon",
        "field_renames": {
            "shape__length": "shape_length",
            "shape__area": "shape_area",
        },
        "batch_size": 200,  # 266K records — moderate batch size
    },
    {
        "name": "Sous-secteurs GIREC",
        "code": "ge_geo_girec",
        "table": "ge_sitg_girec_geo",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/GEO_GIREC/FeatureServer/0",
        "conflict_column": "objectid",
        "geom_type": "polygon",
        "field_renames": {
            "shape__length": "shape_length",
            "shape__area": "shape_area",
        },
        "batch_size": 500,
    },
    {
        "name": "Zones amenagement",
        "code": "ge_geo_zones",
        "table": "ge_sitg_zones_amenag_geo",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/SIT_ZONE_AMENAG/FeatureServer/0",
        "conflict_column": "objectid",
        "geom_type": "polygon",
        "field_renames": {},
        "batch_size": 500,
    },
    {
        "name": "Zones developpement",
        "code": "ge_rdppf_zones_dev",
        "table": "ge_sitg_rdppf_zones_dev_geo",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/RDPPF_ZONES_DEV/FeatureServer/0",
        "conflict_column": "objectid",
        "geom_type": "polygon",
        "field_renames": {
            "shape__length": "shape_length",
            "shape__area": "shape_area",
        },
        "batch_size": 500,
    },
    {
        "name": "RDPPF DSOPB",
        "code": "ge_rdppf_dsopb_planning",
        "table": "ge_sitg_rdppf_dsopb_geo",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/RDPPF_DSOPB/FeatureServer/0",
        "conflict_column": "objectid",
        "geom_type": "polygon",
        "field_renames": {
            "shape__length": "shape_length",
            "shape__area": "shape_area",
        },
        "batch_size": 500,
    },
]

# Fields that exist in the ArcGIS response but we never manage
EXCLUDE_FIELDS = {"iteration", "globalid"}


def get_arcgis_count(url: str) -> int | None:
    """Quick query to get the total feature count from an ArcGIS FeatureServer."""
    try:
        r = requests.get(
            f"{url}/query",
            params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json().get("count")
    except Exception:
        pass
    return None


# ──────────────────────────────────────────────────────────────
# ArcGIS → GeoJSON geometry conversion
# ──────────────────────────────────────────────────────────────

def arcgis_to_geojson(geom_json_str: str, geom_type: str) -> str | None:
    """
    Convert raw ArcGIS geometry JSON string to GeoJSON string.

    ArcGIS polygon:  {"rings": [[...]]}        → {"type":"Polygon", "coordinates":[[...]]}
    ArcGIS polyline: {"paths": [[...]]}        → {"type":"MultiLineString", "coordinates":[[...]]}
    ArcGIS point:    {"x": ..., "y": ...}      → {"type":"Point", "coordinates":[x,y]}

    Returns a JSON string ready for PostGIS, or None if conversion fails.
    """
    if not geom_json_str:
        return None

    try:
        geom = json.loads(geom_json_str)
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(geom, dict):
        return None

    geojson = None

    if geom_type == "polygon" and "rings" in geom:
        geojson = {
            "type": "Polygon",
            "coordinates": geom["rings"],
        }
    elif geom_type == "polyline" and "paths" in geom:
        geojson = {
            "type": "MultiLineString",
            "coordinates": geom["paths"],
        }
    elif "x" in geom and "y" in geom:
        geojson = {
            "type": "Point",
            "coordinates": [geom["x"], geom["y"]],
        }

    if geojson is None:
        return None

    return json.dumps(geojson)


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
        geom_type = ds.get("geom_type", "polygon")
        batch_size = ds.get("batch_size", 500)

        print(f"\n{'━' * 60}")
        print(f"  [{ds['name']}] -> {dest_schema}.{table}")
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

        # Convert ArcGIS geometry to GeoJSON for PostGIS
        geom_converted = 0
        for r in work_records:
            if "geometry" in r and r["geometry"]:
                geojson = arcgis_to_geojson(r["geometry"], geom_type)
                if geojson:
                    r["geometry"] = geojson
                    geom_converted += 1
                else:
                    r["geometry"] = None
        if geom_converted:
            print(f"  Geometry: converted {geom_converted:,} features (ArcGIS -> GeoJSON {geom_type})")

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
            print("  Geometry: included (PostGIS)")
        else:
            work_records = [{k: v for k, v in r.items() if k != "geometry"} for r in work_records]
            print("  Geometry: column not found in table, stripping")

        # Normalise keys: PostgREST requires all objects in a batch to have
        # identical keys.  Some ArcGIS features may lack optional fields.
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
            batch_size=batch_size,
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
    # ── Required: re-llm ──
    rellm_url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    rellm_key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    rellm_schema = os.environ.get("RE_LLM_SCHEMA", "bronze_ch")
    camelote_url = os.environ.get("CAMELOTE_SUPABASE_URL", "")
    camelote_key = os.environ.get("CAMELOTE_SUPABASE_KEY", "")

    if not rellm_url or not rellm_key:
        print("ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  SITG Geo Layers Pipeline")
    print(f"  Datasets: {len(DATASETS)}")
    print("=" * 60)

    # ── Fetch datasets (with freshness pre-check) ──
    datasets_with_records: list[tuple[dict, list[dict]]] = []

    for ds in DATASETS:
        print(f"\n{'━' * 60}")
        print(f"  Fetching: {ds['name']}")
        print(f"  URL:      {ds['url'][:80]}...")
        print(f"{'━' * 60}")

        # Freshness pre-check: compare source count with stored count
        meta = get_dataset_meta(camelote_url, camelote_key, ds["code"])
        if meta and meta.get("record_count"):
            source_count = get_arcgis_count(ds["url"])
            if source_count is not None and source_count == meta["record_count"]:
                print(f"  Source count ({source_count:,}) matches stored count — skipping")
                datasets_with_records.append((ds, []))
                continue
            elif source_count is not None:
                print(f"  Source count: {source_count:,} vs stored: {meta['record_count']:,} — fetching")

        try:
            records = fetch_all_features(ds["url"], include_geometry=True)
        except Exception as e:
            print(f"  FETCH ERROR: {e}")
            records = []

        datasets_with_records.append((ds, records))

    # ── Upsert to re-llm (required) ──
    rellm_ok = process_destination(
        "re-llm", rellm_url, rellm_key, rellm_schema, datasets_with_records
    )

    # ── Update dataset metadata ──
    for ds, records in datasets_with_records:
        if records:
            rows_after = get_row_count(rellm_url, rellm_key, rellm_schema, ds["table"])
            update_dataset_meta(
                camelote_url, camelote_key, ds["code"],
                record_count=rows_after,
                status="active",
            )

    # ── Final status ──
    print("\n" + "=" * 60)
    print("  IMPORT COMPLETE")
    print("=" * 60)

    if not rellm_ok:
        print("  FAILED: re-llm had errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
