#!/usr/bin/env python3
"""
SITG Cadastral & Ownership — Import Pipeline (config-driven, pure ArcGIS)

Fetches 7 SITG datasets via ArcGIS REST API and upserts into re-LLM bronze_ch:
  1. ge_cad_parcelles              — Survey parcels             (polygon)
  2. ge_rdppf_synthese             — RDPPF synthesis by EGRID   (polygon)
  3. ge_cad_ddp                    — Permanent separate rights  (polygon)
  4. ge_cad_ppe                    — Co-ownership by floor      (point)
  5. ge_cad_batiments              — Above-ground buildings     (polygon)
  6. ge_cad_batiments_souterrains  — Underground buildings      (polygon)
  7. ge_cad_adresses               — Cadastral addresses        (point)

All datasets use the SITG ArcGIS REST API (Hosted FeatureServer).
No CSV downloads, no filesystem operations.

CONFIG-DRIVEN: Adding a new SITG table is just adding a dict to DATASETS.

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.
    - Row count should only go UP or stay the same.

Environment variables:
    RE_LLM_SUPABASE_URL              - re-LLM Supabase project URL (required)
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY - service_role key (required)
    RE_LLM_SCHEMA                    - target schema (default: bronze_ch)
"""

import os
import sys
from datetime import datetime, timezone

import requests

# Set once per run — used as the soft-delete watermark for `soft_delete=True` datasets.
SYNC_STARTED_AT = datetime.now(timezone.utc).isoformat()

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.sitg_arcgis import fetch_all_features

# ──────────────────────────────────────────────────────────────
# Dataset configs
#
# Adding a new SITG table = adding a dict here.
#   url:              ArcGIS FeatureServer URL
#   table:            Target table name in lamap_db
#   conflict_column:  Column with UNIQUE constraint for upsert
#   field_renames:    Optional dict to rename API fields → table columns
# ──────────────────────────────────────────────────────────────

DATASETS = [
    {
        "name": "Parcelles cadastrales",
        "code": "ge_cad_parcelle_mensu",
        "table": "ge_cad_parcelles",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/cad_parcelle_mensu/FeatureServer/0",
        "conflict_column": "objectid",
        # ArcGIS returns SHAPE__Area/SHAPE__Length → shape__area/shape__length
        # but table columns (from CSV era) are shape_area/shape_len
        "field_renames": {
            "shape__area": "shape_area",
            "shape__length": "shape_len",
        },
    },
    {
        "name": "RDPPF servitudes synthèse",
        "code": "ge_rdppf_synth",
        "table": "ge_rdppf_synthese",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/rdppf_synth_dyn_extract/FeatureServer/0",
        "conflict_column": "egrid",
        # Table already has shape__area / shape__length columns — no rename needed
    },
    {
        "name": "DDP droits distincts",
        "code": "ge_cad_ddp",
        "table": "ge_cad_ddp",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/cad_ddp/FeatureServer/0",
        "conflict_column": "objectid",
        "field_renames": {
            "shape__area": "shape_area",
            "shape__length": "shape_len",
        },
    },
    {
        "name": "PPE copropriété",
        "code": "ge_cad_ppe",
        "table": "ge_cad_ppe",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/cad_ppe/FeatureServer/0",
        "conflict_column": "objectid",
    },
    {
        "name": "Bâtiments hors-sol",
        "code": "ge_cad_batiment_horsol",
        "table": "ge_cad_batiments",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/cad_batiment_horsol/FeatureServer/0",
        "conflict_column": "objectid",
        "field_renames": {
            "shape__area": "shape_area",
            "shape__length": "shape_len",
        },
    },
    {
        "name": "Bâtiments sous-sol",
        "code": "ge_cad_batiment_sousol",
        "table": "ge_cad_batiments_souterrains",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/cad_batiment_sousol/FeatureServer/0",
        "conflict_column": "objectid",
        "field_renames": {
            "shape__area": "shape_area",
            "shape__length": "shape_len",
        },
    },
    {
        "name": "Bâtiments projetés (planned/under-construction)",
        "code": "ge_cad_bati_projet",
        "table": "ge_cad_bati_projet",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/cad_bati_projet/FeatureServer/0",
        "conflict_column": "objectid",
        # Source archives rows after construction → flag deleted_at, never hard-delete.
        # Per SITG: "Après construction et dépôt du dossier de cadastration ces objets sont
        # archivés (voir couche Historique des bâtiments hors-sol et sous-sol)".
        "soft_delete": True,
    },
    {
        "name": "Adresses cadastrales",
        "code": "ge_cad_adresse",
        "table": "ge_cad_adresses",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/cad_adresse/FeatureServer/0",
        "conflict_column": "objectid",
        # ArcGIS returns x/y fields but table has e/n (Swiss easting/northing)
        "field_renames": {
            "x": "e",
            "y": "n",
        },
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


def patch_count(
    url: str,
    key: str,
    schema: str,
    table: str,
    filters: dict[str, str],
    payload: dict,
) -> int:
    """PATCH a table with filters; return count of affected rows.

    `filters` is a dict of {column: postgrest_op_value}, e.g. {"deleted_at": "is.null"}.
    Filter values are URL-encoded (notably `+` in ISO timestamps must become `%2B`).
    Returns 0 on failure (logged).
    """
    from urllib.parse import quote
    qs = "&".join(f"{col}={quote(op, safe='.')}" for col, op in filters.items())
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?{qs}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "count=exact,return=minimal",
    }
    if schema and schema != "public":
        headers["Content-Profile"] = schema
    try:
        r = requests.patch(endpoint, headers=headers, json=payload, timeout=60)
        if r.status_code in (200, 204):
            cr = r.headers.get("content-range", "")
            if "/" in cr:
                return int(cr.split("/")[1])
            return 0
        print(f"  Warning: PATCH returned {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"  Warning: PATCH failed: {e}")
    return 0


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
        # If we couldn't discover columns, just strip excluded fields
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

        # Apply field renames (e.g. shape__area → shape_area)
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
            # Strip geometry if column doesn't exist or records don't have it
            work_records = [{k: v for k, v in r.items() if k != "geometry"} for r in work_records]
            print("  Geometry: column not found in table, stripping")

        # Touch `last_seen_at` on every sync so soft-delete logic can detect
        # which rows are missing from the source (only if column exists).
        if "last_seen_at" in known_cols:
            for r in work_records:
                r["last_seen_at"] = SYNC_STARTED_AT

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

        # Soft-delete reconciliation (opt-in per dataset).
        # For tables with `last_seen_at` + `deleted_at`, after a successful sync:
        #   - Mark rows that were NOT touched (last_seen_at < SYNC_STARTED_AT) as deleted.
        #   - Resurrect any previously-deleted row that came back in this sync.
        if ds.get("soft_delete") and upserted > 0:
            archived = patch_count(
                dest_url, dest_key, dest_schema, table,
                filters={
                    "last_seen_at": f"lt.{SYNC_STARTED_AT}",
                    "deleted_at": "is.null",
                },
                payload={"deleted_at": SYNC_STARTED_AT},
            )
            resurrected = patch_count(
                dest_url, dest_key, dest_schema, table,
                filters={
                    "last_seen_at": f"gte.{SYNC_STARTED_AT}",
                    "deleted_at": "not.is.null",
                },
                payload={"deleted_at": None},
            )
            print(f"    Soft-delete:  {archived} archived, {resurrected} resurrected")

    return all_ok


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    # ── Required: re-LLM ──
    rellm_url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    rellm_key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    rellm_schema = os.environ.get("RE_LLM_SCHEMA", "bronze_ch")

    if not rellm_url or not rellm_key:
        print("ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  SITG Cadastral & Ownership Pipeline")
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

    # ── Upsert to re-LLM (required) ──
    rellm_ok = process_destination(
        "re-LLM", rellm_url, rellm_key, rellm_schema, datasets_with_records
    )

    # ── Final status ──
    print("\n" + "=" * 60)
    print("  IMPORT COMPLETE")
    print("=" * 60)

    if not rellm_ok:
        print("  FAILED: re-LLM had errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
