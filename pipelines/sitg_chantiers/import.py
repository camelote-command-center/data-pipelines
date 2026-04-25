#!/usr/bin/env python3
"""
SITG Chantiers — Import Pipeline (config-driven, pure ArcGIS)

Fetches 2 SITG datasets via ArcGIS REST API and upserts into re-llm bronze_ch:
  1. INFOMOB_CHANTIER_POINT  — Public-facing real-time high-impact construction sites (points, ~85 rows)
  2. PCMOB_CHANTIER_CONSULT  — Full PCM coordination platform (planned + ongoing, polygons, ~201 rows)

Note: INFOMOB is a curated subset of PCMOB (high-impact sites only) but the two
datasets have meaningfully different schemas (point vs polygon, public vs internal
fields), so both are ingested.

CONFIG-DRIVEN: Adding a new SITG table is just adding a dict to DATASETS.

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.

Environment variables:
    RE_LLM_SUPABASE_URL          - re-llm Supabase project URL (required)
    RE_LLM_SUPABASE_SERVICE_KEY  - service_role key (required)
    RE_LLM_SCHEMA                - target schema (default: bronze_ch)
"""

import os
import sys

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.sitg_arcgis import fetch_all_features

# ──────────────────────────────────────────────────────────────
# Dataset configs
# ──────────────────────────────────────────────────────────────

DATASETS = [
    {
        "name": "Infomobilité chantiers (points)",
        "code": "ge_infomob_chantier_point",
        "table": "ge_infomob_chantier_point",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/INFOMOB_CHANTIER_POINT/FeatureServer/0",
        "conflict_column": "objectid",
    },
    {
        "name": "PCM chantiers (consultation)",
        "code": "ge_pcmob_chantier_consult",
        "table": "ge_pcmob_chantier_consult",
        "source": "arcgis",
        "url": "https://vector.sitg.ge.ch/arcgis/rest/services/PCMOB_CHANTIER_CONSULT/FeatureServer/0",
        "conflict_column": "objectid",
    },
]

# Fields that exist in the source but we do not store
EXCLUDE_FIELDS: set[str] = set()


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def get_row_count(url: str, key: str, schema: str, table: str) -> int | None:
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?select=count"
    headers = {"apikey": key, "Authorization": f"Bearer {key}", "Prefer": "count=exact"}
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
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?limit=1"
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
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


def filter_to_known_columns(records: list[dict], known: set[str], exclude: set[str]) -> list[dict]:
    if not known:
        for r in records:
            for f in exclude:
                r.pop(f, None)
        return records
    allowed = known - exclude - {"id", "created_at", "updated_at"}
    # always keep conflict column even if not surfaced via /limit=1 (empty table)
    return [{k: v for k, v in r.items() if k in allowed or k == "objectid" or k == "geometry"} for r in records]


# ──────────────────────────────────────────────────────────────
# Process a single destination
# ──────────────────────────────────────────────────────────────

def process_destination(dest_name, dest_url, dest_key, dest_schema, datasets_with_records) -> bool:
    print(f"\n{'=' * 60}\n  Destination: {dest_name} ({dest_schema})\n{'=' * 60}")
    all_ok = True

    for ds, records in datasets_with_records:
        table = ds["table"]
        conflict = ds["conflict_column"]

        print(f"\n{'━' * 60}\n  [{ds['name']}] → {dest_schema}.{table}")
        print(f"  Records fetched: {len(records):,}\n{'━' * 60}")

        if not records:
            print("  No records. Skipping.")
            continue

        work = [dict(r) for r in records]
        for r in work:
            for f in EXCLUDE_FIELDS:
                r.pop(f, None)

        known_cols = get_table_columns(dest_url, dest_key, dest_schema, table)
        if known_cols:
            before = set(work[0].keys()) if work else set()
            work = filter_to_known_columns(work, known_cols, EXCLUDE_FIELDS)
            after = set(work[0].keys()) if work else set()
            dropped = before - after
            if dropped:
                print(f"  Dropped unknown columns: {', '.join(sorted(dropped))}")

        # Normalise keys
        all_keys: set[str] = set()
        for r in work:
            all_keys |= r.keys()
        for r in work:
            for k in all_keys:
                r.setdefault(k, None)

        rows_before = get_row_count(dest_url, dest_key, dest_schema, table)
        print(f"  Rows before: {rows_before if rows_before is not None else 'unknown'}")

        upserted = batch_upsert(
            url=dest_url, key=dest_key, table=table,
            records=work, conflict_column=conflict,
            schema=dest_schema, batch_size=500,
        )

        rows_after = get_row_count(dest_url, dest_key, dest_schema, table)
        print(f"\n  Results:\n    Upserted:    {upserted:,}\n    Rows before: {rows_before}\n    Rows after:  {rows_after}")

        if rows_before is not None and rows_after is not None:
            print(f"    Net new:     {rows_after - rows_before:,}")
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
    re_url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    re_key = os.environ.get("RE_LLM_SUPABASE_SERVICE_KEY", "")
    re_schema = os.environ.get("RE_LLM_SCHEMA", "bronze_ch")

    if not re_url or not re_key:
        print("ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  SITG Chantiers Pipeline")
    print(f"  Datasets: {len(DATASETS)}")
    print(f"  Target:   re-llm.{re_schema}")
    print("=" * 60)

    datasets_with_records: list[tuple[dict, list[dict]]] = []

    for ds in DATASETS:
        print(f"\n{'━' * 60}\n  Fetching: {ds['name']}\n  URL:      {ds['url'][:80]}...\n{'━' * 60}")
        try:
            records = fetch_all_features(ds["url"], include_geometry=True)
        except Exception as e:
            print(f"  FETCH ERROR: {e}")
            records = []
        datasets_with_records.append((ds, records))

    ok = process_destination("re-llm", re_url, re_key, re_schema, datasets_with_records)

    print("\n" + "=" * 60 + "\n  IMPORT COMPLETE\n" + "=" * 60)
    if not ok:
        print("  FAILED: re-llm had errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
