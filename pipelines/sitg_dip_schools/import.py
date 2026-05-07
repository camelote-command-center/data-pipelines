#!/usr/bin/env python3
"""
SITG DIP Schools — Consolidated Import Pipeline

Fetches 11 SITG ArcGIS endpoints under the DIP (Département de l'instruction
publique) namespace and upserts each into its own bronze_ch table on re-LLM.

CONFIG-DRIVEN: One config dict, one bronze table per endpoint, one parser.

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

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert  # noqa: E402
from shared.sitg_arcgis import fetch_all_features  # noqa: E402

# ──────────────────────────────────────────────────────────────
# Datasets — 11 DIP endpoints
# ──────────────────────────────────────────────────────────────

SITG_BASE = "https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/{slug}/FeatureServer/0"

DATASETS = [
    {"slug": "DIP_BIBLIOTHEQUES_SCOLAIRES",  "table": "ge_dip_bibliotheques_scolaires"},
    {"slug": "DIP_COLLEGES",                 "table": "ge_dip_colleges"},
    {"slug": "DIP_CYCLES_ORIENTATION",       "table": "ge_dip_cycles_orientation"},
    {"slug": "DIP_ECOLES_PRIMAIRE",          "table": "ge_dip_ecoles_primaires"},
    {"slug": "DIP_ECOLE_PRIMAIRE_BATIMENT",  "table": "ge_dip_ecole_primaire_batiment"},
    {"slug": "DIP_ECOLES_COMMERCE",          "table": "ge_dip_ecoles_commerce"},
    {"slug": "DIP_ECOLES_CULTURE_GENERALE",  "table": "ge_dip_ecoles_culture_generale"},
    {"slug": "DIP_ENSEIGNEMENT_SPECIALISE",  "table": "ge_dip_enseignement_specialise"},
    {"slug": "DIP_FORMATION_PROFESSIONNELLE","table": "ge_dip_formation_professionnelle"},
    {"slug": "DIP_POLE_MEDICO_PSYCHOLOGIQUE","table": "ge_dip_pole_medico_psychologique"},
    {"slug": "DIP_UNIVERSITES",              "table": "ge_dip_universites"},
]

CONFLICT_COLUMN = "objectid"

# globalid is an ArcGIS-only identifier; conflict key is objectid.
EXCLUDE_FIELDS = {"globalid", "iteration"}


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
    except Exception:
        pass
    return None


def get_table_columns(url: str, key: str, schema: str, table: str) -> set[str]:
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?select=*&limit=1"
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    if schema and schema != "public":
        headers["Accept-Profile"] = schema
    try:
        r = requests.get(endpoint, headers=headers, timeout=15)
        if r.status_code == 200:
            rows = r.json()
            if rows:
                return set(rows[0].keys())
            # Empty table: fall back to OPTIONS or schema discovery via the openapi endpoint
            r2 = requests.get(f"{url.rstrip('/')}/rest/v1/", headers=headers, timeout=15)
            if r2.status_code == 200:
                spec = r2.json()
                paths = spec.get("definitions", {})
                t = paths.get(table, {})
                props = t.get("properties", {})
                if props:
                    return set(props.keys())
    except Exception:
        pass
    return set()


def has_column(url: str, key: str, schema: str, table: str, column: str) -> bool:
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?select={column}&limit=0"
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    if schema and schema != "public":
        headers["Accept-Profile"] = schema
    try:
        r = requests.get(endpoint, headers=headers, timeout=10)
        return r.status_code == 200
    except Exception:
        return False


def filter_to_known(records: list[dict], known: set[str]) -> list[dict]:
    if not known:
        return records
    return [{k: v for k, v in r.items() if k in known} for r in records]


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    schema = os.environ.get("RE_LLM_SCHEMA", "bronze_ch")

    if not url or not key:
        print("ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  SITG DIP Schools Pipeline")
    print(f"  Datasets: {len(DATASETS)}")
    print(f"  Target:   {schema} on {url}")
    print("=" * 60)

    any_fail = False

    for ds in DATASETS:
        slug, table = ds["slug"], ds["table"]
        print(f"\n{'━' * 60}")
        print(f"  {slug} → {schema}.{table}")
        print(f"{'━' * 60}")

        api = SITG_BASE.format(slug=slug)
        try:
            records = fetch_all_features(api, include_geometry=True)
        except Exception as e:
            print(f"  FETCH ERROR: {e}")
            any_fail = True
            continue

        if not records:
            print("  No records fetched.")
            continue

        # Drop excluded fields
        for r in records:
            for f in EXCLUDE_FIELDS:
                r.pop(f, None)

        # Filter to known table columns
        known = get_table_columns(url, key, schema, table)
        if known:
            records = filter_to_known(records, known)

        # Strip geometry if no column
        if not has_column(url, key, schema, table, "geometry"):
            records = [{k: v for k, v in r.items() if k != "geometry"} for r in records]

        rows_before = get_row_count(url, key, schema, table)
        print(f"  Rows before: {rows_before}")

        upserted = batch_upsert(
            url=url, key=key, table=table,
            records=records,
            conflict_column=CONFLICT_COLUMN,
            schema=schema, batch_size=500,
        )

        rows_after = get_row_count(url, key, schema, table)
        delta = (rows_after - rows_before) if (rows_before is not None and rows_after is not None) else None
        print(f"  Upserted: {upserted}  Rows after: {rows_after}  Net new: {delta}")

        if upserted == 0:
            print("  ERROR: zero upserted")
            any_fail = True

    print("\n" + "=" * 60)
    print("  IMPORT COMPLETE" + ("  (with errors)" if any_fail else ""))
    print("=" * 60)
    if any_fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
