#!/usr/bin/env python3
"""
BFS Dwellings by Commune — Import Pipeline

Downloads commune-level total dwellings from the Swiss Federal Statistical
Office (BFS) via the SDMX REST endpoint and upserts into bronze_ch.bfs_dwellings
on re-LLM.

Source:  BFS SDMX REST
         Dataflow: CH1.GWS:DF_GWS_REG5 (1.0.0)
                   "Dwellings by canton/municipality, building category,
                    number of rooms, and construction period"
         Annual GWS register-based figures, 2010-present.

Filter:  GBAUPS=_T (all construction periods)
         GKATS=_T (all building categories)
         WAZIMS=_T (all room counts)
         GEMEINDENAME=<all> — filter to commune-level rows in code.

Aggregates skipped:
  - GEMEINDENAME=8100 (Switzerland total)
  - alpha codes (canton 2-letter codes like ZH, BE, ...)
  - keep numeric codes (1..9999) → bfs_commune_number

Target:  bronze_ch.bfs_dwellings
Conflict: year,bfs_commune_number

Enrichment:
  commune_name + canton_code are looked up from bronze_ch.bfs_population
  via PostgREST.

DATA SAFETY:
    - UPSERT only. Never truncates or deletes.

Environment variables:
    RE_LLM_SUPABASE_URL              - re-LLM Supabase project URL (required)
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY - service_role key (required)
    RE_LLM_SCHEMA                    - target schema (default: bronze_ch)
    CAMELOTE_SUPABASE_URL            - Command center URL (optional, for metadata)
    CAMELOTE_SUPABASE_KEY            - Command center key (optional)
"""

import csv
import io
import os
import sys
import time

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.freshness import update_dataset_meta


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

SDMX_URL = (
    "https://disseminate.stats.swiss/rest/data/CH1.GWS,DF_GWS_REG5,1.0.0"
    "/A._T._T._T./?detail=dataonly"
)
SDMX_ACCEPT = "application/vnd.sdmx.data+csv;version=2.0.0"

TABLE = "bfs_dwellings"
CONFLICT_COLUMN = "year,bfs_commune_number"
BATCH_SIZE = 1000
DATASET_CODE = "ch_bfs_dwellings"


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def get_row_count(url: str, key: str, schema: str) -> int | None:
    endpoint = f"{url.rstrip('/')}/rest/v1/{TABLE}?select=count"
    headers = {"apikey": key, "Authorization": f"Bearer {key}", "Prefer": "count=exact"}
    if schema and schema != "public":
        headers["Accept-Profile"] = schema
    try:
        r = requests.head(endpoint, headers=headers, timeout=30)
        cr = r.headers.get("content-range", "")
        if "/" in cr:
            return int(cr.split("/")[1])
    except Exception as e:
        print(f"  Warning: row count lookup failed: {e}")
    return None


def fetch_population_lookup(url: str, key: str, schema: str) -> dict[int, tuple[str | None, str | None]]:
    """Fetch (bfs_commune_number → (commune_name, canton_code)) from bronze_ch.bfs_population."""
    headers = {"apikey": key, "Authorization": f"Bearer {key}", "Accept-Profile": schema}
    out: dict[int, tuple[str | None, str | None]] = {}
    page = 1000
    offset = 0
    while True:
        r = requests.get(
            f"{url.rstrip('/')}/rest/v1/bfs_population"
            f"?select=bfs_commune_number,commune_name,canton_code&order=bfs_commune_number.asc",
            headers={**headers, "Range": f"{offset}-{offset+page-1}"},
            timeout=60,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        for row in rows:
            n = row.get("bfs_commune_number")
            if n is None:
                continue
            # First seen wins; later years' overrides preserved if commune_name was missing
            if n not in out or (out[n][0] is None and row.get("commune_name")):
                out[n] = (row.get("commune_name"), row.get("canton_code"))
        if len(rows) < page:
            break
        offset += page
    return out


# ──────────────────────────────────────────────────────────────
# SDMX fetch + parse
# ──────────────────────────────────────────────────────────────

def download_sdmx() -> str:
    print(f"  GET {SDMX_URL}")
    r = requests.get(SDMX_URL, headers={"Accept": SDMX_ACCEPT}, timeout=300)
    r.raise_for_status()
    text = r.text
    print(f"  Downloaded {len(text):,} chars")
    return text


def parse_records(csv_text: str, lookup: dict[int, tuple[str | None, str | None]]) -> list[dict]:
    reader = csv.DictReader(io.StringIO(csv_text))
    records = []
    skipped_aggregate = 0
    skipped_non_numeric = 0
    skipped_no_value = 0
    for row in reader:
        gem = row.get("GEMEINDENAME", "").strip()
        if not gem:
            continue
        if gem == "8100":
            skipped_aggregate += 1
            continue
        if not gem.isdigit():
            skipped_non_numeric += 1
            continue
        try:
            bfs_number = int(gem)
        except ValueError:
            skipped_non_numeric += 1
            continue

        try:
            year = int(row["TIME_PERIOD"])
        except (KeyError, ValueError):
            continue

        try:
            value = int(row["OBS_VALUE"])
        except (KeyError, ValueError):
            skipped_no_value += 1
            continue

        commune_name, canton_code = lookup.get(bfs_number, (None, None))

        records.append({
            "year": year,
            "bfs_commune_number": bfs_number,
            "commune_name": commune_name,
            "canton_code": canton_code,
            "total_dwellings": value,
            "source": "BFS",
        })

    print(f"  Parsed {len(records):,} commune-year rows "
          f"(skipped: {skipped_aggregate} country, {skipped_non_numeric} canton, "
          f"{skipped_no_value} no value)")
    return records


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    rellm_url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    rellm_key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    rellm_schema = os.environ.get("RE_LLM_SCHEMA", "bronze_ch")
    camelote_url = os.environ.get("CAMELOTE_SUPABASE_URL", "")
    camelote_key = os.environ.get("CAMELOTE_SUPABASE_KEY", "")

    if not rellm_url or not rellm_key:
        print("ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  BFS Dwellings by Commune Pipeline")
    print(f"  Source: {SDMX_URL}")
    print(f"  Target: {rellm_schema}.{TABLE}")
    print("=" * 60)

    rows_before = get_row_count(rellm_url, rellm_key, rellm_schema)
    print(f"\n  Rows before: {rows_before:,}" if rows_before is not None else "\n  Rows before: unknown")

    print("\n  Fetching commune lookup from bronze_ch.bfs_population...")
    lookup = fetch_population_lookup(rellm_url, rellm_key, rellm_schema)
    print(f"  Loaded {len(lookup):,} communes from bfs_population")

    start = time.time()
    csv_text = download_sdmx()
    print("\n  Parsing CSV...")
    records = parse_records(csv_text, lookup)

    if not records:
        print("  ERROR: parsed zero records")
        sys.exit(1)

    print(f"\n  Upserting {len(records):,} records...")
    upserted = batch_upsert(
        url=rellm_url,
        key=rellm_key,
        table=TABLE,
        records=records,
        conflict_column=CONFLICT_COLUMN,
        schema=rellm_schema,
        batch_size=BATCH_SIZE,
    )

    elapsed = time.time() - start
    rows_after = get_row_count(rellm_url, rellm_key, rellm_schema)

    print(f"\n{'=' * 60}")
    print("  IMPORT COMPLETE")
    print(f"  Records parsed:  {len(records):,}")
    print(f"  Rows upserted:   {upserted:,}")
    print(f"  Rows before:     {rows_before:,}" if rows_before is not None else "  Rows before:     unknown")
    print(f"  Rows after:      {rows_after:,}" if rows_after is not None else "  Rows after:      unknown")
    if rows_before is not None and rows_after is not None:
        print(f"  Net new:         {rows_after - rows_before:,}")
    print(f"  Duration:        {elapsed:.1f}s")
    print("=" * 60)

    if upserted == 0:
        print("  FAILED: zero rows upserted")
        sys.exit(1)

    update_dataset_meta(
        camelote_url, camelote_key, DATASET_CODE,
        record_count=rows_after,
        status="active",
    )


if __name__ == "__main__":
    main()
