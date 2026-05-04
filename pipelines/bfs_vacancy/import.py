#!/usr/bin/env python3
"""
BFS Vacancy Rates — Commune-level Import Pipeline

Downloads commune-level vacancy data (Leerwohnungszählung) from BFS via the
SDMX REST API and upserts into bronze_ch.bfs_vacancy_rates on re-LLM.

Source:  BFS / Office fédéral de la statistique
         Dataflow: CH1.LWZ:DF_LWZ_1 (1.0.0)
         Title: "Vacant dwellings by region, canton, district, municipality,
         number of rooms and type of vacant dwelling"
         Years: 1995-present (annual, reference date June 1)
         Released: usually September of the same year

SDMX REST endpoint:
    https://disseminate.stats.swiss/rest/data/CH1.LWZ,DF_LWZ_1,1.0.0/all
    Accept: application/vnd.sdmx.data+csv;version=2.0.0

Filter: WOHN_ANZAHL=_T (all room counts), LEERWOHN_TYP=_T (all vacancy types).
        DIFF_REGION_REF=POLG (commune level - all rows in totals slice).
        Skip GR_KT_GDE=8100 (Switzerland total aggregate).

Target:  bronze_ch.bfs_vacancy_rates on re-LLM
Conflict: year, bfs_commune_number

Coverage: ~2,200 communes × 31 years (1995-2025) → ~57k commune-year pairs.
Many older years have NULL counts for smaller communes (data quality varies).

DATA SAFETY:
    - UPSERT only. Never truncates or deletes.
    - Row count should only go UP or stay the same.

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

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.freshness import get_dataset_meta, update_dataset_meta


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

SDMX_URL = "https://disseminate.stats.swiss/rest/data/CH1.LWZ,DF_LWZ_1,1.0.0/all"
SDMX_ACCEPT = "application/vnd.sdmx.data+csv;version=2.0.0"

TABLE = "bfs_vacancy_rates"
CONFLICT_COLUMN = "year,bfs_commune_number"
BATCH_SIZE = 1000
DATASET_CODE = "ch_bfs_vacancy_rates"

# Filter: only the commune-aggregate slice (totals across rooms and types)
# Skip Switzerland total (8100) and any aggregate codes
EXCLUDED_GR_KT_GDE_CODES = {"8100"}


# ──────────────────────────────────────────────────────────────
# Fetch
# ──────────────────────────────────────────────────────────────

def fetch_sdmx_csv() -> str:
    """Download the SDMX CSV. Returns CSV text."""
    print(f"  Fetching SDMX CSV from {SDMX_URL}")
    r = requests.get(
        SDMX_URL,
        headers={
            "User-Agent": "Mozilla/5.0 (camelote-data-pipelines)",
            "Accept": SDMX_ACCEPT,
        },
        timeout=600,
    )
    r.raise_for_status()
    print(f"  Downloaded: {len(r.content):,} bytes")
    return r.content.decode("utf-8")


def parse_csv_to_records(csv_text: str) -> list[dict]:
    """
    Parse SDMX CSV. Filter to totals slice (WOHN_ANZAHL=_T, LEERWOHN_TYP=_T).
    Pivot V (vacant_dwellings count) and PC (vacancy_rate_pct) into a single row
    per (commune, year).
    """
    reader = csv.DictReader(io.StringIO(csv_text))

    # Aggregate by (commune_code, year)
    by_key: dict[tuple, dict] = {}
    skipped_aggregate = 0
    skipped_non_total = 0

    for row in reader:
        if row.get("WOHN_ANZAHL") != "_T" or row.get("LEERWOHN_TYP") != "_T":
            skipped_non_total += 1
            continue

        gr_kt_gde = (row.get("GR_KT_GDE") or "").strip()
        if not gr_kt_gde or gr_kt_gde in EXCLUDED_GR_KT_GDE_CODES:
            skipped_aggregate += 1
            continue

        # Skip non-commune entities (cantons / districts). DIFF_REGION_REF=POLG = commune.
        # In the DF_LWZ_1 totals slice, all rows are POLG, but defensive check.
        region_ref = row.get("DIFF_REGION_REF", "")
        if region_ref and region_ref != "POLG":
            skipped_aggregate += 1
            continue

        try:
            commune_number = int(gr_kt_gde)
        except ValueError:
            skipped_aggregate += 1
            continue

        try:
            year = int(row.get("TIME_PERIOD", ""))
        except ValueError:
            continue

        measure = row.get("MEASURE_DIMENSION", "")
        obs_value = row.get("OBS_VALUE", "")
        if obs_value == "":
            obs_value = None
        else:
            try:
                obs_value = float(obs_value)
            except ValueError:
                obs_value = None

        key = (year, commune_number)
        rec = by_key.setdefault(key, {
            "year": year,
            "bfs_commune_number": commune_number,
            "vacant_dwellings": None,
            "vacancy_rate_pct": None,
        })

        if measure == "V":
            rec["vacant_dwellings"] = int(obs_value) if obs_value is not None else None
        elif measure == "PC":
            rec["vacancy_rate_pct"] = obs_value

    # Build final list, dropping rows that have neither V nor PC
    records = []
    for rec in by_key.values():
        if rec["vacant_dwellings"] is not None or rec["vacancy_rate_pct"] is not None:
            records.append(rec)

    print(f"  Parsed: {len(records):,} commune-year rows")
    print(f"  Skipped (non-total slice): {skipped_non_total:,}")
    print(f"  Skipped (aggregate or non-commune): {skipped_aggregate:,}")
    return records


# ──────────────────────────────────────────────────────────────
# Enrich with canton_code / commune_name from bfs_population
# ──────────────────────────────────────────────────────────────

def fetch_commune_lookup(url: str, key: str, schema: str) -> dict[int, dict]:
    """
    Build a {bfs_commune_number → {canton_code, commune_name}} lookup from
    bronze_ch.bfs_population (most recent year per commune).

    Returns empty dict if bfs_population is empty (graceful no-op).
    """
    endpoint = (
        f"{url.rstrip('/')}/rest/v1/bfs_population"
        "?select=bfs_commune_number,canton_code,commune_name,year"
        "&order=year.desc&limit=10000"
    )
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
    }
    if schema and schema != "public":
        headers["Accept-Profile"] = schema
    try:
        r = requests.get(endpoint, headers=headers, timeout=60)
        if r.status_code != 200:
            print(f"  Warning: commune lookup HTTP {r.status_code}; skipping enrichment")
            return {}
        rows = r.json() or []
        # Take the latest record per commune
        seen: dict[int, dict] = {}
        for row in rows:
            cn = row.get("bfs_commune_number")
            if cn and cn not in seen:
                seen[cn] = {
                    "canton_code": row.get("canton_code"),
                    "commune_name": row.get("commune_name"),
                }
        print(f"  Commune lookup: {len(seen):,} entries from bfs_population")
        return seen
    except Exception as e:
        print(f"  Warning: commune lookup failed ({e}); skipping enrichment")
        return {}


def enrich_records(records: list[dict], lookup: dict[int, dict]) -> list[dict]:
    """Add canton_code and commune_name from lookup; leave NULL if not found."""
    matched = 0
    for r in records:
        meta = lookup.get(r["bfs_commune_number"])
        if meta:
            r["canton_code"] = meta.get("canton_code")
            r["commune_name"] = meta.get("commune_name")
            matched += 1
        else:
            r["canton_code"] = None
            r["commune_name"] = None
    if lookup:
        pct = 100 * matched / len(records) if records else 0
        print(f"  Enrichment: {matched:,}/{len(records):,} matched ({pct:.1f}%)")
    return records


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def get_row_count(url: str, key: str, schema: str) -> int | None:
    endpoint = f"{url.rstrip('/')}/rest/v1/{TABLE}?select=count"
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
    except Exception:
        pass
    return None


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
    print("  BFS Vacancy Rates — Commune-level Pipeline")
    print(f"  Source:   BFS SDMX (CH1.LWZ:DF_LWZ_1)")
    print(f"  Target:   {rellm_schema}.{TABLE} on re-LLM")
    print(f"  Conflict: {CONFLICT_COLUMN}")
    print("=" * 60)

    # ── Freshness pre-check ──
    if camelote_url and camelote_key:
        meta = get_dataset_meta(camelote_url, camelote_key, DATASET_CODE)
        if meta and meta.get("last_acquired_at"):
            print(f"\n  Last acquired: {meta['last_acquired_at'].isoformat()}")

    # ── Row count BEFORE ──
    rows_before = get_row_count(rellm_url, rellm_key, rellm_schema)
    print(f"\n  Rows before: {rows_before:,}" if rows_before is not None else "\n  Rows before: unknown")

    # ── Fetch SDMX CSV ──
    start = time.time()
    csv_text = fetch_sdmx_csv()

    # ── Parse + filter ──
    records = parse_csv_to_records(csv_text)
    if not records:
        print("\n  No records to upsert. Exiting.")
        sys.exit(1)

    # ── Enrich with canton_code / commune_name ──
    lookup = fetch_commune_lookup(rellm_url, rellm_key, rellm_schema)
    records = enrich_records(records, lookup)

    # ── Upsert ──
    print(f"\n  Upserting {len(records):,} rows in batches of {BATCH_SIZE}")
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

    # ── Row count AFTER ──
    rows_after = get_row_count(rellm_url, rellm_key, rellm_schema)

    print(f"\n{'=' * 60}")
    print(f"  IMPORT COMPLETE")
    print(f"  Rows processed:   {len(records):,}")
    print(f"  Rows upserted:    {upserted:,}")
    print(f"  Rows before:      {rows_before:,}" if rows_before is not None else "  Rows before:      unknown")
    print(f"  Rows after:       {rows_after:,}" if rows_after is not None else "  Rows after:       unknown")
    if rows_before is not None and rows_after is not None:
        delta = rows_after - rows_before
        print(f"  Net new:          {delta:,}")
        if rows_after < rows_before:
            print("  WARNING: Row count DECREASED!")
    print(f"  Duration:         {elapsed / 60:.1f} min")
    print("=" * 60)

    if upserted == 0:
        print("  FAILED: Zero rows upserted!")
        sys.exit(1)

    # ── Update dataset metadata ──
    if camelote_url and camelote_key:
        update_dataset_meta(
            camelote_url, camelote_key, DATASET_CODE,
            record_count=rows_after,
            status="active",
        )


if __name__ == "__main__":
    main()
