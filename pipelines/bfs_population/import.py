#!/usr/bin/env python3
"""
BFS Population by Commune — Import Pipeline

Downloads commune-level population history from the Swiss Federal Statistical
Office (BFS) via the PX-X JSON-stat2 API and upserts into bronze_ch.bfs_population
on re-LLM.

Source:  BFS PX-X portal
         Dataset: px-x-0102020000_201
                  "Demografische Bilanz nach institutionellen Gliederungen"
         Year-end stock by canton/district/commune × nationality × sex × component.
         We pull one query per year, restricted to demographic component 16
         ("Bestand am 31. Dezember"), sex=Total, all 3 nationality categories.

Target:  bronze_ch.bfs_population
Conflict: year,bfs_commune_number

Year range: 1995-2024 by default (override with BFS_POP_YEARS env var, e.g. "2024" or "2020-2024").

DATA SAFETY:
    - UPSERT only. Never truncates or deletes.
    - Row count should only go UP or stay the same.

Environment variables:
    RE_LLM_SUPABASE_URL              - re-LLM Supabase project URL (required)
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY - service_role key (required)
    RE_LLM_SCHEMA                    - target schema (default: bronze_ch)
    BFS_POP_YEARS                    - optional override, e.g. "2024" or "1995-2024"
    CAMELOTE_SUPABASE_URL            - Command center URL (optional, for metadata)
    CAMELOTE_SUPABASE_KEY            - Command center key (optional)
"""

import json
import os
import re
import sys
import time

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.freshness import update_dataset_meta


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

PXWEB_URL = "https://www.pxweb.bfs.admin.ch/api/v1/de/px-x-0102020000_201/px-x-0102020000_201.px"

# Dataset dimensions (from metadata):
#   Jahr                                              -> year
#   Kanton (-) / Bezirk (>>) / Gemeinde (......)      -> entity
#   Staatsangehörigkeit (Kategorie)                   -> 0=Total, 1=Schweiz, 2=Ausland
#   Geschlecht                                        -> 0=Total
#   Demografische Komponente                          -> 16=Bestand am 31. Dezember
DIM_YEAR = "Jahr"
DIM_ENTITY = "Kanton (-) / Bezirk (>>) / Gemeinde (......)"
DIM_NATIONALITY = "Staatsangehörigkeit (Kategorie)"
DIM_SEX = "Geschlecht"
DIM_COMPONENT = "Demografische Komponente"

DEFAULT_FIRST_YEAR = 1995
DEFAULT_LAST_YEAR = 2024  # PX-X dataset upper bound at time of writing

TABLE = "bfs_population"
CONFLICT_COLUMN = "year,bfs_commune_number"
BATCH_SIZE = 1000
DATASET_CODE = "ch_bfs_population"

CANTON_NAME_TO_CODE = {
    "zürich": "ZH", "bern": "BE", "bern / berne": "BE", "luzern": "LU", "uri": "UR",
    "schwyz": "SZ", "obwalden": "OW", "nidwalden": "NW", "glarus": "GL",
    "zug": "ZG", "freiburg": "FR", "fribourg": "FR", "freiburg / fribourg": "FR",
    "fribourg / freiburg": "FR",
    "solothurn": "SO", "basel-stadt": "BS", "basel-landschaft": "BL",
    "schaffhausen": "SH", "appenzell ausserrhoden": "AR", "appenzell a.rh.": "AR",
    "appenzell innerrhoden": "AI", "appenzell i.rh.": "AI",
    "st. gallen": "SG", "st.gallen": "SG", "graubünden": "GR",
    "graubünden / grigioni / grischun": "GR",
    "aargau": "AG", "thurgau": "TG", "tessin": "TI", "ticino": "TI",
    "waadt": "VD", "vaud": "VD", "wallis": "VS", "valais": "VS",
    "wallis / valais": "VS", "valais / wallis": "VS",
    "neuenburg": "NE", "neuchâtel": "NE", "genf": "GE", "genève": "GE",
    "jura": "JU",
}


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


def parse_years_env(env_val: str | None) -> list[int]:
    if not env_val:
        return list(range(DEFAULT_FIRST_YEAR, DEFAULT_LAST_YEAR + 1))
    s = env_val.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(y) for y in s.split(",")]


# ──────────────────────────────────────────────────────────────
# PX-X fetch + parse
# ──────────────────────────────────────────────────────────────

def fetch_year(year: int, retries: int = 3) -> dict:
    """POST a query for a single year and return parsed JSON-stat2."""
    query = {
        "query": [
            {"code": DIM_YEAR, "selection": {"filter": "item", "values": [str(year)]}},
            {"code": DIM_ENTITY, "selection": {"filter": "all", "values": ["*"]}},
            {"code": DIM_NATIONALITY, "selection": {"filter": "item", "values": ["0", "1", "2"]}},
            {"code": DIM_SEX, "selection": {"filter": "item", "values": ["0"]}},
            {"code": DIM_COMPONENT, "selection": {"filter": "item", "values": ["16"]}},
        ],
        "response": {"format": "json-stat2"},
    }
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(PXWEB_URL, json=query, timeout=180)
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.HTTPError) as e:
            last_err = e
            if attempt < retries:
                wait = attempt * 10
                print(f"    {year}: error ({e}); retrying in {wait}s...")
                time.sleep(wait)
    raise RuntimeError(f"fetch_year({year}) failed after {retries} attempts: {last_err}")


def jsonstat_to_records(data: dict, year: int) -> list[dict]:
    """
    Pivot a single-year JSON-stat2 response to one record per commune.

    Iterates over entities in order; tracks current canton from "- <CantonName>"
    headers. Communes match "......NNNN <Name>".
    """
    dims = data["dimension"]
    dim_order = data["id"]  # e.g. ['Jahr','Kanton (...)', 'Staatsangehörigkeit ...', 'Geschlecht', 'Demografische Komponente']
    sizes = data["size"]
    values = data["value"]

    entity_dim = dims[DIM_ENTITY]["category"]
    nat_dim = dims[DIM_NATIONALITY]["category"]

    entity_index = entity_dim["index"]   # code -> position
    entity_label = entity_dim["label"]   # code -> "Schweiz" / "- Zürich" / ">> Bezirk Affoltern" / "......0001 Aeugst..."
    nat_index = nat_dim["index"]
    nat_codes_by_pos = {pos: code for code, pos in nat_index.items()}

    # Stride math for flat array indexing
    strides = []
    s = 1
    for sz in reversed(sizes):
        strides.append(s)
        s *= sz
    strides.reverse()

    def value_at(coord):
        flat = sum(c * st for c, st in zip(coord, strides))
        return values[flat]

    pos_year = list(dim_order).index(DIM_YEAR)
    pos_entity = list(dim_order).index(DIM_ENTITY)
    pos_nat = list(dim_order).index(DIM_NATIONALITY)
    pos_sex = list(dim_order).index(DIM_SEX)
    pos_comp = list(dim_order).index(DIM_COMPONENT)

    # Sort entities by position so we walk them in display order (canton -> district -> communes)
    entities_ordered = sorted(entity_label.items(), key=lambda kv: entity_index[kv[0]])

    current_canton = None
    records = []
    commune_re = re.compile(r"^\.{4,}(\d+)\s+(.*)")

    for code, label in entities_ordered:
        # Canton header: "- Zürich"
        if label.startswith("- "):
            canton_name = label[2:].strip().lower()
            canton_name = re.sub(r"\s*\d+\)\s*$", "", canton_name).strip()
            current_canton = CANTON_NAME_TO_CODE.get(canton_name)
            if not current_canton:
                print(f"    Warning: unknown canton '{label[2:].strip()}' at year {year}")
            continue

        m = commune_re.match(label)
        if not m:
            continue

        bfs_number = int(m.group(1))
        commune_name = re.sub(r"\s*\d+\)\s*$", "", m.group(2).strip()).strip()

        ent_pos = entity_index[code]

        # Build coordinates for each nationality value
        coord = [0] * len(sizes)
        coord[pos_year] = 0
        coord[pos_entity] = ent_pos
        coord[pos_sex] = 0
        coord[pos_comp] = 0

        per_nat = {}
        for nat_pos, nat_code in nat_codes_by_pos.items():
            coord[pos_nat] = nat_pos
            per_nat[nat_code] = value_at(coord)

        rec = {
            "year": year,
            "bfs_commune_number": bfs_number,
            "commune_name": commune_name,
            "canton_code": current_canton,
            "total_population": per_nat.get("0"),
            "swiss_nationals": per_nat.get("1"),
            "foreign_nationals": per_nat.get("2"),
        }
        records.append(rec)

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
    years = parse_years_env(os.environ.get("BFS_POP_YEARS"))

    if not rellm_url or not rellm_key:
        print("ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  BFS Population by Commune Pipeline")
    print(f"  Source: {PXWEB_URL}")
    print(f"  Target: {rellm_schema}.{TABLE}")
    print(f"  Years:  {years[0]}-{years[-1]} ({len(years)} years)")
    print("=" * 60)

    rows_before = get_row_count(rellm_url, rellm_key, rellm_schema)
    print(f"\n  Rows before: {rows_before:,}" if rows_before is not None else "\n  Rows before: unknown")

    start = time.time()
    all_records: list[dict] = []
    for year in years:
        t0 = time.time()
        data = fetch_year(year)
        recs = jsonstat_to_records(data, year)
        all_records.extend(recs)
        print(f"  {year}: {len(recs):,} commune rows ({time.time()-t0:.1f}s)")

    print(f"\n  Parsed {len(all_records):,} total commune-year rows")

    if not all_records:
        print("  ERROR: parsed zero rows")
        sys.exit(1)

    print(f"\n  Upserting {len(all_records):,} records...")
    upserted = batch_upsert(
        url=rellm_url,
        key=rellm_key,
        table=TABLE,
        records=all_records,
        conflict_column=CONFLICT_COLUMN,
        schema=rellm_schema,
        batch_size=BATCH_SIZE,
    )

    elapsed = time.time() - start
    rows_after = get_row_count(rellm_url, rellm_key, rellm_schema)

    print(f"\n{'=' * 60}")
    print("  IMPORT COMPLETE")
    print(f"  Records parsed:  {len(all_records):,}")
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
