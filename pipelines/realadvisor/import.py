#!/usr/bin/env python3
"""
RealAdvisor — Import Pipeline (Pure GraphQL)

Fetches benchmark/commune price data from RealAdvisor's Hasura GraphQL API
for Canton de Genève and upserts into bronze."RealAdvisor" on lamap_db.

Source:
  GraphQL: https://hasura-scrapers-fqs3j3myvq-ew.a.run.app/v1/graphql
  Queries ch_places for municipalities and localities in Geneva canton.

Strategy:
  1. Fetch all Geneva municipalities with price_stats (sale + rent, APPT + HOUSE)
  2. For each municipality, fetch child localities (postal codes) with prices
  3. For each locality, fetch street-level price data
  4. Build records matching the existing RealAdvisor table schema
  5. UPSERT by providerId

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.

Environment variables:
    LAMAP_SUPABASE_URL          - Lamap Supabase project URL (required)
    LAMAP_SUPABASE_SERVICE_KEY  - service_role key (required)
    LAMAP_SCHEMA                - target schema (default: bronze)
"""

import json
import os
import sys
import time
from datetime import datetime

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.freshness import update_dataset_meta


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

GRAPHQL_URL = "https://hasura-scrapers-fqs3j3myvq-ew.a.run.app/v1/graphql"
TABLE = "realadvisor"
CONFLICT_COLUMN = "providerId"
DATASET_CODE = "ext_realadvisor"

GRAPHQL_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json; charset=utf-8",
    "origin": "https://realadvisor.ch",
    "referer": "https://realadvisor.ch/",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
    "x-hasura-website-country-code": "CH",
    "x-hasura-website-language": "fr",
}


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


def graphql_query(query: str, variables: dict | None = None) -> dict | None:
    """Execute a GraphQL query against the Hasura endpoint with retries."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    for attempt in range(1, 4):
        try:
            r = requests.post(
                GRAPHQL_URL, headers=GRAPHQL_HEADERS, json=payload, timeout=30,
            )
            if r.status_code == 200:
                data = r.json()
                if "errors" in data:
                    print(f"    GraphQL error: {data['errors'][0].get('message', '')[:200]}")
                    return None
                return data.get("data")
            print(f"    HTTP {r.status_code} from GraphQL ({attempt}/3)")
        except Exception as e:
            print(f"    GraphQL request error ({attempt}/3): {e}")
        time.sleep(2 ** attempt)
    return None


# ──────────────────────────────────────────────────────────────
# Data fetching
# ──────────────────────────────────────────────────────────────

MUNICIPALITIES_QUERY = """
query GetMunicipalities {
  ch_places(
    where: {
      type: {_eq: "municipality"},
      state: {slug: {_eq: "canton-geneve"}}
    },
    order_by: {name: asc}
  ) {
    name
    slug
    population
    price_stats(where: {number_of_rooms: {_eq: 0}}) {
      property_main_type
      sale_price_m2_50
      yearly_rent_m2_50
      updated_at
    }
  }
}
"""

LOCALITIES_QUERY = """
query GetLocalities($municipalitySlug: String!) {
  ch_places(
    where: {
      type: {_eq: "locality"},
      municipality: {slug: {_eq: $municipalitySlug}}
    },
    order_by: {name: asc}
  ) {
    name
    slug
    postcode
    price_stats(where: {number_of_rooms: {_eq: 0}}) {
      property_main_type
      sale_price_m2_50
      yearly_rent_m2_50
    }
    street_stats_locality(
      where: {total_count: {_gte: 15}},
      order_by: {median_price_per_m2: desc},
      limit: 100
    ) {
      route
      median_price_per_m2
      total_count
      lat
      lng
    }
  }
}
"""

STREETS_QUERY = """
query GetStreets($slug: String!) {
  ch_places(where: {slug: {_eq: $slug}}) {
    street_stats_locality(
      where: {total_count: {_gte: 15}},
      order_by: {median_price_per_m2: desc},
      limit: 100
    ) {
      route
      median_price_per_m2
      total_count
      lat
      lng
    }
  }
}
"""


def extract_prices(price_stats: list) -> dict:
    """Extract APPT and HOUSE prices from price_stats array."""
    result = {"appartement": {}, "maison": {}}

    for stat in price_stats:
        ptype = stat.get("property_main_type", "")
        sale = stat.get("sale_price_m2_50")
        rent_yr = stat.get("yearly_rent_m2_50")

        if ptype == "APPT":
            if sale:
                result["appartement"]["sales_m2"] = round(sale)
            if rent_yr:
                result["appartement"]["rent_m2"] = round(rent_yr / 12)
        elif ptype == "HOUSE":
            if sale:
                result["maison"]["sales_m2"] = round(sale)
            if rent_yr:
                result["maison"]["rent_m2"] = round(rent_yr / 12)
        elif ptype == "ANY" and not result["appartement"]:
            # Fallback: use ANY prices if no APPT-specific data
            if sale:
                result["appartement"]["sales_m2"] = round(sale)
                result["maison"]["sales_m2"] = round(sale)
            if rent_yr:
                result["appartement"]["rent_m2"] = round(rent_yr / 12)
                result["maison"]["rent_m2"] = round(rent_yr / 12)

    return result


def build_streets(street_stats: list) -> list[dict]:
    """Build street list from GraphQL street_stats_locality."""
    streets = []
    for s in street_stats:
        median = s.get("median_price_per_m2")
        if median is None:
            continue
        streets.append({
            "street_name": s.get("route"),
            "avg_prices": {
                "appartement": {"sales_m2": round(median)},
                "maison": {"sales_m2": round(median)},
            },
        })
    return streets


def extract_date_from_stats(price_stats: list) -> tuple[int, int]:
    """Extract year/month from the updated_at field of price_stats."""
    for stat in price_stats:
        updated = stat.get("updated_at", "")
        if updated:
            try:
                dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                return dt.year, dt.month
            except (ValueError, TypeError):
                pass
    now = datetime.now()
    return now.year, now.month


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    lamap_url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    lamap_key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    lamap_schema = os.environ.get("RE_LLM_SCHEMA", "bronze_ch")
    camelote_url = os.environ.get("CAMELOTE_SUPABASE_URL", "")
    camelote_key = os.environ.get("CAMELOTE_SUPABASE_KEY", "")

    if not lamap_url or not lamap_key:
        print("ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  RealAdvisor Pipeline (Pure GraphQL)")
    print(f"  Source: {GRAPHQL_URL}")
    print(f"  Target: {lamap_schema}.{TABLE}")
    print("=" * 60)

    # ── Row count BEFORE ──
    rows_before = get_row_count(lamap_url, lamap_key, lamap_schema, TABLE)
    print(f"  Rows before: {rows_before}" if rows_before is not None else "  Rows before: unknown")

    # ── Fetch municipalities ──
    print("\n  Fetching Geneva municipalities...")
    data = graphql_query(MUNICIPALITIES_QUERY)
    if not data or not data.get("ch_places"):
        print("  ERROR: Could not fetch municipalities from GraphQL")
        sys.exit(1)

    municipalities = data["ch_places"]
    print(f"  Municipalities found: {len(municipalities)}")

    # ── Extract date from first municipality with price data ──
    year, month = None, None
    for m in municipalities:
        if m.get("price_stats"):
            year, month = extract_date_from_stats(m["price_stats"])
            break
    if not year:
        now = datetime.now()
        year, month = now.year, now.month
    print(f"  Data period: {month}/{year}")

    # ── Process each municipality ──
    records = []
    for i, muni in enumerate(municipalities):
        name = muni["name"]
        slug = muni["slug"]
        population = muni.get("population")
        print(f"\n  [{i + 1}/{len(municipalities)}] {name}")

        # Extract municipality-level prices
        avg_prices = extract_prices(muni.get("price_stats", []))
        if not avg_prices["appartement"] and not avg_prices["maison"]:
            print("    No price data, skipping")
            continue

        # Fetch child localities (postal codes)
        loc_data = graphql_query(LOCALITIES_QUERY, {"municipalitySlug": slug})
        localities = loc_data.get("ch_places", []) if loc_data else []

        code_postaux = []
        for loc in localities:
            postcode = loc.get("postcode")
            if not postcode:
                continue

            loc_prices = extract_prices(loc.get("price_stats", []))
            loc_streets = build_streets(loc.get("street_stats_locality", []))

            # Use locality prices if available, else inherit from municipality
            entry_prices = loc_prices if (loc_prices["appartement"] or loc_prices["maison"]) else avg_prices

            code_postaux.append({
                "no_postal": postcode,
                "avg_prices": entry_prices,
                "rues": loc_streets,
            })

        print(f"    Prices: APPT {avg_prices['appartement'].get('sales_m2', 'N/A')}/m², "
              f"HOUSE {avg_prices['maison'].get('sales_m2', 'N/A')}/m²")
        print(f"    Postal codes: {len(code_postaux)}")

        # If no localities found, try streets at municipality level
        if not code_postaux:
            streets_data = graphql_query(STREETS_QUERY, {"slug": slug})
            if streets_data:
                places = streets_data.get("ch_places", [])
                if places:
                    streets = build_streets(places[0].get("street_stats_locality", []))
                    if streets:
                        code_postaux = [{"no_postal": "", "avg_prices": avg_prices, "rues": streets}]

        provider_id = f"ge_{name.lower()}_{year}_{month}"

        record = {
            "providerId": provider_id,
            "canton": "Genève",
            "commune_name": name,
            "population": population,
            "currency": "CHF",
            "avg_prices": avg_prices,
            "year": str(year),
            "month": str(month),
            "code_postaux": code_postaux,
        }
        records.append(record)
        time.sleep(0.3)

    # ── Upsert ──
    print(f"\n{'━' * 60}")
    print(f"  Upserting {len(records)} commune records...")

    upserted = batch_upsert(
        url=lamap_url,
        key=lamap_key,
        table=TABLE,
        records=records,
        conflict_column=CONFLICT_COLUMN,
        schema=lamap_schema,
        batch_size=50,
    )

    # ── Row count AFTER ──
    rows_after = get_row_count(lamap_url, lamap_key, lamap_schema, TABLE)

    # ── Summary ──
    print(f"\n{'=' * 60}")
    print("  IMPORT COMPLETE")
    print(f"  Communes fetched:  {len(records)}")
    print(f"  Records upserted:  {upserted}")
    print(f"  Rows before:       {rows_before}" if rows_before is not None else "  Rows before:       unknown")
    print(f"  Rows after:        {rows_after}" if rows_after is not None else "  Rows after:        unknown")
    if rows_before is not None and rows_after is not None:
        delta = rows_after - rows_before
        print(f"  Net new:           {delta}")
    print("=" * 60)

    if upserted == 0:
        print("  FAILED: Zero rows upserted!")
        sys.exit(1)

    # ── Update dataset metadata ──
    update_dataset_meta(
        camelote_url, camelote_key, DATASET_CODE,
        record_count=rows_after,
        status="active",
    )


if __name__ == "__main__":
    main()
