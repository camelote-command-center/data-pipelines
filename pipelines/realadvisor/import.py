#!/usr/bin/env python3
"""
RealAdvisor — Import Pipeline

Scrapes benchmark/commune price data from RealAdvisor for Canton de Geneve
and upserts into bronze."RealAdvisor" on lamap_db.

Sources:
  - HTML: https://realadvisor.ch/fr/prix-m2-immobilier/canton-geneve
  - GraphQL: Hasura endpoint for street-level data

Fetches commune-level sale/rent prices per m2, postal code breakdowns,
and street-level prices via GraphQL API.

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
import re
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

CANTON_URL = "https://realadvisor.ch/fr/prix-m2-immobilier/canton-geneve"
GRAPHQL_URL = "https://hasura-scrapers-fqs3j3myvq-ew.a.run.app/v1/graphql"
TABLE = "RealAdvisor"
CONFLICT_COLUMN = "providerId"

BROWSER_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
    "accept-language": "fr-CH,fr;q=0.9,en;q=0.8",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
}

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

FRENCH_MONTHS = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
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


def get_number(text: str) -> int | None:
    """Parse price string like 'CHF 12'345' to integer."""
    if not text:
        return None
    cleaned = text.replace("CHF", "").replace("'", "").replace(",", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        return None


def get_postal_number(text: str) -> str | None:
    """Extract 4-digit Swiss postal code from text."""
    match = re.search(r"\b(\d{4})\b", text)
    return match.group(1) if match else None


def fetch_html(url: str) -> BeautifulSoup | None:
    """Fetch page HTML with retries. Uses curl_cffi for browser TLS fingerprint."""
    for attempt in range(1, 4):
        try:
            r = cffi_requests.get(
                url, headers=BROWSER_HEADERS, impersonate="chrome", timeout=30,
            )
            if r.status_code == 200:
                return BeautifulSoup(r.text, "html.parser")
            print(f"    HTTP {r.status_code} for {url}")
        except Exception as e:
            print(f"    Fetch error ({attempt}/3): {e}")
        time.sleep(2 ** attempt)
    return None


# ──────────────────────────────────────────────────────────────
# Data extraction
# ──────────────────────────────────────────────────────────────

def extract_date_info(soup: BeautifulSoup) -> tuple[int, int]:
    """Extract year and month from 'prix du m2 en {month} {year}' text."""
    text = soup.get_text()
    match = re.search(r"m[²2]\s+en\s+(\w+)\s+(\d{4})", text, re.IGNORECASE)
    if match:
        month_name = match.group(1).lower()
        year = int(match.group(2))
        month = FRENCH_MONTHS.get(month_name, 1)
        return year, month
    # Fallback
    now = datetime.now()
    print(f"  Warning: could not extract date, using {now.year}/{now.month}")
    return now.year, now.month


def parse_commune_table(soup: BeautifulSoup) -> list[dict]:
    """Parse commune rows from the canton price table."""
    communes = []
    for tr in soup.select("table tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        link_el = cells[0].find("a")
        if not link_el:
            continue

        href = link_el.get("href", "")
        name = cells[0].get_text(strip=True)
        apartment = get_number(cells[1].get_text(strip=True))
        house = get_number(cells[2].get_text(strip=True))
        population = get_number(cells[3].get_text(strip=True)) if len(cells) > 3 else None

        communes.append({
            "link": href,
            "name": name,
            "apartment_m2": apartment,
            "house_m2": house,
            "population": population,
        })

    return communes


def extract_rent_prices(soup: BeautifulSoup) -> tuple[int | None, int | None]:
    """
    Extract rent prices per m2 from a commune/postal page.
    Looks for price values in the rent section (bold text with CHF).
    """
    rent_apt = None
    rent_house = None

    # Find all bold price-like elements
    bold_prices = soup.select("div.font-bold")
    price_values = []
    for el in bold_prices:
        text = el.get_text(strip=True)
        if "CHF" in text or re.match(r"[\d']+", text):
            val = get_number(text)
            if val and val < 200:  # Rent per m2 is typically < 200 CHF
                price_values.append(val)

    if len(price_values) >= 2:
        rent_apt = price_values[0]
        rent_house = price_values[1]
    elif len(price_values) == 1:
        rent_apt = price_values[0]

    return rent_apt, rent_house


def fetch_streets_graphql(link: str) -> list[dict]:
    """Fetch street-level price data via RealAdvisor's GraphQL endpoint."""
    slug = link.rstrip("/").split("/")[-1]

    query = """
    query streetPagesShowMoreQuery($slug: String!, $direction: order_by!) {
      places: ch_places(where: {slug: {_eq: $slug}}) {
        id
        street_stats_locality(
          where: {total_count: {_gte: 15}}
          order_by: {median_price_per_m2: $direction}
          limit: 100
        ) {
          route
          median_price_per_m2
          total_count
          lat
          lng
          slug
        }
      }
    }
    """

    payload = {
        "query": query,
        "variables": {"slug": slug, "direction": "desc"},
    }

    try:
        r = cffi_requests.post(
            GRAPHQL_URL, headers=GRAPHQL_HEADERS, json=payload,
            impersonate="chrome", timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            places = data.get("data", {}).get("places", [])
            if places:
                return places[0].get("street_stats_locality", [])
    except Exception as e:
        print(f"    GraphQL error for {slug}: {e}")

    return []


def get_streets(link: str) -> list[dict]:
    """Build street price list from GraphQL data."""
    raw = fetch_streets_graphql(link)
    streets = []
    for row in raw:
        median = row.get("median_price_per_m2")
        if median is None:
            continue
        streets.append({
            "street_name": row.get("route"),
            "avg_prices": {
                "appartement": {"sales_m2": round(median)},
                "maison": {"sales_m2": round(median)},
            },
        })
    return streets


def get_postal_codes_data(link: str, commune_avg_prices: dict) -> tuple[list, int | None, int | None]:
    """
    Get postal code breakdown for a commune.

    If the link itself contains a 4-digit postal code, it's a single-postal commune.
    Otherwise, follow the link to scrape the postal code table.

    Returns: (code_postaux_list, rent_apartment, rent_house)
    """
    postal = get_postal_number(link)

    # Single-postal commune: link already has the postal code
    if postal and re.search(r"\b\d{4}\b", link.split("/")[-1] if "/" in link else link):
        streets = get_streets(link)
        code_postaux = [{
            "no_postal": postal,
            "avg_prices": commune_avg_prices,
            "rues": streets,
        }]
        return code_postaux, None, None

    # Multi-postal commune: scrape the commune detail page
    full_url = link if link.startswith("http") else f"https://realadvisor.ch{link}"
    soup = fetch_html(full_url)
    if not soup:
        return [], None, None

    # Extract rent prices from the detail page
    rent_apt, rent_house = extract_rent_prices(soup)

    # Parse postal code table
    code_postaux = []
    for tr in soup.select("table tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        cell_link_el = cells[0].find("a")
        cell_link = cell_link_el.get("href", "") if cell_link_el else ""
        cell_name = cells[0].get_text(strip=True)
        cell_apt = get_number(cells[1].get_text(strip=True))
        cell_house = get_number(cells[2].get_text(strip=True))
        cell_postal = get_postal_number(cell_name)

        if not cell_postal:
            continue

        # Fetch rent prices for this postal code
        pc_rent_apt = None
        pc_rent_house = None
        if cell_link:
            pc_url = cell_link if cell_link.startswith("http") else f"https://realadvisor.ch{cell_link}"
            pc_soup = fetch_html(pc_url)
            if pc_soup:
                pc_rent_apt, pc_rent_house = extract_rent_prices(pc_soup)
            time.sleep(0.5)

        # Fetch street data
        streets = get_streets(cell_link) if cell_link else []

        entry = {
            "no_postal": cell_postal,
            "avg_prices": {
                "appartement": {
                    "sales_m2": cell_apt,
                    **({"rent_m2": pc_rent_apt} if pc_rent_apt else {}),
                },
                "maison": {
                    "sales_m2": cell_house,
                    **({"rent_m2": pc_rent_house} if pc_rent_house else {}),
                },
            },
            "rues": streets,
        }
        code_postaux.append(entry)
        time.sleep(0.3)

    return code_postaux, rent_apt, rent_house


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    lamap_url = os.environ.get("LAMAP_SUPABASE_URL", "")
    lamap_key = os.environ.get("LAMAP_SUPABASE_SERVICE_KEY", "")
    lamap_schema = os.environ.get("LAMAP_SCHEMA", "bronze")

    if not lamap_url or not lamap_key:
        print("ERROR: LAMAP_SUPABASE_URL and LAMAP_SUPABASE_SERVICE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  RealAdvisor Pipeline")
    print(f"  Source: {CANTON_URL}")
    print(f"  Target: {lamap_schema}.{TABLE}")
    print("=" * 60)

    # ── Row count BEFORE ──
    rows_before = get_row_count(lamap_url, lamap_key, lamap_schema, TABLE)
    print(f"  Rows before: {rows_before}" if rows_before is not None else "  Rows before: unknown")

    # ── Fetch canton page ──
    print(f"\n  Fetching canton page...")
    soup = fetch_html(CANTON_URL)
    if not soup:
        print("  ERROR: Could not fetch canton page")
        sys.exit(1)

    # ── Extract date ──
    year, month = extract_date_info(soup)
    print(f"  Data period: {month}/{year}")

    # ── Parse commune table ──
    communes = parse_commune_table(soup)
    print(f"  Communes found: {len(communes)}")

    if not communes:
        print("  ERROR: No communes found in table")
        sys.exit(1)

    # ── Process each commune ──
    records = []
    for i, commune in enumerate(communes):
        name = commune["name"]
        print(f"\n  [{i + 1}/{len(communes)}] {name}")

        avg_prices_base = {
            "appartement": {"sales_m2": commune["apartment_m2"]},
            "maison": {"sales_m2": commune["house_m2"]},
        }

        # Get postal code data (with rent prices + street data)
        code_postaux, rent_apt, rent_house = get_postal_codes_data(
            commune["link"], avg_prices_base
        )
        print(f"    Postal codes: {len(code_postaux)}, streets via GraphQL")

        # Build final avg_prices with rent data if available
        avg_prices = {
            "appartement": {
                "sales_m2": commune["apartment_m2"],
                **({"rent_m2": rent_apt} if rent_apt else {}),
            },
            "maison": {
                "sales_m2": commune["house_m2"],
                **({"rent_m2": rent_house} if rent_house else {}),
            },
        }

        provider_id = f"ge_{name.lower()}_{year}_{month}"

        record = {
            "providerId": provider_id,
            "canton": "Genève",
            "commune_name": name,
            "population": commune["population"],
            "currency": "CHF",
            "avg_prices": avg_prices,
            "year": str(year),
            "month": str(month),
            "code_postaux": code_postaux,
        }
        records.append(record)
        time.sleep(0.5)

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
    print(f"  Communes scraped: {len(communes)}")
    print(f"  Records upserted: {upserted}")
    print(f"  Rows before:      {rows_before}" if rows_before is not None else "  Rows before:      unknown")
    print(f"  Rows after:       {rows_after}" if rows_after is not None else "  Rows after:       unknown")
    if rows_before is not None and rows_after is not None:
        delta = rows_after - rows_before
        print(f"  Net new:          {delta}")
    print("=" * 60)

    if upserted == 0:
        print("  FAILED: Zero rows upserted!")
        sys.exit(1)


if __name__ == "__main__":
    main()
