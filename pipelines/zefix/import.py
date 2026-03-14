#!/usr/bin/env python3
"""
Zefix Swiss Company Registry — Bulk Import Pipeline

Downloads CSV exports of all 26 Swiss cantons from Basel Open Data
and upserts them into zefix_companies on one or more Supabase projects.

Source:  https://data-bs.ch/stata/zefix_handelsregister/all_cantons/
Destinations:
    - camelote_data (required) — command center database
    - lamap         (optional) — if LAMAP_SUPABASE_URL is set
    - yooneet       (optional) — if YOONEET_SUPABASE_URL is set

Each canton CSV is downloaded ONCE and upserted to ALL active destinations.

Environment variables (set by GitHub Actions secrets):
    SUPABASE_URL              - camelote_data project URL (required)
    SUPABASE_SERVICE_KEY      - camelote_data service_role key (required)
    SUPABASE_SCHEMA           - camelote_data schema (default: public)
    SUPABASE_TABLE            - camelote_data table (default: zefix_companies)

    LAMAP_SUPABASE_URL        - lamap project URL (optional)
    LAMAP_SUPABASE_SERVICE_KEY - lamap service_role key (optional)
    LAMAP_SCHEMA              - lamap schema (default: bronze)
    LAMAP_TABLE               - lamap table (default: zefix_companies)

    YOONEET_SUPABASE_URL        - yooneet project URL (optional)
    YOONEET_SUPABASE_SERVICE_KEY - yooneet service_role key (optional)
    YOONEET_SCHEMA              - yooneet schema (default: public)
    YOONEET_TABLE               - yooneet table (default: zefix_companies)
"""

import os
import sys
import csv
import io
import time
import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert

CSV_BASE_URL = "https://data-bs.ch/stata/zefix_handelsregister/all_cantons/companies_{}.csv"

ALL_CANTONS = [
    "AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR",
    "JU", "LU", "NE", "NW", "OW", "SG", "SH", "SO", "SZ", "TG",
    "TI", "UR", "VD", "VS", "ZG", "ZH",
]


def build_destinations():
    """
    Build list of destination Supabase projects from environment variables.
    camelote_data is always required. Others are optional.
    """
    destinations = []

    # Primary — always required
    destinations.append({
        "name": "camelote_data",
        "url": os.environ.get("SUPABASE_URL", ""),
        "key": os.environ.get("SUPABASE_SERVICE_KEY", ""),
        "schema": os.environ.get("SUPABASE_SCHEMA", "public"),
        "table": os.environ.get("SUPABASE_TABLE", "zefix_companies"),
    })

    # Secondary — lamap (optional)
    if os.environ.get("LAMAP_SUPABASE_URL"):
        destinations.append({
            "name": "lamap",
            "url": os.environ["LAMAP_SUPABASE_URL"],
            "key": os.environ["LAMAP_SUPABASE_SERVICE_KEY"],
            "schema": os.environ.get("LAMAP_SCHEMA", "bronze"),
            "table": os.environ.get("LAMAP_TABLE", "zefix_companies"),
        })

    # Secondary — yooneet (optional)
    if os.environ.get("YOONEET_SUPABASE_URL"):
        destinations.append({
            "name": "yooneet",
            "url": os.environ["YOONEET_SUPABASE_URL"],
            "key": os.environ["YOONEET_SUPABASE_SERVICE_KEY"],
            "schema": os.environ.get("YOONEET_SCHEMA", "public"),
            "table": os.environ.get("YOONEET_TABLE", "zefix_companies"),
        })

    return destinations


def download_csv(canton):
    """Download CSV for one canton. Forces UTF-8 to avoid mojibake."""
    url = CSV_BASE_URL.format(canton)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    r.encoding = "utf-8"  # CRITICAL: prevents "SociÃ©tÃ©" instead of "Société"
    return r.text


def format_uid(raw):
    """Convert raw UID digits to CHE-XXX.XXX.XXX format."""
    if not raw:
        return None, None
    digits = "".join(c for c in str(raw) if c.isdigit())
    if len(digits) >= 9:
        digits = digits[:9]
        formatted = f"CHE-{digits[:3]}.{digits[3:6]}.{digits[6:9]}"
        return formatted, int(digits)
    return raw, None


def parse_csv(csv_text, canton):
    """Parse CSV into list of dicts matching zefix_companies schema."""
    reader = csv.DictReader(io.StringIO(csv_text))
    records = []

    for row in reader:
        name = (row.get("company_legal_name") or "").strip()
        uid_raw = (row.get("company_uid") or "").strip()
        legal_form = (row.get("company_type_fr") or row.get("company_type_de") or "").strip()
        city = (row.get("locality") or row.get("municipality") or "").strip()

        if not name:
            continue

        uid_formatted, uid_num = format_uid(uid_raw)

        records.append({
            "uid": uid_formatted,
            "uid_raw": uid_num,
            "name": name,
            "legal_form": legal_form,
            "status": "ACTIVE",
            "city": city,
            "canton": canton,
            "source": "csv_import",
        })

    return records


def upsert_to_destinations(destinations, records, canton):
    """
    Upsert parsed records to all destinations.
    Returns dict of {dest_name: rows_upserted}.
    """
    results = {}
    for dest in destinations:
        print(f"  → {dest['name']} ({dest['schema']}.{dest['table']})")
        upserted = batch_upsert(
            url=dest["url"],
            key=dest["key"],
            table=dest["table"],
            records=records,
            conflict_column="uid",
            schema=dest["schema"],
            batch_size=500,
        )
        results[dest["name"]] = upserted
        print(f"    Upserted: {upserted}/{len(records)} rows")
    return results


def import_canton(canton, destinations):
    """Download + parse + upsert one canton to all destinations."""
    print(f"\n{'='*50}")
    print(f"  Canton: {canton}")
    print(f"{'='*50}")

    # Download (once)
    try:
        csv_text = download_csv(canton)
    except Exception as e:
        print(f"  DOWNLOAD FAILED: {e}")
        return None  # signal failure

    # Parse (once)
    records = parse_csv(csv_text, canton)
    print(f"  Downloaded: {len(records)} rows")

    if not records:
        print("  No records to import, skipping")
        return {}

    # Upsert to all destinations
    results = upsert_to_destinations(destinations, records, canton)
    return results


def main():
    # Build destinations from env vars
    destinations = build_destinations()

    # Validate primary destination
    primary = destinations[0]
    if not primary["url"] or not primary["key"]:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables are required")
        sys.exit(1)

    dest_names = [d["name"] for d in destinations]

    print("=" * 50)
    print("  Zefix Import Pipeline")
    print(f"  Destinations: {', '.join(dest_names)}")
    print(f"  Cantons: ALL ({len(ALL_CANTONS)})")
    print("=" * 50)

    # Track totals per destination
    totals = {d["name"]: 0 for d in destinations}
    failed_cantons = []

    for canton in ALL_CANTONS:
        results = import_canton(canton, destinations)
        if results is None:
            failed_cantons.append(canton)
        else:
            for dest_name, count in results.items():
                totals[dest_name] += count
        time.sleep(0.5)  # rate limiting between cantons

    # Summary
    print("\n" + "=" * 50)
    print("  IMPORT COMPLETE")
    for dest_name, total in totals.items():
        print(f"  {dest_name}: {total:,} companies upserted")
    print(f"  Cantons OK: {len(ALL_CANTONS) - len(failed_cantons)}/{len(ALL_CANTONS)}")

    if failed_cantons:
        print(f"  FAILED cantons: {', '.join(failed_cantons)}")
        print("=" * 50)
        sys.exit(1)

    print("=" * 50)


if __name__ == "__main__":
    main()
