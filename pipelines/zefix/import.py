#!/usr/bin/env python3
"""
Zefix Swiss Company Registry — Bulk Import Pipeline

Downloads CSV exports of all 26 Swiss cantons from Basel Open Data
and upserts them into the zefix_companies table on Supabase.

Source:  https://data-bs.ch/stata/zefix_handelsregister/all_cantons/
Target:  camelote_data Supabase → public.zefix_companies
Schedule: 1st of every month via GitHub Actions

Environment variables (set by GitHub Actions secrets):
    SUPABASE_URL          - camelote_data project URL
    SUPABASE_SERVICE_KEY  - camelote_data service_role key
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


def import_canton(canton):
    """Download + parse + upsert one canton. Returns rows upserted."""
    print(f"\n{'='*50}")
    print(f"  Canton: {canton}")
    print(f"{'='*50}")

    # Download
    try:
        csv_text = download_csv(canton)
    except Exception as e:
        print(f"  DOWNLOAD FAILED: {e}")
        return -1  # signal failure

    # Parse
    records = parse_csv(csv_text, canton)
    print(f"  Downloaded: {len(records)} rows")

    if not records:
        print("  No records to import, skipping")
        return 0

    # Upsert via shared client
    upserted = batch_upsert(
        table="zefix_companies",
        records=records,
        conflict_column="uid",
        batch_size=500,
    )

    print(f"  Upserted:   {upserted}/{len(records)} rows")
    return upserted


def main():
    # Validate environment
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY", "")

    if not supabase_url or not supabase_key:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables are required")
        sys.exit(1)

    print("=" * 50)
    print("  Zefix Import Pipeline")
    print(f"  Target: camelote_data (zefix_companies)")
    print(f"  Cantons: ALL ({len(ALL_CANTONS)})")
    print("=" * 50)

    total_upserted = 0
    failed_cantons = []

    for canton in ALL_CANTONS:
        result = import_canton(canton)
        if result < 0:
            failed_cantons.append(canton)
        else:
            total_upserted += result
        time.sleep(0.5)  # rate limiting between cantons

    # Summary
    print("\n" + "=" * 50)
    print("  IMPORT COMPLETE")
    print(f"  Total upserted: {total_upserted:,} companies")
    print(f"  Cantons OK:     {len(ALL_CANTONS) - len(failed_cantons)}/{len(ALL_CANTONS)}")

    if failed_cantons:
        print(f"  FAILED cantons: {', '.join(failed_cantons)}")
        print("=" * 50)
        sys.exit(1)

    print("=" * 50)


if __name__ == "__main__":
    main()
