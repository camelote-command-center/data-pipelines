#!/usr/bin/env python3
"""
Zefix Swiss Company Registry — Bulk Import Pipeline

Downloads CSV exports of all 26 Swiss cantons from Basel Open Data
and upserts them into re-LLM bronze_ch.zefix_companies.

Source:  https://data-bs.ch/stata/zefix_handelsregister/all_cantons/
Destination: re-LLM (znrvddgmczdqoucmykij), schema bronze_ch, table zefix_companies

Environment variables (set by GitHub Actions secrets):
    RE_LLM_SUPABASE_URL              - re-LLM project URL (required)
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY - re-LLM service_role key (required)
    RE_LLM_SCHEMA                    - schema (default: bronze_ch)
    RE_LLM_TABLE                     - table (default: zefix_companies)
"""

import os
import sys
import csv
import io
import time
import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert

CSV_BASE_URL = "https://data-bs.ch/stata/zefix_handelsregister/all_cantons/companies_{}.csv"

ALL_CANTONS = [
    "AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR",
    "JU", "LU", "NE", "NW", "OW", "SG", "SH", "SO", "SZ", "TG",
    "TI", "UR", "VD", "VS", "ZG", "ZH",
]


def build_destination():
    """Single re-LLM destination from environment variables."""
    url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    if not url or not key:
        print(
            "ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY "
            "environment variables are required"
        )
        sys.exit(1)
    return {
        "name": "re-LLM",
        "url": url,
        "key": key,
        "schema": os.environ.get("RE_LLM_SCHEMA", "bronze_ch"),
        "table": os.environ.get("RE_LLM_TABLE", "zefix_companies"),
    }


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


def upsert_to_destination(dest, records):
    """Upsert parsed records to the destination. Returns rows upserted."""
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
    print(f"    Upserted: {upserted}/{len(records)} rows")
    return upserted


def import_canton(canton, dest):
    """Download + parse + upsert one canton."""
    print(f"\n{'='*50}")
    print(f"  Canton: {canton}")
    print(f"{'='*50}")

    try:
        csv_text = download_csv(canton)
    except Exception as e:
        print(f"  DOWNLOAD FAILED: {e}")
        return None

    records = parse_csv(csv_text, canton)
    print(f"  Downloaded: {len(records)} rows")

    if not records:
        print("  No records to import, skipping")
        return 0

    return upsert_to_destination(dest, records)


def main():
    dest = build_destination()

    print("=" * 50)
    print("  Zefix Import Pipeline")
    print(f"  Destination: {dest['name']} ({dest['schema']}.{dest['table']})")
    print(f"  Cantons: ALL ({len(ALL_CANTONS)})")
    print("=" * 50)

    total = 0
    failed_cantons = []

    for canton in ALL_CANTONS:
        count = import_canton(canton, dest)
        if count is None:
            failed_cantons.append(canton)
        else:
            total += count
        time.sleep(0.5)

    print("\n" + "=" * 50)
    print("  IMPORT COMPLETE")
    print(f"  {dest['name']}: {total:,} companies upserted")
    print(f"  Cantons OK: {len(ALL_CANTONS) - len(failed_cantons)}/{len(ALL_CANTONS)}")

    if failed_cantons:
        print(f"  FAILED cantons: {', '.join(failed_cantons)}")
        print("=" * 50)
        sys.exit(1)

    print("=" * 50)


if __name__ == "__main__":
    main()
