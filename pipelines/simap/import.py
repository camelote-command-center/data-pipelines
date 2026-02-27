#!/usr/bin/env python3
"""
Simap Swiss Public Procurement — Import Pipeline

Fetches public tender publications from the simap.ch REST API and upserts
them into lamap_db's bronze."Simap" table.

This is a clean Python rewrite of LamapParser/parsers/simap.js.  It preserves
the same API endpoints, field mapping, JSON column serialisation, and
composite unique key (project_number + publication_number).

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.
    - Row count should only go UP or stay the same.

Source:  https://www.simap.ch/
Table:   bronze."Simap"
Key:     (project_number, publication_number)

Environment variables:
    LAMAP_SUPABASE_URL          - Lamap Supabase project URL (required)
    LAMAP_SUPABASE_SERVICE_KEY  - service_role key (required)
    LAMAP_SCHEMA                - target schema (default: bronze)
"""

import os
import sys
import re
import json
import time
import html
from datetime import datetime, timedelta, timezone

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert

# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

TABLE_NAME = "Simap"  # case-sensitive, matches existing table
CONFLICT_COLUMNS = "project_number,publication_number"

SEARCH_URL = "https://www.simap.ch/rest/publications/v2/project/project-search"
DETAIL_URL = "https://www.simap.ch/rest/publications/v1/project/{project_id}/publication-details/{publication_id}"
COOKIE_URL = "https://www.simap.ch/en?time=%22month%22"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
)

MAX_RETRIES = 5
RETRY_BACKOFF = 2  # seconds


# ──────────────────────────────────────────────────────────────
# Helpers — ported from simap.js
# ──────────────────────────────────────────────────────────────

def any_value(obj):
    """Return the French value if available, else first non-empty value.
    Mirrors JS anyValue(): prefers obj.fr, falls back to first truthy value."""
    if not obj or not isinstance(obj, dict):
        return None
    if obj.get("fr"):
        return re.sub(r"\s+", " ", obj["fr"]).strip()
    for v in obj.values():
        if v:
            return re.sub(r"\s+", " ", str(v)).strip()
    return None


def replace_underscore(s):
    return s.replace("_", " ") if s else None


def format_order_address(a):
    if not a:
        return None
    parts = [a.get("postalCode"), any_value(a.get("city")), a.get("cantonId"), a.get("countryId")]
    return ", ".join(str(p) for p in parts if p) or None


def format_other_address(a):
    if not a:
        return None
    parts = [a.get("street"), a.get("postalCode"), a.get("city"), a.get("cantonId"), a.get("countryId")]
    return ", ".join(str(p).strip() for p in parts if p) or None


def strip_html(html_str):
    """Strip HTML tags and normalise whitespace. Mirrors stripHtmlWithRegex()."""
    if not html_str:
        return None
    text = re.sub(r"<[^>]*>", ". ", html_str)
    text = text.replace("&nbsp;", " ")
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    # Clean up artefact from splitting on tags
    text = re.sub(r"(\.\s*)+", ". ", text).strip(". ")
    return text or None


def format_datetime(s):
    """If ISO-8601 datetime, format as YYYY-MM-DD, HH:mm. Otherwise return as-is."""
    if not s:
        return s
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d, %H:%M")
    except (ValueError, TypeError):
        return s


def format_proc_address(item):
    """Format procurement office / recipient address. Mirrors formatProcAddress()."""
    if not item:
        return None
    name = any_value(item.get("name"))
    if not name:
        return None

    parts = [
        any_value(item.get("street")),
        item.get("postalCode"),
        any_value(item.get("city")),
        item.get("cantonId"),
        item.get("countryId"),
    ]
    address = ", ".join(str(p).strip() for p in parts if p) or None

    return {
        "name": name,
        "contact_person": any_value(item.get("contactPerson")),
        "address": address,
        "phone": item.get("phone"),
        "email": item.get("email"),
        "url": any_value(item.get("url")),
    }


def format_vendors(vendor_list):
    """Format vendor/award data. Mirrors getFormatVendors()."""
    if not vendor_list:
        return []
    result = []
    for v in vendor_list:
        vendor_name = v.get("vendorName")
        if not vendor_name:
            continue
        va = v.get("vendorAddress")
        price_data = v.get("price")
        entry = {
            "name": vendor_name,
            "address": format_other_address(va) if va else None,
            "price": (
                {
                    "currency": price_data.get("currency"),
                    "price": price_data.get("price"),
                    "vat_type": price_data.get("vatType"),
                }
                if price_data
                else None
            ),
            "rank": v.get("rank"),
        }
        result.append(entry)
    return result


# ──────────────────────────────────────────────────────────────
# API calls
# ──────────────────────────────────────────────────────────────

def _get(url, headers=None, retry=0):
    """GET with retry logic. Mirrors JS get()."""
    try:
        r = requests.get(url, headers=headers or {}, timeout=60)
        r.raise_for_status()
        return r
    except Exception as e:
        if retry >= MAX_RETRIES:
            raise
        wait = RETRY_BACKOFF ** (retry + 1)
        print(f"    Request error ({e}), retrying in {wait}s (attempt {retry + 1}/{MAX_RETRIES})")
        time.sleep(wait)
        return _get(url, headers=headers, retry=retry + 1)


def get_cookies():
    """Hit the Simap homepage to get a session cookie."""
    r = _get(COOKIE_URL, headers={"User-Agent": USER_AGENT})
    cookies = r.headers.get("set-cookie", "")
    # requests may return multiple set-cookie values joined
    return cookies


def search_projects(session_cookies):
    """
    Search for projects published in the last month.
    Mirrors getData() from simap.js (recurring version).
    """
    one_month_ago = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    url = f"{SEARCH_URL}?orderAddressCountryOnlySwitzerland=false&newestPublicationFrom={one_month_ago}"

    headers = {
        "User-Agent": USER_AGENT,
        "Cookie": session_cookies,
    }

    r = _get(url, headers=headers)
    data = r.json()
    return data.get("projects", [])


def get_publication_details(project_id, publication_id):
    """Fetch full publication details for one project. Mirrors getRecordData()."""
    url = DETAIL_URL.format(project_id=project_id, publication_id=publication_id)
    r = _get(url)
    return r.json()


# ──────────────────────────────────────────────────────────────
# Parse — exact port of formatData() from simap.js
# ──────────────────────────────────────────────────────────────

def parse_publication(raw):
    """
    Transform raw publication-detail JSON into a flat dict matching the
    bronze."Simap" column structure.  Mirrors formatData() from simap.js.
    """
    base = raw.get("base") or {}
    decision = raw.get("decision") or {}
    procurement = raw.get("procurement") or {}
    project_info = raw.get("project-info") or {}
    dates = raw.get("dates") or {}

    pd = {}

    # ── base ──
    pd["project_number"] = base.get("projectNumber")
    pd["title"] = any_value(base.get("title"))
    pd["publication_number"] = base.get("publicationNumber")
    pd["publication_date"] = base.get("publicationDate")
    pd["type"] = replace_underscore(base.get("type"))
    pd["contract_type"] = base.get("orderType")

    # ── decision ──
    if decision:
        pd["vendors"] = format_vendors(decision.get("vendors"))
        pd["total_price_selection"] = replace_underscore(decision.get("totalPriceSelection"))
        pd["number_of_submissions"] = decision.get("numberOfSubmissions")
        pd["award_decision_date"] = decision.get("awardDecisionDate")

    # ── procurement ──
    if procurement:
        oa = procurement.get("orderAddress") or {}
        pd["order_address"] = format_order_address(oa)
        pd["no_postal"] = oa.get("postalCode")

        canton = oa.get("cantonId")
        pd["canton"] = canton if canton and canton != "CH" else None

        contract_type = procurement.get("orderType")
        type_of_contract_type = None
        if contract_type:
            type_of_contract_type = procurement.get(f"{contract_type}Type")

        if not pd.get("contract_type"):
            pd["contract_type"] = contract_type

        pd["order_description"] = strip_html(any_value(procurement.get("orderDescription")))

        contract_period = (procurement.get("contractPeriod") or {}).get("dateRange")
        contract_days = procurement.get("contractDays")
        if contract_period and isinstance(contract_period, list):
            pd["contract_duration"] = " - ".join(str(d) for d in contract_period)
        elif contract_days:
            pd["contract_duration"] = f"{contract_days} days"

        pd["can_contract_be_extended"] = procurement.get("canContractBeExtended") or None

        # CPV codes
        cpv = []
        cpv_code = procurement.get("cpvCode")
        if cpv_code:
            cpv.append({"code": cpv_code.get("code"), "text": any_value(cpv_code.get("label"))})
        additional = procurement.get("additionalCpvCodes") or []
        for c in additional:
            cpv.append({"code": c.get("code"), "text": any_value(c.get("label"))})
        pd["cpv"] = cpv

        pd["type_of_contract_type"] = replace_underscore(type_of_contract_type)

    # ── dates ──
    if dates:
        pd["offer_deadline"] = format_datetime(dates.get("offerDeadline"))
        validity_date = dates.get("offerValidityDeadlineDate")
        validity_days = dates.get("offerValidityDeadlineDays")
        if validity_date:
            pd["offer_validity_deadline"] = validity_date
        elif validity_days:
            pd["offer_validity_deadline"] = f"{validity_days} days"

    # ── project-info ──
    if project_info:
        pd["procurement_office"] = format_proc_address(project_info.get("procOfficeAddress"))
        pd["procurement_recipient"] = format_proc_address(project_info.get("procurementRecipientAddress"))

    return pd


# ──────────────────────────────────────────────────────────────
# Serialise JSON columns for PostgREST upsert
# ──────────────────────────────────────────────────────────────

def serialise_record(record):
    """
    Convert Python dicts/lists in JSON columns to JSON strings,
    exactly as the JS version does before insert.
    Also strip any None-valued keys to let DB defaults apply.
    """
    out = {}
    for k, v in record.items():
        if v is None:
            continue  # let DB default/null apply
        if k in ("cpv", "vendors", "procurement_office", "procurement_recipient"):
            if isinstance(v, (list, dict)):
                # Empty list/dict → null
                if not v:
                    continue
                out[k] = json.dumps(v)
            else:
                out[k] = v
        else:
            out[k] = v
    return out


# ──────────────────────────────────────────────────────────────
# Row count helper (for safety logging)
# ──────────────────────────────────────────────────────────────

def get_row_count(url, key, schema, table):
    """GET current row count from the table via PostgREST HEAD request."""
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}?select=count"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Prefer": "count=exact",
    }
    if schema and schema != "public":
        headers["Accept-Profile"] = schema
    try:
        r = requests.head(endpoint, headers=headers, timeout=15)
        # Count is in the content-range header: "0-N/TOTAL"
        cr = r.headers.get("content-range", "")
        if "/" in cr:
            return int(cr.split("/")[1])
    except Exception as e:
        print(f"  Warning: could not get row count: {e}")
    return None


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    url = os.environ.get("LAMAP_SUPABASE_URL", "")
    key = os.environ.get("LAMAP_SUPABASE_SERVICE_KEY", "")
    schema = os.environ.get("LAMAP_SCHEMA", "bronze")

    if not url or not key:
        print("ERROR: LAMAP_SUPABASE_URL and LAMAP_SUPABASE_SERVICE_KEY are required")
        sys.exit(1)

    print("=" * 55)
    print("  Simap Import Pipeline")
    print(f"  Destination: lamap_db ({schema}.{TABLE_NAME})")
    print("=" * 55)

    # ── Safety: count rows BEFORE import ──
    rows_before = get_row_count(url, key, schema, TABLE_NAME)
    print(f"\n  Rows before import: {rows_before or 'unknown'}")

    # ── Step 1: Get session cookies ──
    print("\n  Fetching session cookies...")
    cookies = get_cookies()

    # ── Step 2: Search projects from last month ──
    print("  Searching projects (last 30 days)...")
    projects = search_projects(cookies)
    print(f"  Found: {len(projects)} projects")

    if not projects:
        print("  No projects found. Exiting.")
        return

    # ── Step 3: Fetch details + parse for each project ──
    records = []
    errors = 0

    for i, proj in enumerate(projects):
        project_id = proj.get("id")
        publication_id = proj.get("publicationId")

        if not project_id or not publication_id:
            continue

        if (i + 1) % 100 == 0 or i == 0:
            print(f"  Fetching details: {i + 1}/{len(projects)}...")

        try:
            raw = get_publication_details(project_id, publication_id)
            parsed = parse_publication(raw)

            # Add the URL field (same as JS version)
            parsed["url"] = f"https://www.simap.ch/en/project-detail/{project_id}"

            # Skip records without a unique key
            if not parsed.get("project_number") or not parsed.get("publication_number"):
                continue

            record = serialise_record(parsed)
            records.append(record)

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"    Error on project {project_id}: {e}")
            elif errors == 6:
                print("    (suppressing further error messages)")

        # Rate limiting — be respectful to simap.ch
        time.sleep(0.3)

    print(f"\n  Parsed: {len(records)} records ({errors} errors)")

    if not records:
        print("  No records to upsert. Exiting.")
        return

    # ── Step 4: Upsert to lamap_db ──
    print(f"\n  Upserting to {schema}.{TABLE_NAME}...")
    upserted = batch_upsert(
        url=url,
        key=key,
        table=TABLE_NAME,
        records=records,
        conflict_column=CONFLICT_COLUMNS,
        schema=schema,
        batch_size=200,
    )

    # ── Safety: count rows AFTER import ──
    rows_after = get_row_count(url, key, schema, TABLE_NAME)

    print("\n" + "=" * 55)
    print("  IMPORT COMPLETE")
    print(f"  Records fetched:     {len(records)}")
    print(f"  Records upserted:    {upserted}")
    print(f"  Rows before import:  {rows_before or 'unknown'}")
    print(f"  Rows after import:   {rows_after or 'unknown'}")
    if rows_before is not None and rows_after is not None:
        delta = rows_after - rows_before
        print(f"  Net new rows:        {delta}")
        if rows_after < rows_before:
            print("  WARNING: Row count decreased! This should never happen.")
            sys.exit(1)
    print(f"  Fetch errors:        {errors}")
    print("=" * 55)


if __name__ == "__main__":
    main()
