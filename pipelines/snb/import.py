#!/usr/bin/env python3
"""
SNB / BWO — Reference rate + property price index ingestion.

Writes 3 bronze tables in re-LLM (bronze_ch):
  - snb_price_index        ← SNB Data Portal cubes plimoinchq + plimoinreg
  - snb_taux_reference     ← BWO reference-rate table (Nuxt SPA)
  - ofl_taux_reference     ← same source, dual-write for backward compat

NOTE: snb_mortgage_rates is intentionally excluded — its source cube ID is
unknown; tracked as P2 bug a57a2d20-8d02-483f-8cbe-c59dbfa70fea.

Idempotency: ON CONFLICT upsert via PostgREST (resolution=merge-duplicates).
Schedule: quarterly (workflow cron).
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, datetime

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert  # noqa: E402

# ──────────────────────────────────────────────────────────────
# Sources
# ──────────────────────────────────────────────────────────────

SNB_CUBE_URL = "https://data.snb.ch/api/cube/{cube_id}/data/csv/en"
BWO_REF_RATE_URL = (
    "https://www.bwo.admin.ch/fr/"
    "evolution-du-taux-de-reference-et-du-taux-dinteret-moyen"
)

# plimoinchq dimensions: D0=property_type, D1=measure
CUBE_PLIMOINCHQ = "plimoinchq"
PLIMOINCHQ_NATIONAL_REGION = "CH"

# plimoinreg dimensions: D0=property_type, D1=region, D2=measure
CUBE_PLIMOINREG = "plimoinreg"

# D0 property type → semantic slug (matches legacy snb_price_index naming)
PROPERTY_SLUGS = {
    "EW": "apartments",
    "EH": "houses",
    "MH": "apartment_buildings",
    "MW": "rental_housing",
    "BF": "office_space",
    "GF": "industrial_commercial",
    "VF": "retail_space",
}

# plimoinchq D1 → (measure_slug, provider_slug)
CHQ_MEASURE_PROVIDER = {
    "AP": ("asking", "wuest"),
    "TP": ("transaction", "wuest"),
    "TP1": ("transaction", "fpre"),
    "TP2": ("transaction", "iazi"),
    "TP3": ("transaction", "bfs"),
}

# plimoinreg D2 → measure slug
REG_MEASURE = {"A": "asking", "T": "transaction"}

# plimoinreg D1 region code → human name (per SNB cube metadata)
REGION_NAMES = {
    "GS": "Total Switzerland",
    "RZ": "Zurich region",
    "RO": "Eastern Switzerland",
    "RI": "Central Switzerland",
    "RN": "Northwestern Switzerland",
    "RB0": "Berne region",
    "RS": "Southern Switzerland",
    "RG0": "Lake Geneva region",
    "RW": "Western Switzerland",
    "RB1": "Basel region",
    "RG1": "Geneva region",
    "US": "Other regions of Switzerland",
}


# ──────────────────────────────────────────────────────────────
# SNB cube fetch
# ──────────────────────────────────────────────────────────────

def _fetch_snb_csv(cube_id: str) -> list[list[str]]:
    """Fetch SNB CSV. Returns list of data rows (after header)."""
    url = SNB_CUBE_URL.format(cube_id=cube_id)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    text = r.text.lstrip("﻿")
    lines = text.splitlines()

    # Find data header line (starts with "Date")
    data_start = None
    for i, line in enumerate(lines):
        if line.startswith('"Date"'):
            data_start = i + 1
            break
    if data_start is None:
        raise RuntimeError(f"No data header in cube {cube_id}")

    rows = []
    for line in lines[data_start:]:
        line = line.strip()
        if not line:
            continue
        # Strip quotes from each field
        cells = [c.strip().strip('"') for c in line.split(";")]
        rows.append(cells)
    return rows


def fetch_plimoinchq() -> list[dict]:
    """National quarterly index. Returns rows for snb_price_index.

    property_type slug: "{prop}_{measure}_{provider}"  e.g. apartments_transaction_fpre
    """
    raw = _fetch_snb_csv(CUBE_PLIMOINCHQ)
    out = []
    for cells in raw:
        if len(cells) < 4:
            continue
        period, d0, d1, value = cells[0], cells[1], cells[2], cells[3]
        if not value:
            continue
        try:
            iv = float(value)
        except ValueError:
            continue
        prop = PROPERTY_SLUGS.get(d0)
        mp = CHQ_MEASURE_PROVIDER.get(d1)
        if not prop or not mp:
            continue
        measure, provider = mp
        out.append({
            "cube_id": CUBE_PLIMOINCHQ,
            "period": period,
            "property_type": f"{prop}_{measure}_{provider}",
            "region_code": PLIMOINCHQ_NATIONAL_REGION,
            "region_name": "Switzerland",
            "index_value": iv,
            "base_year": "2020",
        })
    return out


def fetch_plimoinreg() -> list[dict]:
    """Regional annual index. Returns rows for snb_price_index.

    property_type slug: "{prop}_{measure}"  e.g. apartments_transaction
    region_code: SNB cube region code (GS/RZ/RO/RI/RN/RB0/RS/RG0/RW/RB1/RG1/US).
    NOTE: Legacy data uses a different region taxonomy (CH/R1/R3..R7/RB0/RW).
    New rows coexist alongside legacy rows under different region_code keys.
    """
    raw = _fetch_snb_csv(CUBE_PLIMOINREG)
    out = []
    for cells in raw:
        if len(cells) < 5:
            continue
        period, d0, d1, d2, value = cells[0], cells[1], cells[2], cells[3], cells[4]
        if not value:
            continue
        try:
            iv = float(value)
        except ValueError:
            continue
        prop = PROPERTY_SLUGS.get(d0)
        measure = REG_MEASURE.get(d2)
        if not prop or not measure:
            continue
        out.append({
            "cube_id": CUBE_PLIMOINREG,
            "period": period,
            "property_type": f"{prop}_{measure}",
            "region_code": d1,
            "region_name": REGION_NAMES.get(d1, d1),
            "index_value": iv,
            "base_year": "2020",
        })
    return out


# ──────────────────────────────────────────────────────────────
# BWO reference rate
# ──────────────────────────────────────────────────────────────

_PCT_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*%")
_DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
_TAG_RE = re.compile(r"<[^>]+>")


def _strip_tags(s: str) -> str:
    return _TAG_RE.sub("", s).strip()


def _parse_pct(s: str) -> float | None:
    s = _strip_tags(s)
    m = _PCT_RE.search(s)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def _parse_date(s: str) -> date | None:
    s = _strip_tags(s)
    m = _DATE_RE.search(s)
    if not m:
        return None
    return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))


def fetch_bwo_reference_rate() -> tuple[list[dict], list[dict]]:
    """Scrape BWO Nuxt SPA. Returns (snb_taux_reference_rows, ofl_taux_reference_rows)."""
    r = requests.get(BWO_REF_RATE_URL, timeout=60)
    r.raise_for_status()
    html = r.text

    m = re.search(
        r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>', html, re.S
    )
    if not m:
        raise RuntimeError("No __NUXT_DATA__ in BWO page")
    data = json.loads(m.group(1))

    # Find the table component: dict with bodyRows + headerColumns where
    # one header text is "Taux d'intérêt de référence applicable aux contrats de bail".
    table_idx = None
    for i, x in enumerate(data):
        if not isinstance(x, dict):
            continue
        if "bodyRows" not in x or "headerColumns" not in x:
            continue
        try:
            hdr_refs = data[x["headerColumns"]]
            hdr_texts = []
            for href in hdr_refs:
                col = data[href]
                hdr_texts.append(data[col["text"]])
            if any(
                isinstance(t, str) and "Taux" in t and "référence" in t
                for t in hdr_texts
            ):
                table_idx = i
                break
        except Exception:
            continue

    if table_idx is None:
        raise RuntimeError("BWO reference-rate table not found in Nuxt data")

    table = data[table_idx]
    body_row_refs = data[table["bodyRows"]]

    # Column order: [taux_ref, valable_dès_le, taux_moyen, jour_de_référence]
    snb_rows: list[dict] = []
    ofl_rows: list[dict] = []
    seen_dates: set[date] = set()

    for row_ref in body_row_refs:
        cell_refs = data[row_ref]
        if len(cell_refs) != 4:
            continue
        try:
            cells_text = []
            for cref in cell_refs:
                cell = data[cref]
                content_refs = data[cell["cellContent"]]
                texts = []
                for tref in content_refs:
                    text_obj = data[tref]
                    if isinstance(text_obj, dict) and "text" in text_obj:
                        texts.append(data[text_obj["text"]])
                cells_text.append(" ".join(t for t in texts if isinstance(t, str)))
        except Exception:
            continue

        if len(cells_text) != 4:
            continue

        taux_ref_pct = _parse_pct(cells_text[0])
        valid_from = _parse_date(cells_text[1])
        taux_moyen_pct = _parse_pct(cells_text[2])
        survey_date = _parse_date(cells_text[3])

        if taux_ref_pct is None or valid_from is None:
            continue
        if valid_from in seen_dates:
            continue
        seen_dates.add(valid_from)

        snb_rows.append({
            "date_effet": valid_from.isoformat(),
            "taux_reference": taux_ref_pct,
            "source": BWO_REF_RATE_URL,
        })
        ofl_rows.append({
            "valid_from": valid_from.isoformat(),
            "taux_reference": taux_ref_pct,
            "taux_moyen": taux_moyen_pct,
            "source_url": BWO_REF_RATE_URL,
            "notes": (
                f"survey_date={survey_date.isoformat()}" if survey_date else None
            ),
        })

    # Sort newest-first; compute valid_until in ofl as next-newer valid_from - 1d
    ofl_rows.sort(key=lambda r: r["valid_from"], reverse=True)
    for i in range(len(ofl_rows) - 1):
        nxt = datetime.fromisoformat(ofl_rows[i]["valid_from"]).date()
        prev = datetime.fromisoformat(ofl_rows[i + 1]["valid_from"]).date()
        # Older row's valid_until = (newer row's valid_from - 1 day)
        ofl_rows[i + 1]["valid_until"] = None  # set below
    # Set valid_until on each older row
    for i in range(len(ofl_rows)):
        if i == 0:
            ofl_rows[i]["valid_until"] = None  # current rate, open-ended
        else:
            newer = datetime.fromisoformat(ofl_rows[i - 1]["valid_from"]).date()
            ofl_rows[i]["valid_until"] = newer.isoformat()

    # Compute variation in snb_taux_reference (vs previous)
    snb_rows.sort(key=lambda r: r["date_effet"])
    prev_rate = None
    for row in snb_rows:
        if prev_rate is None:
            row["variation"] = None
        else:
            row["variation"] = round(row["taux_reference"] - prev_rate, 4)
        prev_rate = row["taux_reference"]

    return snb_rows, ofl_rows


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def _required(name: str) -> str:
    v = os.environ.get(name, "")
    if not v:
        print(f"ERROR: env var {name} required")
        sys.exit(1)
    return v


def main():
    url = _required("RE_LLM_SUPABASE_URL")
    key = _required("RE_LLM_SUPABASE_SERVICE_ROLE_KEY")
    schema = os.environ.get("RE_LLM_SCHEMA", "bronze_ch")

    print("=" * 60)
    print("  SNB / BWO Import")
    print(f"  Target: {schema} on {url}")
    print("=" * 60)

    # ── snb_price_index (national + regional) ─────────────────
    print("\n[snb_price_index] Fetching plimoinchq …")
    chq = fetch_plimoinchq()
    print(f"  national rows: {len(chq):,}")

    print("[snb_price_index] Fetching plimoinreg …")
    reg = fetch_plimoinreg()
    print(f"  regional rows: {len(reg):,}")

    price_rows = chq + reg
    print(f"[snb_price_index] Upserting {len(price_rows):,} rows …")
    n = batch_upsert(
        url=url, key=key, table="snb_price_index",
        records=price_rows,
        conflict_column="cube_id,period,property_type,region_code",
        schema=schema, batch_size=500,
    )
    print(f"  upserted: {n:,}")

    # ── BWO reference rate (dual-write) ───────────────────────
    print("\n[BWO] Fetching reference-rate table …")
    snb_ref, ofl_ref = fetch_bwo_reference_rate()
    print(f"  snb_taux_reference rows: {len(snb_ref)}")
    print(f"  ofl_taux_reference rows: {len(ofl_ref)}")

    if snb_ref:
        print("[snb_taux_reference] Upserting …")
        n = batch_upsert(
            url=url, key=key, table="snb_taux_reference",
            records=snb_ref,
            conflict_column="date_effet",
            schema=schema, batch_size=500,
        )
        print(f"  upserted: {n}")

    if ofl_ref:
        print("[ofl_taux_reference] Upserting …")
        n = batch_upsert(
            url=url, key=key, table="ofl_taux_reference",
            records=ofl_ref,
            conflict_column="valid_from",
            schema=schema, batch_size=500,
        )
        print(f"  upserted: {n}")

    print("\n" + "=" * 60)
    print("  IMPORT COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
