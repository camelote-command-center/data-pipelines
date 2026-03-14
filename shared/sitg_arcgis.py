"""
Reusable helper for SITG ArcGIS REST API data pipelines.

Handles:
  - ArcGIS REST pagination (2000 records per page)
  - Geometry extraction in WGS84 (outSR=4326)
  - Field name mapping (to snake_case, matching existing JS parsers)
  - Value normalisation (stringify + whitespace collapse)
  - Error handling with retries

Usage:
    from shared.sitg_arcgis import fetch_all_features

    records = fetch_all_features(
        base_url="https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/LAYER/FeatureServer/0",
        include_geometry=True,
    )

Each record is a dict with snake_case keys matching the existing bronze table
columns (produced by the legacy JS parsers).  If include_geometry=True, a
'geometry' key is added containing the raw ArcGIS geometry as a JSON string
in WGS84 (EPSG:4326).
"""

import re
import json
import time

import requests

# ── Retry config ──────────────────────────────────────────────
MAX_RETRIES = 5
RETRY_BACKOFF = 2  # seconds: 2, 4, 8, 16, 32

# ── ArcGIS defaults ──────────────────────────────────────────
PAGE_SIZE = 2000  # ArcGIS FeatureServer default max


# ──────────────────────────────────────────────────────────────
# Field name / value transforms  (exact port of JS keyToSnakeCase
# and formatKeys from LamapParser)
# ──────────────────────────────────────────────────────────────

def key_to_snake_case(key: str) -> str:
    """
    Convert ArcGIS field name to snake_case.

    Exact match of JS:
        key.replace(/[^\\w\\s]/g, "_").replace(/\\s+/g, "_").toLowerCase()
    """
    key = re.sub(r"[^\w\s]", "_", key)
    key = re.sub(r"\s+", "_", key)
    return key.lower()


def format_value(val):
    """
    Normalise an attribute value: stringify and collapse whitespace.

    Exact match of JS:
        val ? val.toString().replace(/\\s+/g, " ").trim() : null
    """
    if val is None:
        return None
    s = str(val)
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None


# ──────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────

def _get_json(url: str, retry: int = 0) -> dict:
    """GET JSON with retry and exponential backoff."""
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        if retry >= MAX_RETRIES:
            raise
        wait = RETRY_BACKOFF ** (retry + 1)
        print(f"    Request error ({e}), retrying in {wait}s ({retry + 1}/{MAX_RETRIES})")
        time.sleep(wait)
        return _get_json(url, retry=retry + 1)


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────

def get_record_count(base_url: str) -> int:
    """Get total record count from an ArcGIS Feature Service layer."""
    url = f"{base_url}/query?where=1%3D1&returnCountOnly=true&f=json"
    data = _get_json(url)
    count = data.get("count")
    if count is None:
        raise RuntimeError(
            f"ArcGIS API did not return a count.  Response: {json.dumps(data)[:500]}"
        )
    return count


def fetch_all_features(
    base_url: str,
    include_geometry: bool = True,
    out_sr: int = 4326,
    page_size: int = PAGE_SIZE,
) -> list[dict]:
    """
    Fetch ALL features from an ArcGIS REST Feature Service, paginated.

    Args:
        base_url:          Layer URL (e.g. .../FeatureServer/0)
        include_geometry:  Include geometry in output (default True)
        out_sr:            Output spatial reference (default 4326 = WGS84)
        page_size:         Records per page (default 2000, ArcGIS max)

    Returns:
        List of dicts with snake_case keys.
        If include_geometry=True, each dict has a 'geometry' key with
        the raw ArcGIS geometry as a JSON string.
    """
    count = get_record_count(base_url)
    if not count:
        print("  WARNING: ArcGIS API returned 0 records")
        return []

    print(f"  Total records in API: {count:,}")

    all_records: list[dict] = []
    offsets = list(range(0, count, page_size))
    total_pages = len(offsets)

    for page_num, offset in enumerate(offsets, 1):
        geom_flag = "true" if include_geometry else "false"
        url = (
            f"{base_url}/query?where=1%3D1&outFields=*"
            f"&returnGeometry={geom_flag}&outSR={out_sr}"
            f"&resultOffset={offset}&resultRecordCount={page_size}&f=json"
        )

        data = _get_json(url)

        # Check for ArcGIS-level error
        if "error" in data:
            err = data["error"]
            raise RuntimeError(
                f"ArcGIS API error {err.get('code')}: {err.get('message')}"
            )

        features = data.get("features", [])
        if not features:
            break

        for feat in features:
            attrs = feat.get("attributes")
            if not attrs:
                continue

            # Convert keys to snake_case + normalise values
            record: dict = {}
            for k, v in attrs.items():
                record[key_to_snake_case(k)] = format_value(v)

            # Include geometry as JSON string
            if include_geometry and feat.get("geometry"):
                record["geometry"] = json.dumps(feat["geometry"])

            all_records.append(record)

        if page_num % 10 == 0 or page_num == 1 or page_num == total_pages:
            print(f"  Page {page_num}/{total_pages}: {len(all_records):,} records fetched")

        # Small delay between pages to be respectful
        time.sleep(0.1)

    print(f"  Fetch complete: {len(all_records):,} records")
    return all_records
