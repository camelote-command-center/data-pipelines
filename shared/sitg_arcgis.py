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

import hashlib
import re
import json
import time
from decimal import Decimal, ROUND_HALF_UP

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


def get_object_id_field(base_url: str) -> str:
    """
    Read the layer's own object-id field from its service metadata.

    This must NOT be guessed. Across the ten pipelines sharing this module,
    four different spellings are live: `objectid` on 51 layers, `fid` on 3,
    `OBJECTID` on 2 and `shape_fid` on 1 (BRUIT_ROUTIER_MESURE_AUX_FACADES,
    which is 26 pages). Hardcoding `objectid` would have sorted 6 layers by a
    field that does not exist, which ArcGIS may answer by ignoring the sort
    rather than erroring, leaving pagination exactly as unstable as before.

    Raises if the field cannot be determined. A layer whose object-id field is
    unknown must abort, never fall back to an arbitrary order: an unstable sort
    is the defect this exists to remove, and a silent default would reintroduce
    it in the one place nobody would look.
    """
    data = _get_json(f"{base_url}?f=pjson")
    if "error" in data:
        err = data["error"]
        raise RuntimeError(
            f"cannot read layer metadata for {base_url}: "
            f"{err.get('code')} {err.get('message')}"
        )
    oid = data.get("objectIdField")
    if not oid:
        raise RuntimeError(
            f"layer {base_url} does not declare objectIdField. Refusing to "
            f"paginate without a stable sort key: resultOffset paging is not "
            f"deterministic without an explicit orderByFields, and guessing a "
            f"default is what this check exists to prevent."
        )
    return oid


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

    # Stable sort key, read from the layer itself. Without an explicit
    # orderByFields, ArcGIS gives no ordering guarantee between requests, so
    # resultOffset paging can return a row twice and miss another. The row
    # COUNT still looks plausible, which is why this went unnoticed.
    order_field = get_object_id_field(base_url)

    print(f"  Total records in API: {count:,}  (ordering by {order_field})")

    all_records: list[dict] = []
    offsets = list(range(0, count, page_size))
    total_pages = len(offsets)

    for page_num, offset in enumerate(offsets, 1):
        geom_flag = "true" if include_geometry else "false"
        url = (
            f"{base_url}/query?where=1%3D1&outFields=*"
            f"&returnGeometry={geom_flag}&outSR={out_sr}"
            f"&orderByFields={order_field}"
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

    # Count assertion. This is what makes the ordering fix verifiable rather
    # than merely plausible: if paging still skipped or duplicated a row, the
    # paginated total will not equal the count the service declared up front.
    # Abort rather than land a partial layer, exactly as the forest parser does.
    if len(all_records) != count:
        raise RuntimeError(
            f"{base_url}: paginated total {len(all_records):,} does not match the "
            f"declared count {count:,} (difference {len(all_records) - count:+,}). "
            f"Aborting rather than landing a partial or duplicated layer. "
            f"Ordered by {order_field}, page size {page_size}."
        )

    print(f"  Fetch complete: {len(all_records):,} records, count assertion OK")
    return all_records


# ──────────────────────────────────────────────────────────────
# Content keys for layers that publish no durable identifier
# ──────────────────────────────────────────────────────────────

GEOM_KEY_DECIMALS = 7
_Q = Decimal(1).scaleb(-GEOM_KEY_DECIMALS)


def _coord(v) -> str:
    """
    One coordinate, rounded to GEOM_KEY_DECIMALS and rendered as fixed-scale
    text.

    ROUND_HALF_UP and Decimal(str(v)) are not stylistic. The twin of this
    function in SQL (public.sitg_arcgis_geom_key) does
    `round(<text>::numeric, 7)::text`, and Postgres numeric rounds half away
    from zero and keeps the full scale. Python's own round() is half-to-even
    and drops trailing zeros, so `round(6.1, 7)` would render "6.1" against
    Postgres's "6.1000000" and every key would differ. Decimal(str(v)) sees
    exactly the digits Postgres sees, because the geometry crosses the wire as
    the repr of this same float.
    """
    return str(Decimal(str(v)).quantize(_Q, rounding=ROUND_HALF_UP))


def arcgis_geom_key(geometry: str | None) -> str:
    """
    Canonical text form of an ArcGIS JSON geometry, for use inside a content
    key. Must stay byte-identical to public.sitg_arcgis_geom_key() on re-LLM.

    7 decimal degrees is ~1.1 cm north-south and ~0.8 cm east-west at 46°N,
    far below the positional accuracy of any SITG layer, so rounding cannot
    merge two features the source can tell apart. Coordinates are rounded and
    written out as text rather than hashed as binary so that a key remains
    inspectable when one has to be debugged.

    Unrecognised geometry types fall back to the raw text rather than to an
    empty string: '' would silently collapse every such feature onto one row,
    which is the exact failure mode this exists to remove (bug ab259069).
    """
    if not geometry or not geometry.strip():
        return "nogeom"
    g = json.loads(geometry)
    if "x" in g:
        return f"pt:{_coord(g['x'])},{_coord(g['y'])}"
    parts = g.get("paths") or g.get("rings")
    if parts is None:
        return "raw:" + hashlib.md5(geometry.encode()).hexdigest()
    return "|".join(
        ";".join(f"{_coord(p[0])},{_coord(p[1])}" for p in ring) for ring in parts
    )


def content_key(record: dict, attr_fields: list[str], include_geometry: bool = True) -> str:
    """
    Deterministic identity for one feature: md5 over the named attributes in
    the given order, then the canonical geometry.

    `attr_fields` must list only fields the parser itself writes. An enrichment
    column filled in later, or a legacy column the source no longer publishes,
    would make the key of an existing row differ from the key the next fetch
    computes — orphaning the row instead of updating it.
    """
    parts = [(record.get(f) or "") for f in attr_fields]
    if include_geometry:
        parts.append(arcgis_geom_key(record.get("geometry")))
    return hashlib.md5("|".join(parts).encode()).hexdigest()
