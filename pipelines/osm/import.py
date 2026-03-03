#!/usr/bin/env python3
"""
OSM (OpenStreetMap) — Import Pipeline

Fetches geographic features from OpenStreetMap via the Overpass API
for Canton de Genève and upserts into bronze."OSM" on lamap_db.

Strategy:
  1. Fetch all Geneva commune relations from Overpass
  2. For each commune, query all relevant features via area filter
     → commune assignment is automatic (no shapely needed)
  3. Map OSM tags → fclass / code / description (Geofabrik convention)
  4. Use negative osm_id for ways/relations (Geofabrik convention)
  5. UPSERT in batches of 1000

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

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.freshness import get_dataset_meta, update_dataset_meta


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
TABLE = "OSM"
CONFLICT_COLUMN = "osm_id"
BATCH_SIZE = 1000
LOG_EVERY = 10000
DATASET_CODE = "ext_osm"

# Tag keys to query, in priority order (first match determines fclass)
TAG_PRIORITY = [
    "amenity", "shop", "tourism", "leisure", "historic", "place",
    "natural", "highway", "railway", "waterway", "landuse", "building",
]


# ──────────────────────────────────────────────────────────────
# OSM tag → Geofabrik code mapping
# Organised by tag key to avoid collisions (e.g. highway=residential
# vs landuse=residential)
# ──────────────────────────────────────────────────────────────

TAG_CODES = {
    "amenity": {
        "police": "2001", "fire_station": "2002", "post_box": "2003",
        "post_office": "2005", "telephone": "2006", "library": "2007",
        "townhall": "2008", "courthouse": "2009", "prison": "2010",
        "recycling": "2011", "embassy": "2013", "community_centre": "2022",
        "fountain": "2030", "marketplace": "2031", "nightclub": "2032",
        "university": "2081", "school": "2082", "kindergarten": "2083",
        "college": "2084", "pharmacy": "2101", "hospital": "2110",
        "clinic": "2111", "doctors": "2120", "dentist": "2121",
        "veterinary": "2129", "theatre": "2201", "cinema": "2203",
        "park": "2204", "playground": "2205", "dog_park": "2206",
        "sports_centre": "2251", "swimming_pool": "2253",
        "restaurant": "2301", "fast_food": "2302", "cafe": "2303",
        "pub": "2304", "bar": "2305", "food_court": "2306",
        "biergarten": "2307", "bank": "2601", "atm": "2602",
        "toilets": "2901", "bench": "2902", "drinking_water": "2903",
        "shelter": "2421", "place_of_worship": "3100",
        "fuel": "5250", "parking": "5260", "bus_station": "5622",
        "taxi": "5641", "ferry_terminal": "5661",
    },
    "shop": {
        "supermarket": "2501", "bakery": "2502", "kiosk": "2503",
        "mall": "2504", "department_store": "2505", "convenience": "2511",
        "clothes": "2512", "florist": "2513", "chemist": "2514",
        "books": "2515", "butcher": "2516", "shoes": "2517",
        "beverages": "2518", "optician": "2519", "jewelry": "2520",
        "gift": "2521", "sports": "2522", "stationery": "2523",
        "outdoor": "2524", "mobile_phone": "2525", "toys": "2526",
        "newsagent": "2527", "greengrocer": "2528", "beauty": "2529",
        "video": "2530", "car": "2541", "bicycle": "2542",
        "doityourself": "2543", "furniture": "2544", "computer": "2546",
        "garden_centre": "2547", "hairdresser": "2561",
        "car_repair": "2562", "car_rental": "2563", "car_wash": "2564",
        "travel_agent": "2567", "laundry": "2568",
    },
    "tourism": {
        "information": "2701", "hotel": "2401", "motel": "2402",
        "bed_and_breakfast": "2403", "guest_house": "2404",
        "hostel": "2405", "chalet": "2406", "camp_site": "2422",
        "alpine_hut": "2423", "caravan_site": "2424",
        "attraction": "2721", "museum": "2722", "monument": "2723",
        "memorial": "2724", "artwork": "2725", "castle": "2731",
        "ruins": "2732", "archaeological_site": "2733",
        "wayside_cross": "2734", "wayside_shrine": "2735",
        "battlefield": "2736", "fort": "2737", "picnic_site": "2741",
        "viewpoint": "2742", "zoo": "2743", "theme_park": "2744",
    },
    "leisure": {
        "park": "2204", "playground": "2205", "dog_park": "2206",
        "sports_centre": "2251", "pitch": "2252", "swimming_pool": "2253",
        "golf_course": "2255", "stadium": "2256", "ice_rink": "2257",
        "garden": "7207", "nature_reserve": "7210",
    },
    "historic": {
        "castle": "2731", "ruins": "2732", "archaeological_site": "2733",
        "wayside_cross": "2734", "wayside_shrine": "2735",
        "battlefield": "2736", "fort": "2737", "monument": "2723",
        "memorial": "2724",
    },
    "place": {
        "city": "1001", "town": "1002", "village": "1003",
        "hamlet": "1004", "suburb": "1006", "neighbourhood": "1010",
        "island": "1020", "farm": "1030", "isolated_dwelling": "1031",
    },
    "natural": {
        "spring": "4101", "glacier": "4103", "peak": "4111",
        "cliff": "4112", "volcano": "4113", "tree": "4120",
        "cave_entrance": "4132", "beach": "4141",
        "wood": "7201", "scrub": "7217", "heath": "7219",
        "grassland": "7218", "water": "8200", "wetland": "8221",
    },
    "highway": {
        "motorway": "5111", "trunk": "5112", "primary": "5113",
        "secondary": "5114", "tertiary": "5115", "unclassified": "5121",
        "residential": "5122", "living_street": "5123",
        "pedestrian": "5124", "busway": "5125",
        "motorway_link": "5131", "trunk_link": "5132",
        "primary_link": "5133", "secondary_link": "5134",
        "tertiary_link": "5135", "service": "5141", "track": "5143",
        "bridleway": "5151", "cycleway": "5152", "footway": "5153",
        "path": "5154", "steps": "5155",
        "traffic_signals": "5201", "mini_roundabout": "5202",
        "stop": "5203", "crossing": "5204", "turning_circle": "5207",
        "speed_camera": "5208", "street_lamp": "5209",
        "bus_stop": "5621",
    },
    "railway": {
        "station": "5601", "halt": "5602", "tram_stop": "5603",
        "rail": "6101", "tram": "6102", "light_rail": "6103",
        "subway": "6104", "narrow_gauge": "6105", "funicular": "6106",
        "monorail": "6107",
    },
    "waterway": {
        "river": "8101", "stream": "8102", "canal": "8103",
        "drain": "8104", "dam": "5311", "waterfall": "5321",
        "lock_gate": "5331",
    },
    "landuse": {
        "forest": "7201", "residential": "7203", "industrial": "7204",
        "cemetery": "7206", "allotments": "7207", "meadow": "7208",
        "commercial": "7209", "nature_reserve": "7210",
        "recreation_ground": "7211", "retail": "7212",
        "military": "7213", "quarry": "7214", "orchard": "7215",
        "vineyard": "7216", "scrub": "7217", "grass": "7218",
        "farmland": "7229", "farmyard": "7229",
    },
    "building": {
        # All building types map to generic building code
    },
}

# Flat code → description lookup
CODE_DESCRIPTIONS = {
    "1001": "City", "1002": "Town", "1003": "Village", "1004": "Hamlet",
    "1005": "National capital", "1006": "Suburb", "1010": "Neighbourhood",
    "1020": "Island", "1030": "Farm", "1031": "Isolated dwelling",
    "1500": "Building",
    "2001": "Police", "2002": "Fire station", "2003": "Post box",
    "2005": "Post office", "2006": "Telephone", "2007": "Library",
    "2008": "Town hall", "2009": "Courthouse", "2010": "Prison",
    "2011": "Recycling", "2013": "Embassy", "2022": "Community centre",
    "2030": "Fountain", "2031": "Marketplace", "2032": "Nightclub",
    "2081": "University", "2082": "School", "2083": "Kindergarten",
    "2084": "College", "2101": "Pharmacy", "2110": "Hospital",
    "2111": "Clinic", "2120": "Doctors", "2121": "Dentist",
    "2129": "Veterinary", "2201": "Theatre", "2203": "Cinema",
    "2204": "Park", "2205": "Playground", "2206": "Dog park",
    "2251": "Sports centre", "2252": "Pitch", "2253": "Swimming pool",
    "2255": "Golf course", "2256": "Stadium", "2257": "Ice rink",
    "2301": "Restaurant", "2302": "Fast food", "2303": "Cafe",
    "2304": "Pub", "2305": "Bar", "2306": "Food court",
    "2307": "Biergarten", "2401": "Hotel", "2402": "Motel",
    "2403": "Bed and breakfast", "2404": "Guest house", "2405": "Hostel",
    "2406": "Chalet", "2421": "Shelter", "2422": "Campsite",
    "2423": "Alpine hut", "2424": "Caravan site",
    "2501": "Supermarket", "2502": "Bakery", "2503": "Kiosk",
    "2504": "Mall", "2505": "Department store", "2511": "Convenience",
    "2512": "Clothes", "2513": "Florist", "2514": "Chemist",
    "2515": "Books", "2516": "Butcher", "2517": "Shoes",
    "2518": "Beverages", "2519": "Optician", "2520": "Jewelry",
    "2521": "Gift", "2522": "Sports shop", "2523": "Stationery",
    "2524": "Outdoor", "2525": "Mobile phone", "2526": "Toys",
    "2527": "Newsagent", "2528": "Greengrocer", "2529": "Beauty",
    "2530": "Video", "2541": "Car dealer", "2542": "Bicycle shop",
    "2543": "DIY", "2544": "Furniture", "2546": "Computer",
    "2547": "Garden centre", "2561": "Hairdresser",
    "2562": "Car repair", "2563": "Car rental", "2564": "Car wash",
    "2567": "Travel agent", "2568": "Laundry", "2590": "Vending machine",
    "2601": "Bank", "2602": "ATM",
    "2701": "Tourist info", "2721": "Attraction", "2722": "Museum",
    "2723": "Monument", "2724": "Memorial", "2725": "Artwork",
    "2731": "Castle", "2732": "Ruins", "2733": "Archaeological site",
    "2734": "Wayside cross", "2735": "Wayside shrine",
    "2736": "Battlefield", "2737": "Fort", "2741": "Picnic site",
    "2742": "Viewpoint", "2743": "Zoo", "2744": "Theme park",
    "2901": "Public toilets", "2902": "Bench", "2903": "Drinking water",
    "3100": "Place of worship",
    "4101": "Spring", "4103": "Glacier", "4111": "Peak",
    "4112": "Cliff", "4113": "Volcano", "4120": "Tree",
    "4132": "Cave entrance", "4141": "Beach",
    "5111": "Motorway", "5112": "Trunk road", "5113": "Primary road",
    "5114": "Secondary road", "5115": "Tertiary road",
    "5121": "Unclassified road", "5122": "Residential road",
    "5123": "Living street", "5124": "Pedestrian", "5125": "Busway",
    "5131": "Motorway link", "5132": "Trunk link",
    "5133": "Primary link", "5134": "Secondary link",
    "5135": "Tertiary link", "5141": "Service road", "5143": "Track",
    "5151": "Bridleway", "5152": "Cycleway", "5153": "Footway",
    "5154": "Path", "5155": "Steps",
    "5201": "Traffic signals", "5202": "Roundabout", "5203": "Stop sign",
    "5204": "Crossing", "5207": "Turning circle",
    "5208": "Speed camera", "5209": "Street lamp",
    "5250": "Fuel station", "5260": "Parking",
    "5311": "Dam", "5321": "Waterfall", "5331": "Lock gate",
    "5601": "Railway station", "5602": "Railway halt",
    "5603": "Tram stop", "5621": "Bus stop", "5622": "Bus station",
    "5641": "Taxi rank", "5661": "Ferry terminal",
    "6101": "Rail", "6102": "Tram line", "6103": "Light rail",
    "6104": "Subway", "6105": "Narrow gauge", "6106": "Funicular",
    "6107": "Monorail",
    "7201": "Forest", "7203": "Residential area", "7204": "Industrial",
    "7206": "Cemetery", "7207": "Garden / allotments", "7208": "Meadow",
    "7209": "Commercial area", "7210": "Nature reserve",
    "7211": "Recreation ground", "7212": "Retail area",
    "7213": "Military", "7214": "Quarry", "7215": "Orchard",
    "7216": "Vineyard", "7217": "Scrub", "7218": "Grassland",
    "7219": "Heath", "7229": "Farmland",
    "8101": "River", "8102": "Stream", "8103": "Canal", "8104": "Drain",
    "8200": "Water", "8201": "Reservoir", "8221": "Wetland",
}

# Default code for buildings not in the mapping
BUILDING_DEFAULT_CODE = "1500"


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


def overpass_query(query: str, timeout: int = 120) -> dict | None:
    """Execute an Overpass API query with retries."""
    for attempt in range(1, 4):
        try:
            r = requests.post(
                OVERPASS_URL,
                data={"data": query},
                timeout=timeout + 60,  # HTTP timeout > Overpass timeout
            )
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429 or r.status_code >= 500:
                wait = 15 * attempt
                print(f"    Overpass {r.status_code}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    Overpass error {r.status_code}: {r.text[:300]}")
                return None
        except requests.exceptions.Timeout:
            wait = 15 * attempt
            print(f"    Overpass timeout, retrying in {wait}s ({attempt}/3)")
            time.sleep(wait)
        except Exception as e:
            wait = 15 * attempt
            print(f"    Overpass error ({attempt}/3): {e}")
            time.sleep(wait)
    return None


# ──────────────────────────────────────────────────────────────
# Data fetching
# ──────────────────────────────────────────────────────────────

def get_communes() -> list[dict]:
    """Fetch all commune relations in Geneva canton from Overpass."""
    query = """
[out:json][timeout:30];
rel["boundary"="administrative"]["admin_level"="4"]["name"="Genève"];
map_to_area -> .canton;
rel(area.canton)["boundary"="administrative"]["admin_level"="8"];
out tags;
"""
    data = overpass_query(query, timeout=30)
    if not data:
        return []

    communes = []
    for el in data.get("elements", []):
        name = el.get("tags", {}).get("name")
        if name:
            communes.append({
                "name": name,
                "rel_id": el["id"],
                "area_id": el["id"] + 3600000000,  # Overpass area ID convention
            })

    communes.sort(key=lambda c: c["name"])
    return communes


def build_tag_union() -> str:
    """Build the Overpass union block for all tag categories."""
    parts = []
    for tag in TAG_PRIORITY:
        parts.append(f'  node["{tag}"](area.a);')
        parts.append(f'  way["{tag}"](area.a);')
    return "\n".join(parts)


def fetch_commune_features(area_id: int, tag_union: str) -> list[dict]:
    """Fetch all tagged features within a commune area."""
    query = f"""
[out:json][timeout:300];
area({area_id})->.a;
(
{tag_union}
);
out center tags;
"""
    data = overpass_query(query, timeout=300)
    if not data:
        return []
    return data.get("elements", [])


# ──────────────────────────────────────────────────────────────
# Record building
# ──────────────────────────────────────────────────────────────

def determine_fclass(tags: dict) -> tuple[str | None, str | None, str | None]:
    """
    Determine primary (tag_key, fclass, code) from OSM tags.
    Uses TAG_PRIORITY order so amenity beats highway beats building, etc.
    """
    for key in TAG_PRIORITY:
        value = tags.get(key)
        if not value:
            continue

        # Look up code from the tag-specific mapping
        codes = TAG_CODES.get(key, {})
        code = codes.get(value)

        # Building fallback: any building=* maps to 1500
        if key == "building" and not code:
            code = BUILDING_DEFAULT_CODE

        return key, value, code

    return None, None, None


def build_record(element: dict, commune_name: str) -> dict | None:
    """Build a database record from an Overpass element."""
    tags = element.get("tags", {})
    _tag_key, fclass, code = determine_fclass(tags)

    if not fclass:
        return None

    # osm_id: positive for nodes, negative for ways/relations (Geofabrik convention)
    elem_type = element.get("type", "node")
    raw_id = element["id"]
    if elem_type in ("way", "relation"):
        osm_id = str(-raw_id)
    else:
        osm_id = str(raw_id)

    # Coordinates: direct for nodes, center for ways/relations
    if elem_type == "node":
        lat = element.get("lat")
        lon = element.get("lon")
    else:
        center = element.get("center", {})
        lat = center.get("lat")
        lon = center.get("lon")

    if lat is None or lon is None:
        return None

    # Geometry as GeoJSON point
    geometry = json.dumps({"type": "Point", "coordinates": [lon, lat]})

    # Code (integer column) and description
    code_int = int(code) if code else None
    description = CODE_DESCRIPTIONS.get(code or "", fclass.replace("_", " ").title())
    name = tags.get("name") or None

    return {
        "osm_id": osm_id,
        "code": code_int,
        "fclass": fclass,
        "name": name,
        "description": description,
        "commune": commune_name,
        "geometry": geometry,
    }


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    lamap_url = os.environ.get("LAMAP_SUPABASE_URL", "")
    lamap_key = os.environ.get("LAMAP_SUPABASE_SERVICE_KEY", "")
    lamap_schema = os.environ.get("LAMAP_SCHEMA", "bronze")
    camelote_url = os.environ.get("CAMELOTE_SUPABASE_URL", "")
    camelote_key = os.environ.get("CAMELOTE_SUPABASE_KEY", "")

    if not lamap_url or not lamap_key:
        print("ERROR: LAMAP_SUPABASE_URL and LAMAP_SUPABASE_SERVICE_KEY are required")
        sys.exit(1)

    print("=" * 60)
    print("  OSM (OpenStreetMap) Pipeline")
    print("  Source: Overpass API — Canton de Genève")
    print(f"  Target: {lamap_schema}.{TABLE}")
    print("=" * 60)

    # ── Show previous metadata ──
    meta = get_dataset_meta(camelote_url, camelote_key, DATASET_CODE)
    if meta and meta.get("last_acquired_at"):
        print(f"\n  Last acquired: {meta['last_acquired_at'].isoformat()}")
        print(f"  Previous record count: {meta.get('record_count', 'unknown')}")

    # ── Row count BEFORE ──
    rows_before = get_row_count(lamap_url, lamap_key, lamap_schema, TABLE)
    print(f"  Rows before: {rows_before:,}" if rows_before is not None else "  Rows before: unknown")

    # ── Fetch communes ──
    print("\n  Fetching Geneva communes from Overpass...")
    communes = get_communes()
    if not communes:
        print("  ERROR: Could not fetch communes")
        sys.exit(1)
    print(f"  Found {len(communes)} communes")

    # ── Build tag union query (reused for each commune) ──
    tag_union = build_tag_union()

    # ── Process each commune ──
    all_records = []
    seen_ids = set()
    total_raw = 0
    start_time = time.time()

    for i, commune in enumerate(communes):
        name = commune["name"]
        area_id = commune["area_id"]
        print(f"\n  [{i + 1}/{len(communes)}] {name} (area {area_id})")

        elements = fetch_commune_features(area_id, tag_union)
        total_raw += len(elements)
        commune_count = 0

        for el in elements:
            record = build_record(el, name)
            if not record:
                continue
            # Deduplicate: keep first occurrence (earlier commune wins)
            if record["osm_id"] in seen_ids:
                continue
            seen_ids.add(record["osm_id"])
            all_records.append(record)
            commune_count += 1

        print(f"    Overpass: {len(elements)} elements → {commune_count} records")

        # Progress logging
        if len(all_records) > 0 and len(all_records) % LOG_EVERY < commune_count:
            elapsed = time.time() - start_time
            print(f"    ── Total so far: {len(all_records):,} records ({elapsed:.0f}s)")

        # Rate limit: be respectful to Overpass API
        time.sleep(3)

    print(f"\n{'━' * 60}")
    print(f"  Overpass complete: {total_raw:,} raw elements → {len(all_records):,} unique records")

    if not all_records:
        print("  ERROR: No records fetched")
        sys.exit(1)

    # ── Upsert in batches ──
    print(f"  Upserting {len(all_records):,} records (batch size {BATCH_SIZE})...")

    total_upserted = 0
    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i : i + BATCH_SIZE]
        upserted = batch_upsert(
            url=lamap_url,
            key=lamap_key,
            table=TABLE,
            records=batch,
            conflict_column=CONFLICT_COLUMN,
            schema=lamap_schema,
            batch_size=BATCH_SIZE,
        )
        total_upserted += upserted

        if (i + BATCH_SIZE) % LOG_EVERY < BATCH_SIZE:
            print(f"    Progress: {total_upserted:,} / {len(all_records):,} upserted")

    # ── Row count AFTER ──
    rows_after = get_row_count(lamap_url, lamap_key, lamap_schema, TABLE)

    # ── Summary ──
    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print("  IMPORT COMPLETE")
    print(f"  Communes queried: {len(communes)}")
    print(f"  Raw elements:     {total_raw:,}")
    print(f"  Unique records:   {len(all_records):,}")
    print(f"  Rows upserted:    {total_upserted:,}")
    print(f"  Rows before:      {rows_before:,}" if rows_before is not None else "  Rows before:      unknown")
    print(f"  Rows after:       {rows_after:,}" if rows_after is not None else "  Rows after:       unknown")
    if rows_before is not None and rows_after is not None:
        delta = rows_after - rows_before
        print(f"  Net new:          {delta:,}")
    print(f"  Duration:         {elapsed / 60:.1f} min")
    print("=" * 60)

    if total_upserted == 0:
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
