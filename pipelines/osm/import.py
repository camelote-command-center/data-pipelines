#!/usr/bin/env python3
"""
OSM (OpenStreetMap) — Import Pipeline

Fetches geographic features from OpenStreetMap via the Overpass API
for Suisse Romande (cantons GE, VD, FR, VS, NE, JU) and upserts into
bronze_ch.osm on re-LLM.

Strategy:
  1. For each Suisse Romande canton, fetch commune relations via Overpass
     area filter using ISO3166-2 codes (e.g. area["ISO3166-2"="CH-GE"])
  2. For each commune, query all relevant features via area filter
     → commune assignment is automatic (no shapely needed)
  3. Map OSM tags → fclass / code / description (Geofabrik convention)
  4. Use negative osm_id for ways/relations (Geofabrik convention)
  5. UPSERT in batches of 1000

DATA SAFETY:
    - UPSERT only (INSERT ... ON CONFLICT DO UPDATE).
    - Never truncates or deletes existing data.

Environment variables:
    RE_LLM_SUPABASE_URL              - re-LLM Supabase project URL (required)
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY - service_role key (required)
    RE_LLM_SCHEMA                    - target schema (default: bronze_ch)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

# Add repo root to path so we can import shared/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert
from shared.freshness import get_dataset_meta, update_dataset_meta


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
# Overpass answers 406 Not Acceptable to a request without a User-Agent (verified 2026-09-27:
# every run since 2026-09-26 failed that way). Their usage policy asks for an identifying agent.
OVERPASS_HEADERS = {
    "User-Agent": "lamap-osm-import/1.0 (+https://lamap.ch; contact: ops@lamap.ch)",
    "Accept": "application/json",
}
TABLE = "osm"
CONFLICT_COLUMN = "osm_id"
BATCH_SIZE = 1000
LOG_EVERY = 10000
DATASET_CODE = "ext_osm"

# Suisse Romande cantons — ISO 3166-2 codes
# Order: smallest first to fail fast if there's an issue
CANTONS = [
    ("NE", "CH-NE"),   # Neuchâtel        ~27 communes
    ("JU", "CH-JU"),   # Jura             ~53 communes
    ("GE", "CH-GE"),   # Genève           ~45 communes
    ("FR", "CH-FR"),   # Fribourg         ~121 communes
    ("VS", "CH-VS"),   # Valais           ~123 communes
    ("VD", "CH-VD"),   # Vaud             ~300 communes (largest → last)
]

# A full sweep no longer fits in one job: Overpass throttles (429/504 with backoff) and the
# GitHub job is capped at 180 min — run 36348442561 was cancelled at 3 h with everything still
# in memory, so nothing landed. Cantons are now written and recorded one by one, and a run stops
# starting new cantons before the cap; the next run continues where this one stopped.
STATE_TABLE = "osm_canton_state"
REFRESH_DAYS = int(os.environ.get("OSM_REFRESH_DAYS", "80"))   # quarterly cadence
DEFAULT_BUDGET_MIN = int(os.environ.get("OSM_TIME_BUDGET_MIN", "150"))
FLUSH_COMMUNES = int(os.environ.get("OSM_FLUSH_COMMUNES", "10"))   # write and record progress this often

# Delay between Overpass queries (seconds) — be respectful to the API
QUERY_DELAY = 3
# Extra delay between cantons (seconds) — give the API breathing room
CANTON_DELAY = 15

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
                headers=OVERPASS_HEADERS,
                timeout=timeout + 60,  # HTTP timeout > Overpass timeout
            )
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429 or r.status_code >= 500:
                wait = 15 * attempt
                print(f"    Overpass {r.status_code}, retrying in {wait}s...")
                time.sleep(wait)
            elif r.status_code in (403, 406):
                # Rejected request shape (missing/blocked agent), not a transient failure: say so plainly.
                print(f"    Overpass refused the request ({r.status_code}) — check OVERPASS_HEADERS: {r.text[:200]}")
                return None
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


def load_canton_state(url: str, key: str, schema: str) -> dict:
    """{canton: row} from the state table; empty dict when it cannot be read (every canton due)."""
    try:
        r = requests.get(
            f"{url.rstrip('/')}/rest/v1/{STATE_TABLE}"
            "?select=canton,completed_at,complete,done_communes,records",
            headers={"apikey": key, "Authorization": f"Bearer {key}", "Accept-Profile": schema},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"  Warning: could not read {schema}.{STATE_TABLE} ({r.status_code}); treating every canton as due")
            return {}
        out = {}
        for row in r.json():
            done = row.get("completed_at")
            try:
                row["completed_at"] = datetime.fromisoformat(done.replace("Z", "+00:00")) if done else None
            except Exception:
                row["completed_at"] = None
            out[row["canton"]] = row
        return out
    except Exception as e:
        print(f"  Warning: could not read {schema}.{STATE_TABLE}: {e}")
        return {}


def save_canton_state(url: str, key: str, schema: str, canton: str, iso: str, communes: int,
                      records: int, done_communes: list, complete: bool) -> None:
    """Record a canton's progress, finished or not. A failure here only costs repeated work."""
    now = datetime.now(timezone.utc).isoformat()
    row = {"canton": canton, "iso_code": iso, "communes": communes, "records": records,
           "done_communes": sorted(done_communes), "complete": complete,
           "run_url": os.environ.get("GITHUB_RUN_URL", "")}
    # completed_at marks a finished canton only; a partial row keeps the previous value untouched.
    if complete:
        row["completed_at"] = now
    try:
        r = requests.post(
            f"{url.rstrip('/')}/rest/v1/{STATE_TABLE}",
            headers={"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json",
                     "Content-Profile": schema, "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=[row], timeout=30,
        )
        if r.status_code >= 300:
            print(f"    Warning: could not record {canton} state ({r.status_code}): {r.text[:150]}")
    except Exception as e:
        print(f"    Warning: could not record {canton} state: {e}")


def upsert_records(url: str, key: str, schema: str, records: list) -> int:
    """Upsert one canton's records in batches. Never deletes."""
    done = 0
    for i in range(0, len(records), BATCH_SIZE):
        done += batch_upsert(url=url, key=key, table=TABLE, records=records[i : i + BATCH_SIZE],
                             conflict_column=CONFLICT_COLUMN, schema=schema, batch_size=BATCH_SIZE)
    return done


def probe_canton_column(url: str, key: str, schema: str) -> bool:
    """Check if the 'canton' column exists in the OSM table.

    Makes a lightweight GET with select=canton&limit=0. If PostgREST
    returns 200 the column exists; a 400 with PGRST204 means it doesn't.
    This is read-only and never writes to the database.
    """
    endpoint = f"{url.rstrip('/')}/rest/v1/{TABLE}?select=canton&limit=0"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
    }
    if schema and schema != "public":
        headers["Accept-Profile"] = schema
    try:
        r = requests.get(endpoint, headers=headers, timeout=10)
        return r.status_code == 200
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────
# Data fetching
# ──────────────────────────────────────────────────────────────

def get_canton_communes(iso_code: str) -> list[dict]:
    """Fetch all commune relations in a canton from Overpass.

    Uses ISO 3166-2 area filter (e.g. area["ISO3166-2"="CH-GE"])
    which works reliably for bilingual cantons like FR and VS.
    """
    query = f"""
[out:json][timeout:60];
area["ISO3166-2"="{iso_code}"]->.canton;
rel(area.canton)["boundary"="administrative"]["admin_level"="8"];
out tags;
"""
    data = overpass_query(query, timeout=60)
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


def build_record(
    element: dict,
    commune_name: str,
    canton: str | None = None,
    include_canton: bool = True,
) -> dict | None:
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

    record = {
        "osm_id": osm_id,
        "code": code_int,
        "fclass": fclass,
        "name": name,
        "description": description,
        "commune": commune_name,
        "geometry": geometry,
    }

    if include_canton and canton:
        record["canton"] = canton

    return record


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="OSM import (resumable, time-budgeted)")
    ap.add_argument("--time-budget-minutes", type=int, default=DEFAULT_BUDGET_MIN,
                    help="stop starting new cantons after N minutes (job cap is 180)")
    ap.add_argument("--refresh-days", type=int, default=REFRESH_DAYS,
                    help="a canton completed less than N days ago is skipped")
    ap.add_argument("--all", action="store_true", help="ignore recorded progress and refresh every canton")
    args = ap.parse_args()

    rellm_url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    rellm_key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    rellm_schema = os.environ.get("RE_LLM_SCHEMA", "bronze_ch")
    camelote_url = os.environ.get("CAMELOTE_SUPABASE_URL", "")
    camelote_key = os.environ.get("CAMELOTE_SUPABASE_KEY", "")

    if not rellm_url or not rellm_key:
        print("ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required")
        sys.exit(1)

    canton_labels = ", ".join(c[0] for c in CANTONS)
    print("=" * 60)
    print("  OSM (OpenStreetMap) Pipeline — Suisse Romande")
    print(f"  Cantons: {canton_labels}")
    print(f"  Target: {rellm_schema}.{TABLE}")
    print("=" * 60)

    # ── Show previous metadata ──
    meta = get_dataset_meta(camelote_url, camelote_key, DATASET_CODE)
    if meta and meta.get("last_acquired_at"):
        print(f"\n  Last acquired: {meta['last_acquired_at'].isoformat()}")
        print(f"  Previous record count: {meta.get('record_count', 'unknown')}")

    # ── Row count BEFORE ──
    rows_before = get_row_count(rellm_url, rellm_key, rellm_schema, TABLE)
    print(f"  Rows before: {rows_before:,}" if rows_before is not None else "  Rows before: unknown")

    # ── Probe: does the canton column exist? ──
    include_canton = probe_canton_column(rellm_url, rellm_key, rellm_schema)
    if include_canton:
        print("  Canton column: found — will populate")
    else:
        print("  Canton column: not found — skipping (add column to table to enable)")

    # ── Which cantons are due? ──
    state = {} if args.all else load_canton_state(rellm_url, rellm_key, rellm_schema)
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.refresh_days)

    def due_reason(code):
        """None when the canton needs no work, else why it does."""
        row = state.get(code)
        if args.all or row is None:
            return "new"
        if not row.get("complete", True):
            return f"resume at {len(row.get('done_communes') or [])} communes"
        if row.get("completed_at") is None or row["completed_at"] < cutoff:
            return "stale"
        return None

    due = [(c, iso, due_reason(c)) for c, iso in CANTONS if due_reason(c)]
    fresh = [c for c, _ in CANTONS if not due_reason(c)]
    print(f"\n  Due this run: {', '.join(f'{c} ({why})' for c, _, why in due) or 'none'}"
          f"{' | already fresh: ' + ', '.join(fresh) if fresh else ''}")
    print(f"  Time budget: {args.time_budget_minutes} min (refresh interval {args.refresh_days} days)")
    if not due:
        # Nothing was acquired, so dataset freshness must not be restamped: the marker is neither
        # 'true' (which would PATCH last_acquired_at) nor 'false' (which would claim work is left).
        print("\n  Nothing due — every canton was refreshed within the interval.")
        print("OSM_CYCLE_COMPLETE=skipped")
        return

    # ── Build tag union query (reused for every commune) ──
    tag_union = build_tag_union()

    # ── Process each canton ──
    # Records are kept per canton only: each canton is written before the next one starts, so a
    # cancelled or budget-stopped run keeps everything it already fetched.
    seen_ids = set()
    total_records = 0
    total_raw = 0
    total_communes = 0
    canton_stats = {}
    start_time = time.time()

    budget_s = args.time_budget_minutes * 60
    total_upserted = 0
    stopped_early = False
    for canton_idx, (canton_code, iso_code, _why) in enumerate(due):
        if time.time() - start_time > budget_s:
            print(f"\n  Time budget reached — {len(due) - canton_idx} canton(s) left for the next run: "
                  f"{', '.join(c for c, _, _w in due[canton_idx:])}")
            stopped_early = True
            break
        canton_start = time.time()

        print(f"\n{'━' * 60}")
        print(f"  Canton {canton_idx + 1}/{len(due)}: {canton_code} ({iso_code})")
        print(f"{'━' * 60}")

        # ── Fetch communes for this canton ──
        print(f"  Fetching communes...")
        communes = get_canton_communes(iso_code)
        if not communes:
            print(f"  WARNING: No communes found for {canton_code}, skipping")
            canton_stats[canton_code] = {"communes": 0, "records": 0, "raw": 0}
            continue

        # Resume: communes already imported in this cycle are skipped by name, so a shifted
        # commune list (a merged or renamed commune) cannot make the run skip unimported ground.
        prev = {} if args.all else (state.get(canton_code) or {})
        already = set(prev.get("done_communes") or []) if not prev.get("complete", True) else set()
        canton_total_records = prev.get("records", 0) if already else 0
        todo = [c for c in communes if c["name"] not in already]
        print(f"  Found {len(communes)} communes"
              + (f" — {len(already)} already imported, {len(todo)} to go" if already else ""))
        total_communes += len(todo)
        canton_raw = 0
        canton_batch = []
        canton_records_run = 0
        done_names = list(already)
        canton_partial = False

        # Wait after the commune-list query
        time.sleep(QUERY_DELAY)

        # ── Process each remaining commune in this canton ──
        for i, commune in enumerate(todo):
            name = commune["name"]
            area_id = commune["area_id"]

            # Print every commune for small cantons, every 10th for large ones
            verbose = len(todo) <= 60 or (i % 10 == 0) or (i == len(todo) - 1)
            if verbose:
                print(f"\n  [{i + 1}/{len(todo)}] {name}")

            elements = fetch_commune_features(area_id, tag_union)
            total_raw += len(elements)
            canton_raw += len(elements)
            commune_count = 0

            for el in elements:
                record = build_record(el, name, canton=canton_code, include_canton=include_canton)
                if not record:
                    continue
                # Deduplicate: keep first occurrence (earlier commune wins)
                if record["osm_id"] in seen_ids:
                    continue
                seen_ids.add(record["osm_id"])
                canton_batch.append(record)
                commune_count += 1

            if verbose:
                print(f"    {len(elements)} elements → {commune_count} new records")

            # Progress logging
            running = total_records + len(canton_batch)
            if running > 0 and running % LOG_EVERY < commune_count:
                elapsed = time.time() - start_time
                print(f"    ── Total so far: {running:,} records ({elapsed:.0f}s)")

            done_names.append(name)

            # A canton can outlast the job cap on its own: VD has ~300 communes and one commune
            # costs ~150 s. Flush every FLUSH_COMMUNES communes so the work survives, and stop
            # here — not only between cantons — when the budget is spent.
            out_of_budget = time.time() - start_time > budget_s
            last_one = i == len(todo) - 1
            if canton_batch and (len(done_names) % FLUSH_COMMUNES == 0 or out_of_budget or last_one):
                n_up = upsert_records(rellm_url, rellm_key, rellm_schema, canton_batch)
                total_upserted += n_up
                canton_records_run += len(canton_batch)
                canton_total_records += len(canton_batch)
                total_records += len(canton_batch)
                canton_batch = []   # persisted: free it
                save_canton_state(rellm_url, rellm_key, rellm_schema, canton_code, iso_code,
                                  len(communes), canton_total_records, done_names,
                                  complete=last_one)
                print(f"    ── flushed {n_up:,} records "
                      f"({len(done_names)}/{len(communes)} communes of {canton_code} done)")
            if out_of_budget and not last_one:
                print(f"\n  Time budget reached inside {canton_code} — "
                      f"{len(todo) - i - 1} commune(s) of it continue next run")
                stopped_early = True
                canton_partial = True
                break

            # Rate limit: be respectful to Overpass API
            time.sleep(QUERY_DELAY)

        # Nothing new to write (every commune was already imported): close the canton out.
        if not todo:
            save_canton_state(rellm_url, rellm_key, rellm_schema, canton_code, iso_code,
                              len(communes), canton_total_records, done_names, complete=True)

        canton_elapsed = time.time() - canton_start
        canton_stats[canton_code] = {
            "communes": len(done_names),
            "records": canton_records_run,
            "raw": canton_raw,
        }
        print(f"\n  ── {canton_code} {'partial' if canton_partial else 'complete'}: "
              f"{len(done_names)}/{len(communes)} communes, "
              f"{canton_raw:,} raw → {canton_records_run:,} records this run ({canton_elapsed:.0f}s)")
        if stopped_early:
            break

        # Extra delay between cantons to avoid Overpass rate limits
        if canton_idx < len(due) - 1 and not stopped_early:
            print(f"  Waiting {CANTON_DELAY}s before next canton...")
            time.sleep(CANTON_DELAY)

    # ── Summary of Overpass phase ──
    overpass_elapsed = time.time() - start_time
    print(f"\n{'━' * 60}")
    print(f"  Overpass complete ({overpass_elapsed / 60:.1f} min)")
    print(f"  Cantons: {len(canton_stats)}, Communes: {total_communes}")
    print(f"  Raw elements: {total_raw:,} → Unique records: {total_records:,}")
    for code, stats in canton_stats.items():
        print(f"    {code}: {stats['communes']} communes, {stats['records']:,} records")
    print(f"{'━' * 60}")

    if not total_records and not stopped_early:
        print("  ERROR: No records fetched")
        sys.exit(1)

    # ── Row count AFTER (records were written canton by canton, above) ──
    rows_after = get_row_count(rellm_url, rellm_key, rellm_schema, TABLE)

    # ── Summary ──
    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print("  IMPORT COMPLETE — Suisse Romande")
    print(f"  Cantons:          {canton_labels}")
    print(f"  Communes queried: {total_communes}")
    print(f"  Raw elements:     {total_raw:,}")
    print(f"  Unique records:   {total_records:,}")
    print(f"  Rows upserted:    {total_upserted:,}")
    print(f"  Rows before:      {rows_before:,}" if rows_before is not None else "  Rows before:      unknown")
    print(f"  Rows after:       {rows_after:,}" if rows_after is not None else "  Rows after:       unknown")
    if rows_before is not None and rows_after is not None:
        delta = rows_after - rows_before
        print(f"  Net new:          {delta:,}")
    print(f"  Duration:         {elapsed / 60:.1f} min")
    print("=" * 60)

    if total_upserted == 0 and not stopped_early:
        print("  FAILED: Zero rows upserted!")
        sys.exit(1)

    # A run that stopped on its budget made real progress; the next run continues. Only a cycle
    # that refreshed every canton may stamp dataset freshness.
    print(f"OSM_CYCLE_COMPLETE={'false' if stopped_early else 'true'}")
    if stopped_early:
        print("  PARTIAL: budget reached, remaining cantons continue next run")
        return

    # ── Update dataset metadata ──
    update_dataset_meta(
        camelote_url, camelote_key, DATASET_CODE,
        record_count=rows_after,
        status="active",
    )


if __name__ == "__main__":
    main()
