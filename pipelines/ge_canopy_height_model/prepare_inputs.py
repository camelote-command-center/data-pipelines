#!/usr/bin/env python3
"""
Stage 1 — build the inputs the tile workers need.

  tiles.json     357 GE tiles, each with a chosen DSM/DTM asset pair
  parcels.gpkg   72,949 GE parcels, ST_MakeValid-ed, with a stable global pid
  buildings.gpkg 82,899 GE building footprints BUFFERED BY 1.5 m

Run once before fanning out chm_pipeline.py.

TILE PAIRING — newest DSM with nearest DTM, deliberately NOT same-year.
swisstopo runs the two products on offset cycles, so over Geneva the
available vintages are DSM {2016,2019,2023,2025} and DTM {2019,2021}.
Requiring a shared year yields only 328 of 357 tiles and pins the canopy
to 2019. Measured on one tile: DTM 2021 vs 2019 differ by a median
0.085 m with only 3.25% of pixels moving >0.5 m (bare earth is static),
while DSM 2025 vs 2019 differ by >0.5 m on 46.6% of pixels (the canopy
grew). So the DSM vintage governs accuracy and the DTM vintage barely
matters — take the newest DSM, pair the nearest DTM, cover all 357.

BUILDING BUFFER — 1.5 m, and it is not optional. Cadastral footprints are
the GROUND outline; roofs overhang them. Unbuffered, a rim of roof edge
survives the mask and reads as tall vegetation hugging every building
(tile 2500-1117: 41,875 m2 of rim at >=3 m, mean height 14.5 m).
"""
import collections, json, os, re, subprocess, sys, time, urllib.request

BBOX = "5.95,46.13,6.32,46.37"
COLLECTIONS = {"dtm": "ch.swisstopo.swissalti3d",
               "dsm": "ch.swisstopo.swisssurface3d-raster"}
RES = "0.5"
ASSET_RX = re.compile(
    r'^[a-z0-9\-]+_(?P<year>\d{4})_(?P<e>\d{4})-(?P<n>\d{4})_(?P<res>[\d.]+)_\d+_\d+\.tif$')

PG = os.environ.get("RE_LLM_PG_URI")
if not PG:
    sys.exit("RE_LLM_PG_URI not set")


def stac(collection):
    url = (f"https://data.geo.admin.ch/api/stac/v1/collections/{collection}"
           f"/items?bbox={BBOX}&limit=100")
    items = []
    while url:
        for attempt in range(4):
            try:
                with urllib.request.urlopen(url, timeout=60) as r:
                    d = json.load(r)
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(2 * (attempt + 1))
        items.extend(d.get("features", []))
        url = next((l["href"] for l in d.get("links", []) if l.get("rel") == "next"), None)
    return items


def index(items):
    out = collections.defaultdict(lambda: collections.defaultdict(dict))
    for it in items:
        for name, a in it.get("assets", {}).items():
            m = ASSET_RX.match(name)
            if m:
                out[f"{m['e']}-{m['n']}"][m["year"]][m["res"]] = a["href"]
    return out


def ge_tiles():
    """1 km tiles intersecting the canton polygon. silver_ch.ref_communes is
    the boundary source (49 GE rows, SRID 2056; union area 282.5 km2 against
    the canton's true ~282 km2). Clip to the polygon, not the bbox — the
    bbox pulls in 72 Vaud/France tiles we would otherwise process for nothing."""
    sql = """
    WITH ge AS (SELECT ST_Union(geometry) g FROM silver_ch.ref_communes WHERE canton_code='GE'),
    b AS (SELECT floor(ST_XMin(g)/1000)::int x0, ceil(ST_XMax(g)/1000)::int x1,
                 floor(ST_YMin(g)/1000)::int y0, ceil(ST_YMax(g)/1000)::int y1, g FROM ge)
    SELECT x||'-'||y FROM b, generate_series(b.x0,b.x1) x, generate_series(b.y0,b.y1) y
    WHERE ST_Intersects(b.g, ST_MakeEnvelope(x*1000,y*1000,(x+1)*1000,(y+1)*1000,2056))
    ORDER BY 1;"""
    r = subprocess.run(["psql", PG, "-q", "-tA", "-c", sql],
                       capture_output=True, text=True,
                       env={**os.environ, "PGOPTIONS": "-c client_min_messages=error"})
    if r.returncode != 0:
        raise RuntimeError(r.stderr[:400])
    return sorted(x for x in (l.strip() for l in r.stdout.splitlines()) if x)


def main():
    out = os.environ.get("CHM_WORK", "work")
    os.makedirs(out, exist_ok=True)

    idx = {k: index(stac(c)) for k, c in COLLECTIONS.items()}
    print(f"STAC: dtm tiles={len(idx['dtm'])} dsm tiles={len(idx['dsm'])}")

    ge = ge_tiles()
    print(f"tiles intersecting canton GE: {len(ge)}")

    plan, missing = [], []
    for k in ge:
        ya = sorted(y for y, r in idx["dtm"].get(k, {}).items() if RES in r)
        ys = sorted(y for y, r in idx["dsm"].get(k, {}).items() if RES in r)
        if not ya or not ys:
            missing.append(k)
            continue
        dsm_y = max(ys)
        dtm_y = min(ya, key=lambda y: (abs(int(y) - int(dsm_y)), -int(y)))
        e, n = k.split("-")
        plan.append({"key": k, "e": int(e), "n": int(n),
                     "dsm_year": int(dsm_y), "dtm_year": int(dtm_y),
                     "dsm": idx["dsm"][k][dsm_y][RES], "dtm": idx["dtm"][k][dtm_y][RES]})
    if missing:
        print(f"WARNING: {len(missing)} GE tiles have no 0.5 m pair: {missing}")
    json.dump(plan, open(f"{out}/tiles.json", "w"), indent=0)
    print(f"planned {len(plan)} tiles")
    print("  DSM years:", dict(sorted(collections.Counter(p['dsm_year'] for p in plan).items())))
    print("  DTM years:", dict(sorted(collections.Counter(p['dtm_year'] for p in plan).items())))

    # pid = row_number over egrid — stable across runs, and the value burned
    # into the per-tile parcel raster so partials from different tiles merge.
    subprocess.run([
        "ogr2ogr", "-f", "GPKG", f"{out}/parcels.gpkg", f"PG:{PG}",
        "-nln", "parcels", "-nlt", "MULTIPOLYGON", "-a_srs", "EPSG:2056",
        "-lco", "SPATIAL_INDEX=YES", "-sql",
        "SELECT row_number() OVER (ORDER BY egrid)::int AS pid, egrid, no_commune, no_parcelle,"
        " ST_MakeValid(geometry) AS geometry, ST_Area(ST_MakeValid(geometry)) AS poly_area_m2"
        " FROM bronze_ch.ge_plots_geo"], check=True)
    print(f"wrote {out}/parcels.gpkg")

    subprocess.run([
        "ogr2ogr", "-f", "GPKG", f"{out}/buildings.gpkg", f"PG:{PG}",
        "-nln", "bldg", "-nlt", "MULTIPOLYGON", "-a_srs", "EPSG:2056",
        "-lco", "SPATIAL_INDEX=YES", "-sql",
        "SELECT 1::int AS b, ST_Buffer(ST_MakeValid(geometry), 1.5) AS geometry"
        " FROM bronze_ch.ge_buildings_geo WHERE geometry IS NOT NULL"], check=True)
    print(f"wrote {out}/buildings.gpkg")


if __name__ == "__main__":
    main()
