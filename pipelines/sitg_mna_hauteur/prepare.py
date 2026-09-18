#!/usr/bin/env python3
"""
Stage 1 of sitg_mna_hauteur: decide whether to run, then build the worker inputs.

Source: SITG MNA_HAUTEUR_2025_03 — ONE canton-wide Cloud-Optimized GeoTIFF, 20 cm,
Float32, EPSG:2056, 13 GB, read in place with HTTP range requests (/vsicurl/). We never
download the whole file: each worker reads only its 1 km windows.

Freshness: SITG gives no cadence ("Irrégulière"). The only honest change signal is the
file's Last-Modified header, so a scheduled run that sees the same Last-Modified as the
last successful run exits early (skip=true) and PATCHes nothing — a skipped run must not
stamp freshness it did not earn.

Outputs (work/): tiles.json, parcels.gpkg (global pid), bldg_buf.gpkg (footprints +1.5 m,
vegetation mask), bldg_fp.gpkg (footprints -0.4 m with id/egid, roof-height pixels).
"""
import email.utils, json, os, subprocess, sys, urllib.request

SRC = "https://ge.ch/sitg/geodata/SITG/COG-DATA/MNA_HAUTEUR_2025_03.tif"
PG = os.environ.get("RE_LLM_PG_URI") or sys.exit("RE_LLM_PG_URI not set")
FORCE = os.environ.get("FORCE", "false").lower() == "true"
COMMUNE = os.environ.get("COMMUNE", "").strip()      # optional no_commune filter, for tests
WORK = os.environ.get("WORK", "work")
os.makedirs(WORK, exist_ok=True)


def psql(sql):
    r = subprocess.run(["psql", PG, "-v", "ON_ERROR_STOP=1", "-q", "-tAc", sql],
                       capture_output=True, text=True, env={**os.environ, "PGOPTIONS": "-c client_min_messages=error"})
    if r.returncode:
        sys.exit(f"psql failed: {r.stderr[:400]}")
    return r.stdout.strip()


def out(k, v):
    print(f"{k}={v}")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"{k}={v}\n")


def ogr(dest, layer, sql):
    r = subprocess.run(["ogr2ogr", "-f", "GPKG", dest, f"PG:{PG}", "-nln", layer, "-nlt", "MULTIPOLYGON",
                        "-a_srs", "EPSG:2056", "-lco", "SPATIAL_INDEX=YES", "-overwrite", "-sql", sql],
                       capture_output=True, text=True)
    if r.returncode:
        # surface ogr2ogr's own message; a bare CalledProcessError hid the cause on the first CI run
        sys.exit(f"ogr2ogr failed for {dest}: {(r.stderr or r.stdout)[-1500:]}")
    print(f"  wrote {dest}", flush=True)


with urllib.request.urlopen(urllib.request.Request(SRC, method="HEAD"), timeout=60) as r:
    size = int(r.headers["Content-Length"])
    lm = email.utils.parsedate_to_datetime(r.headers["Last-Modified"]).isoformat()
print(f"source {size/1e9:.2f} GB, Last-Modified {lm}")

last_ok = psql("SELECT coalesce(max(source_last_modified)::text,'') FROM bronze_ch.ge_mna_hauteur_runs WHERE status='ok'")
if last_ok and psql(f"SELECT '{last_ok}'::timestamptz = '{lm}'::timestamptz") == "t" and not FORCE and not COMMUNE:
    print("unchanged since last successful run -> skip")
    out("skip", "true")
    sys.exit(0)
out("skip", "false")

where = f"no_commune={int(COMMUNE)}" if COMMUNE else "canton_code='GE'"
src = ("SELECT geometry g FROM bronze_ch.ge_communes_geo WHERE no_commune=" + str(int(COMMUNE))) if COMMUNE \
      else "SELECT ST_Union(geometry) g FROM silver_ch.ref_communes WHERE canton_code='GE'"
tiles = psql(f"""
WITH ge AS ({src}),
b AS (SELECT floor(ST_XMin(g)/1000)::int x0, floor(ST_XMax(g)/1000)::int x1,
             floor(ST_YMin(g)/1000)::int y0, floor(ST_YMax(g)/1000)::int y1, g FROM ge)
SELECT string_agg(x||'-'||y, ',' ORDER BY x, y) FROM b, generate_series(b.x0,b.x1) x, generate_series(b.y0,b.y1) y
WHERE ST_Intersects(b.g, ST_MakeEnvelope(x*1000,y*1000,(x+1)*1000,(y+1)*1000,2056));""").split(",")
json.dump({"source": SRC, "bytes": size, "last_modified": lm, "tiles": tiles}, open(f"{WORK}/tiles.json", "w"))
print(f"tiles: {len(tiles)}")

ogr(f"{WORK}/parcels.gpkg", "parcels",
    "SELECT row_number() OVER (ORDER BY egrid)::int AS pid, egrid, no_commune, no_parcelle, "
    "ST_Multi(ST_CollectionExtract(ST_MakeValid(geometry),3)) AS geometry FROM bronze_ch.ge_plots_geo")
ogr(f"{WORK}/bldg_buf.gpkg", "bldg",
    "SELECT 1::int AS b, ST_Multi(ST_Buffer(ST_MakeValid(geometry),1.5)) AS geometry FROM bronze_ch.ge_buildings_geo")
ogr(f"{WORK}/bldg_fp.gpkg", "bldg",
    "SELECT id::int AS fid_b, egid, no_commune, no_parcelle, "
    "ST_Multi(ST_CollectionExtract(CASE WHEN ST_IsEmpty(ST_Buffer(ST_MakeValid(geometry),-0.4)) "
    "THEN ST_MakeValid(geometry) ELSE ST_Buffer(ST_MakeValid(geometry),-0.4) END,3)) AS geometry "
    "FROM bronze_ch.ge_buildings_geo")
print("inputs ready")
