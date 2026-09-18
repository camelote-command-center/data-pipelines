#!/usr/bin/env python3
"""
SITG LiDAR 2025 -> COPC mirror on R2 + manifest in bronze_ch.ge_lidar_2025_tiles.

ON DEMAND and COMMUNE-SCOPED by design. The canton is 5,505 x 250 m tiles at ~381 MB
each (~2.1 TB); nobody needs that to render one commune. Run it for the communes you are
building, nothing more.

  python lidar.py plan  <no_commune,...>          -> work/tiles.json
  python lidar.py fetch <shard> <nshards>          -> work/rows/<tile>.json (+ R2 upload)
  python lidar.py load                             -> upsert manifest + PATCH by code

Per tile: download .las.zip (the SITG zip is STORED, not compressed) -> PDAL -> COPC
(measured 326 MB -> 88 MB, 3.7x, all points and RGB kept) -> R2 -> stats row.

Idempotent: a tile already in the manifest at this version is skipped. R2
camelote-backups is WRITE-ONCE (object lock: re-uploading a key returns HTTP 409), so a
recompute must bump --version rather than overwrite.

Catalogue vs reality (measured on Genève-Cité, 2026-09-18) — trust the measurement:
  * LAS 1.2 / PDRF 3 WITH RGB (catalogue: LAS 1.4 / PDRF 1, no colour)
  * ~153 pts/m2 (catalogue: minimum 100)
  * class 16 is the STREET SURFACE (height above ground 0.01 +/- 0.05 m), not noise
"""
import glob, json, os, re, subprocess, sys, tempfile, time, urllib.request, zipfile, datetime

PAGE = "https://sitg.ge.ch/donnees/lidar-aerien-2025-03"
R2 = os.environ.get("R2_REMOTE", "r2:camelote-backups") + "/lamap-2025/lidar/sitg-2025"
VERSION = os.environ.get("VERSION", "v1")
WORK = os.environ.get("WORK", "work")
PG = os.environ.get("RE_LLM_PG_URI")
os.makedirs(f"{WORK}/rows", exist_ok=True)


def psql(sql, stdin=None):
    r = subprocess.run(["psql", PG, "-v", "ON_ERROR_STOP=1", "-q", "-tA", "-c" if stdin is None else "-f",
                        sql if stdin is None else "-"], input=stdin, capture_output=True, text=True,
                       env={**os.environ, "PGOPTIONS": "-c client_min_messages=error"})
    if r.returncode:
        sys.exit(f"psql failed: {r.stderr[:600]}")
    return r.stdout.strip()


def url_list():
    """The URL list is a .txt whose name carries a hash, so resolve it from the page every
    run instead of hard-coding a link that dies at the next republication."""
    html = urllib.request.urlopen(PAGE, timeout=60).read().decode("utf-8", "ignore")
    m = re.search(r'https://media\.sitg\.ge\.ch/lidar-aerien-2025-03-url-telechargement-[a-z0-9]+\.txt', html)
    if not m:
        sys.exit("could not find the LiDAR URL list on the SITG page — layout changed?")
    txt = urllib.request.urlopen(m.group(0), timeout=60).read().decode("utf-8", "ignore")
    return {u.rsplit("/", 1)[1][:-8]: u for u in (l.strip() for l in txt.splitlines()) if u.endswith(".las.zip")}


def plan(communes):
    ids = [int(c) for c in communes.split(",") if c.strip()]
    rows = psql(f"""
    WITH c AS (SELECT no_commune n, geometry g FROM bronze_ch.ge_communes_geo WHERE no_commune = ANY(ARRAY{ids}))
    SELECT x||'_'||y||'|'||string_agg(DISTINCT n::text, ',')
    FROM c, generate_series((floor(ST_XMin(g)/250)*250)::int,(floor(ST_XMax(g)/250)*250)::int,250) x,
            generate_series((floor(ST_YMin(g)/250)*250)::int,(floor(ST_YMax(g)/250)*250)::int,250) y
    WHERE ST_Intersects(g, ST_MakeEnvelope(x,y,x+250,y+250,2056)) GROUP BY x,y ORDER BY 1;""").splitlines()
    urls = url_list()
    done = set(psql(f"SELECT string_agg(tile_key, ',') FROM bronze_ch.ge_lidar_2025_tiles WHERE version='{VERSION}'").split(","))
    tiles, water = [], []
    for r in rows:
        key, comm = r.split("|")
        if key not in urls:
            water.append(key); continue
        if key in done:
            continue
        tiles.append({"key": key, "url": urls[key], "communes": [int(c) for c in comm.split(",")]})
    json.dump({"communes": ids, "tiles": tiles, "no_source": water}, open(f"{WORK}/tiles.json", "w"))
    print(f"communes {ids}: {len(rows)} tiles intersect, {len(tiles)} to fetch, {len(rows)-len(tiles)-len(water)} already mirrored, "
          f"{len(water)} with no SITG file (open water): {water}")


def stats(copc):
    m = json.loads(subprocess.run(["pdal", "info", "--stats", "--enumerate", "Classification", copc],
                                  check=True, capture_output=True, text=True).stdout)
    st = {s["name"]: s for s in m["stats"]["statistic"]}
    counts = {}
    for c in st["Classification"].get("counts", []):
        k, v = c.split("/"); counts[str(int(float(k)))] = int(v)
    n = int(st["X"]["count"])
    return {"points": n, "z_min": st["Z"]["minimum"], "z_max": st["Z"]["maximum"],
            "has_rgb": "Red" in st and st["Red"]["maximum"] > 0, "class_counts": counts}


def fetch(shard, nshards):
    meta = json.load(open(f"{WORK}/tiles.json"))
    mine = [t for i, t in enumerate(meta["tiles"]) if i % nshards == shard]
    t0 = time.time()
    for i, t in enumerate(mine, 1):
        out = f"{WORK}/rows/{t['key']}.json"
        if os.path.exists(out):
            continue
        with tempfile.TemporaryDirectory(dir=WORK) as d:
            z = f"{d}/t.zip"
            subprocess.run(["curl", "-sS", "--fail", "--retry", "4", "--retry-delay", "3", "-o", z, t["url"]], check=True)
            src_bytes = os.path.getsize(z)
            with zipfile.ZipFile(z) as zf:
                las = zf.extract(next(n for n in zf.namelist() if n.lower().endswith(".las")), d)
            os.remove(z)
            copc = f"{d}/{t['key']}.copc.laz"
            pipe = [{"type": "readers.las", "filename": las}, {"type": "writers.copc", "filename": copc}]
            subprocess.run(["pdal", "pipeline", "--stdin"], input=json.dumps(pipe), text=True, check=True, capture_output=True)
            s = stats(copc)
            if s["points"] < 1000:
                sys.exit(f"{t['key']}: only {s['points']} points — corrupt download? refusing to publish")
            key = f"{VERSION}/{t['key']}.copc.laz"
            up = subprocess.run(["rclone", "copyto", copc, f"{R2}/{key}"], capture_output=True, text=True)
            if up.returncode:
                if "409" in up.stderr or "ObjectLocked" in up.stderr:
                    sys.exit(f"R2 refused {key}: bucket is write-once. Bump VERSION to recompute.")
                sys.exit(f"rclone failed: {up.stderr[:300]}")
            e, n = map(int, t["key"].split("_"))
            row = {"tile_key": t["key"], "x_min": e, "y_min": n, "communes": t["communes"], "source_url": t["url"],
                   "source_bytes": src_bytes, "density": round(s["points"] / 62500, 1), "r2_key": f"lamap-2025/lidar/sitg-2025/{key}",
                   "copc_bytes": os.path.getsize(copc), **s}
            json.dump(row, open(out, "w"))
        print(f"[shard {shard}] {i}/{len(mine)} {t['key']} {row['points']:,} pts {row['density']} pts/m2 "
              f"{row['copc_bytes']/1e6:.0f} MB  {(time.time()-t0)/i:.0f}s/tile", flush=True)


def load():
    rows = [json.load(open(p)) for p in sorted(glob.glob(f"{WORK}/rows/*.json"))]
    if not rows:
        print("nothing new to load"); return
    q = lambda s: "'" + str(s).replace("'", "''") + "'"
    vals = ",\n".join(
        f"({q(r['tile_key'])},{r['x_min']},{r['y_min']},ARRAY{r['communes']}::int[],{q(r['source_url'])},{r['source_bytes']},"
        f"{r['points']},{r['density']},{r['z_min']:.2f},{r['z_max']:.2f},{str(r['has_rgb']).lower()},{q(json.dumps(r['class_counts']))}::jsonb,"
        f"{q(r['r2_key'])},{r['copc_bytes']},{q(VERSION)})" for r in rows)
    psql(None, stdin=f"""
INSERT INTO bronze_ch.ge_lidar_2025_tiles (tile_key,x_min,y_min,communes,source_url,source_bytes,points,density_pts_m2,
  z_min,z_max,has_rgb,class_counts,r2_key,copc_bytes,version) VALUES
{vals}
ON CONFLICT (tile_key) DO UPDATE SET communes=(SELECT array_agg(DISTINCT c ORDER BY c) FROM unnest(bronze_ch.ge_lidar_2025_tiles.communes||EXCLUDED.communes) c),
  source_url=EXCLUDED.source_url, source_bytes=EXCLUDED.source_bytes, points=EXCLUDED.points, density_pts_m2=EXCLUDED.density_pts_m2,
  z_min=EXCLUDED.z_min, z_max=EXCLUDED.z_max, has_rgb=EXCLUDED.has_rgb, class_counts=EXCLUDED.class_counts,
  r2_key=EXCLUDED.r2_key, copc_bytes=EXCLUDED.copc_bytes, version=EXCLUDED.version, acquired_at=now();""")
    total = int(psql("SELECT count(*) FROM bronze_ch.ge_lidar_2025_tiles"))
    print(f"upserted {len(rows)} tiles; manifest now {total}")
    url, key = os.environ.get("CAMELOTE_DATA_SUPABASE_URL"), os.environ.get("CAMELOTE_DATA_SUPABASE_SERVICE_KEY")
    if url and key:
        body = json.dumps({"last_acquired_at": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "record_count": total, "last_error": None}).encode()
        req = urllib.request.Request(f"{url}/rest/v1/datasets?code=eq.ge_lidar_2025_tiles", data=body, method="PATCH",
              headers={"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json", "Prefer": "return=representation"})
        with urllib.request.urlopen(req, timeout=60) as r:
            if not json.load(r):
                sys.exit("PATCH ge_lidar_2025_tiles matched 0 rows — dataset not registered")
        print("  PATCH ge_lidar_2025_tiles ok")


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "plan":
        plan(sys.argv[2])
    elif cmd == "fetch":
        fetch(int(sys.argv[2]), int(sys.argv[3]))
    elif cmd == "load":
        load()
