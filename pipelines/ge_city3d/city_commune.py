#!/usr/bin/env python3
"""
Commune-scale 3D city -> 3D Tiles 1.1 (glTF content) for CesiumJS.

One GLB per 250 m tile (same grid as the SITG LiDAR). Each building belongs to exactly ONE tile — the
tile holding its base centroid — so nothing is duplicated at tile edges. No terrain mesh: Cesium draws the
swisstopo terrain + imagery, and a second ground would z-fight. Buildings and trees are placed at their
true heights instead.

  buildings  SITG bati3d LOD2 planar faces, flat shading
  roofs      SWISSIMAGE 10 cm 2023 (JPEG), planar XY projection
  facades    procedural, driven by RegBL per EGID: era (gbaup) -> style, gastw -> floor height,
             gklas -> ground-floor shopfront; colour varies per building within a style
  trees      LiDAR class 5 canopy peaks (>= 4 m, >= 3 m apart), trunk + 5 jittered crown blobs, sized to the
             measured height and neighbour spacing

GEOREFERENCING — each tile carries a 4x4 `transform` built numerically from PROJ:
  O  = ECEF(E0, N0, H0)            LV95 -> WGS84 lon/lat; LN02 height used AS ellipsoidal height, to match
                                   swisstopo's Cesium terrain (measured: it does the same, 4 cm agreement)
  ex = ECEF(E0+1, N0, H0) - O      one metre east  on the LV95 grid  (includes meridian convergence + scale)
  ey = ECEF(E0, N0+1, H0) - O      one metre north on the LV95 grid
  ez = ECEF(E0, N0, H0+1) - O      one metre up
glTF is Y-up; Cesium rotates glTF content to Z-up, so vertices are written as (dE, dH, -dN).

Usage: city_commune.py <no_commune> <copc_dir> <out_dir> [tile_key ...]
"""
import glob, io, json, math, os, subprocess, sys, time
import numpy as np
import trimesh
from glb_writer import Builder
import mapbox_earcut as earcut
from PIL import Image, ImageDraw
from osgeo import gdal
from scipy import ndimage
from scipy.spatial import cKDTree
from shapely.geometry import Polygon as SPoly, box as sbox
from shapely import contains_xy
from pyproj import Transformer, network

gdal.UseExceptions()
gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
network.set_network_enabled(True)
NO_COMMUNE, COPC_DIR, OUT = int(sys.argv[1]), sys.argv[2], sys.argv[3]
ONLY = set(sys.argv[4:])
PG = os.environ.get("RE_LLM_PG_URI") or sys.exit("RE_LLM_PG_URI not set")
TILE = 250.0
ROOF_PX = 0.20                     # roof texture (m/px) for the 250 m leaf tiles; 0.1 native
PARENT = 1000.0                    # LOD parent block size (m)
PARENT_ROOF_PX = 1.0               # roof texture for the distant-view parents
PARENT_GEOM_ERR = 10.0             # m; with MSSE 8-16 the 20 cm leaves swap in within ~0.5 km
os.makedirs(OUT, exist_ok=True)
T0 = time.time()
log = lambda m: print(f"[{time.time()-T0:6.0f}s] {m}", flush=True)
# MEASURED 2026-09-18: swisstopo's Cesium terrain (3d.geo.admin.ch/ch.swisstopo.terrain.3d) stores LN02 heights
# AS IF they were ellipsoidal — it matches swissALTI3D to 4 cm at three Geneva points. Applying the real geoid
# (+49.8 m) makes every building float 50 m above it. So: horizontal LV95 -> WGS84, height passed through as-is.
TO_GEOG = Transformer.from_crs("EPSG:2056", "EPSG:4326", always_xy=True)
TO_ECEF = Transformer.from_crs("EPSG:4979", "EPSG:4978", always_xy=True)


def psql_json(sql):
    r = subprocess.run(["psql", PG, "-q", "-tA", "-c", sql], capture_output=True, text=True,
                       env={**os.environ, "PGOPTIONS": "-c client_min_messages=error"})
    if r.returncode:
        sys.exit(r.stderr[:800])
    return json.loads(r.stdout or "[]")


def ecef(e, n, h):
    lon, lat = TO_GEOG.transform(e, n)
    return np.array(TO_ECEF.transform(lon, lat, h))


# ------------------------------------------------------------------ inputs for the whole commune (one query each)
env = f"(SELECT geometry FROM bronze_ch.ge_communes_geo WHERE no_commune={NO_COMMUNE})"
bldg = psql_json(f"""
SELECT coalesce(json_agg(json_build_object('egid', b.egid::bigint, 'cx', ST_X(ST_Centroid(b.geom)), 'cy', ST_Y(ST_Centroid(b.geom)),
       'z', ST_ZMin(b.geom))), '[]')
FROM bronze_ch.ge_cad_bati3d_base b WHERE b.egid IS NOT NULL AND ST_Intersects(ST_Force2D(b.geom), {env});""")
log(f"commune {NO_COMMUNE}: {len(bldg)} bati3d buildings")
egids = sorted({b["egid"] for b in bldg})
regbl = {r["egid"]: r for r in psql_json(f"""
SELECT coalesce(json_agg(json_build_object('egid', egid::bigint,
  'gastw', NULLIF(gastw::text,'')::numeric::int, 'gbaup', NULLIF(gbaup::text,'')::numeric::int,
  'gklas', NULLIF(gklas::text,'')::numeric::int)), '[]')
FROM bronze_ch.bfs_rebl_buildings WHERE egid::bigint = ANY(ARRAY{egids}::bigint[]);""")}
base_z = {b["egid"]: b["z"] for b in bldg}
log(f"RegBL attributes for {len(regbl)} of {len(egids)} buildings")
tile_of = {}
for b in bldg:
    tile_of.setdefault(b["egid"], f"{int(b['cx'] // TILE * TILE)}_{int(b['cy'] // TILE * TILE)}")
tiles = sorted(set(tile_of.values()))
if ONLY:
    tiles = [t for t in tiles if t in ONLY]
log(f"{len(tiles)} tiles hold buildings")

# ------------------------------------------------------------------ facade styles (same rules as block v3/v4)
STYLES = {
    "belle":   dict(bay=3.3, floor=3.5, palettes=[(226, 214, 190), (214, 203, 182), (232, 224, 206), (207, 190, 162)]),
    "inter":   dict(bay=3.0, floor=3.1, palettes=[(214, 190, 150), (222, 206, 176), (200, 184, 160)]),
    "postwar": dict(bay=2.9, floor=2.9, palettes=[(196, 190, 178), (184, 178, 166), (206, 198, 184)]),
    "modern":  dict(bay=2.7, floor=3.4, palettes=[(92, 108, 120), (70, 84, 96), (120, 132, 140)]),
}


NEUTRAL_RGB = (246, 246, 246)   # facade textures are neutral; the building colour comes from a per-vertex tint


def style_of(egid):
    r = regbl.get(egid, {})
    era, cls = r.get("gbaup") or 8011, r.get("gklas")
    if era >= 8015 or (cls == 1220 and era >= 8014):
        return "modern"
    if era >= 8013:
        return "postwar"
    return "inter" if era == 8012 else "belle"


def shopfront(egid):
    r = regbl.get(egid, {})
    return r.get("gklas") in (1211, 1220, 1230) or (r.get("gbaup") or 8011) <= 8012


def noise(im, amp, n, seed):
    rng = np.random.default_rng(seed); W, H = im.size; px = im.load()
    for _ in range(n):
        x, y = int(rng.integers(0, W)), int(rng.integers(0, H)); c = px[x, y]; k = int(rng.integers(-amp, amp))
        px[x, y] = tuple(max(0, min(255, v + k)) for v in c)
    return im


def jpeg(im, q=85):
    """Re-open as JPEG so trimesh embeds JPEG, not PNG (5-10x smaller)."""
    b = io.BytesIO(); im.convert("RGB").save(b, "JPEG", quality=q); b.seek(0)
    return Image.open(b)


def upper_tex(style, col, seed):
    W, H = 256, 272
    im = Image.new("RGB", (W, H), col); d = ImageDraw.Draw(im)
    lite = tuple(min(255, c + 18) for c in col); dark = tuple(max(0, c - 28) for c in col)
    glass, sky = (46, 58, 70), (104, 122, 140)
    if style == "belle":
        d.rectangle([0, 0, W, 14], fill=lite); d.line([(0, 14), (W, 14)], fill=dark, width=3)
        x0, x1, y0, y1 = 86, 170, 46, 236
        d.rectangle([x0 - 12, y0 - 16, x1 + 12, y1 + 6], fill=lite)
        d.rectangle([x0 - 16, y0 - 22, x1 + 16, y0 - 12], fill=dark)
        d.rectangle([x0, y0, x1, y1], fill=glass); d.rectangle([x0, y0, x1, y0 + 55], fill=sky)
        d.line([(128, y0), (128, y1)], fill=lite, width=5)
        if seed % 2:
            d.rectangle([x0 - 14, y1 - 44, x1 + 14, y1 + 2], outline=(34, 34, 34), width=3)
            for xx in range(x0 - 10, x1 + 12, 9): d.line([(xx, y1 - 44), (xx, y1)], fill=(34, 34, 34), width=2)
        else:
            for sx in (x0 - 40, x1 + 10): d.rectangle([sx, y0, sx + 30, y1], fill=(92, 104, 88) if seed % 3 else (120, 110, 96))
    elif style == "inter":
        d.line([(0, 6), (W, 6)], fill=dark, width=2)
        x0, x1, y0, y1 = 80, 176, 58, 222
        d.rectangle([x0 - 6, y0 - 6, x1 + 6, y1 + 10], fill=lite)
        d.rectangle([x0, y0, x1, y1], fill=glass); d.rectangle([x0, y0, x1, y0 + 44], fill=sky)
        d.line([(128, y0), (128, y1)], fill=lite, width=4); d.line([(x0, 120), (x1, 120)], fill=lite, width=4)
    elif style == "postwar":
        d.rectangle([0, 0, W, 60], fill=dark)
        d.rectangle([0, 90, W, 230], fill=glass); d.rectangle([0, 90, W, 130], fill=sky)
        for xx in range(0, W, 64): d.line([(xx, 90), (xx, 230)], fill=lite, width=6)
    else:
        d.rectangle([0, 0, W, H], fill=tuple(int(c * 0.9) for c in col))
        for xx in range(0, W, 128): d.line([(xx, 0), (xx, H)], fill=(190, 196, 200), width=5)
        for yy in (0, 70): d.line([(0, yy), (W, yy)], fill=(190, 196, 200), width=5)
        d.rectangle([4, 140, W - 4, 150], fill=(140, 160, 176))
    return jpeg(noise(im, 9, 1800, seed).transpose(Image.FLIP_TOP_BOTTOM))


def ground_tex(col, seed):
    W, H = 256, 300
    im = Image.new("RGB", (W, H), tuple(max(0, c - 20) for c in col)); d = ImageDraw.Draw(im)
    d.rectangle([0, 0, W, 22], fill=tuple(min(255, c + 10) for c in col))
    d.rectangle([14, 26, W - 14, 58], fill=[(150, 40, 40), (40, 70, 60), (60, 60, 70), (170, 120, 50)][seed % 4])
    d.rectangle([22, 66, W - 22, H - 8], fill=(34, 42, 50)); d.rectangle([22, 66, W - 22, 120], fill=(88, 104, 118))
    d.line([(W // 2, 66), (W // 2, H - 8)], fill=(20, 20, 20), width=6)
    return jpeg(noise(im, 8, 1200, seed).transpose(Image.FLIP_TOP_BOTTOM))


def slate_tex():
    return jpeg(noise(Image.new("RGB", (128, 128), (70, 74, 80)), 14, 3000, 7))


def orient(P, tri, want):
    """Flip every triangle whose normal points against `want` (3-vector, or callable(normals)->bool mask of
    triangles to flip). Clipping (shapely intersection/buffer) can reverse ring orientation, which turns
    faces inward and makes the sun light them from behind — measured as dark facades after the v2 rewrite."""
    t = P[tri]
    n = np.cross(t[:, 1] - t[:, 0], t[:, 2] - t[:, 0])
    flip = want(n) if callable(want) else (n @ np.asarray(want, dtype=float)) < 0
    tri = tri.copy()
    tri[flip] = tri[flip][:, [0, 2, 1]]
    return tri, int(flip.sum())


def newell(r):
    n = np.zeros(3)
    for p, q in zip(r[:-1], r[1:]):
        n += [(p[1] - q[1]) * (p[2] + q[2]), (p[2] - q[2]) * (p[0] + q[0]), (p[0] - q[0]) * (p[1] + q[1])]
    return n


def triangulate(rings):
    outer = rings[0]
    n = newell(outer)
    if np.linalg.norm(n) < 1e-9:
        return None, None
    n /= np.linalg.norm(n)
    u = np.cross(n, [0, 0, 1] if abs(n[2]) < 0.9 else [1, 0, 0]); u /= np.linalg.norm(u); v = np.cross(n, u)
    rr = [r[:-1] if np.allclose(r[0], r[-1]) else r for r in rings]
    P = np.vstack(rr); ends = np.cumsum([len(r) for r in rr]).astype(np.uint32)
    try:
        tri = earcut.triangulate_float64(np.c_[P @ u, P @ v], ends).reshape(-1, 3)
    except Exception:
        return None, None
    return (P, tri) if len(tri) else (None, None)


def wall_parts(rings, egid, TEX):
    outer = rings[0]
    n = newell(outer)
    if np.linalg.norm(n) < 1e-9:
        return []
    n /= np.linalg.norm(n)
    if abs(n[2]) > 0.35:
        return None
    nh = n[:2] / np.linalg.norm(n[:2]); d = np.array([-nh[1], nh[0]]); c = float(outer[0][:2] @ nh)
    to2 = lambda r: np.c_[r[:, :2] @ d, r[:, 2]]
    try:
        poly = SPoly(to2(rings[0]), [to2(r) for r in rings[1:]]).buffer(0)
    except Exception:
        return []
    st = style_of(egid); S = STYLES[st]
    ri = (int(egid) * 2654435761) % 997
    col = S["palettes"][ri % len(S["palettes"])]
    zb = base_z.get(egid, float(outer[:, 2].min())); zt = float(outer[:, 2].max())
    floors = (regbl.get(egid) or {}).get("gastw")
    gf = 4.2 if shopfront(egid) else 0.0
    fh = S["floor"]
    if floors and floors > 1 and zt - zb > 4:
        fh = max(2.6, min(4.2, (zt - zb - gf) / (floors - (1 if gf else 0))))
    out, zs = [], zb + gf
    for part, clip in (("gf", sbox(-1e9, -1e9, 1e9, zs)), ("up", sbox(-1e9, zs, 1e9, 1e9))):
        if part == "gf" and gf == 0:
            continue
        g = poly.intersection(clip)
        polys = [g] if g.geom_type == "Polygon" else [x for x in getattr(g, "geoms", []) if x.geom_type == "Polygon"]
        for gp in polys:
            if gp.is_empty or gp.area < 0.05:
                continue
            rr = [np.asarray(gp.exterior.coords)[:-1]] + [np.asarray(h.coords)[:-1] for h in gp.interiors]
            P2 = np.vstack(rr); ends = np.cumsum([len(r) for r in rr]).astype(np.uint32)
            try:
                tri = earcut.triangulate_float64(P2, ends).reshape(-1, 3)
            except Exception:
                continue
            P3 = np.c_[np.outer(P2[:, 0], d) + c * nh, P2[:, 1]]
            tri, _ = orient(P3, tri, np.r_[nh, 0.0])
            if part == "gf":
                key = f"gf_{ri % 2}"
                TEX.setdefault(key, ground_tex(NEUTRAL_RGB, ri % 2)); uv = np.c_[P2[:, 0] / 5.0, (P2[:, 1] - zb) / gf]
            else:
                key = f"up_{st}_{ri % 2}"
                TEX.setdefault(key, upper_tex(st, NEUTRAL_RGB, ri % 2)); uv = np.c_[P2[:, 0] / S["bay"], (P2[:, 1] - zs) / fh]
            out.append((key, P3, tri, uv, col))
    return out


def _tree_variant(seed):
    """Unit tree (Y-up): trunk to y=0.45, crown of 3 jittered low-poly blobs in y 0.35..1, radius 0.5."""
    r = np.random.default_rng(seed)
    parts, cols = [], []
    trunk = trimesh.creation.cylinder(radius=0.035, height=0.47, sections=6)
    trunk.apply_translation([0, 0, 0.235])
    parts.append(trunk); cols.append(np.array([84, 66, 50]))
    green = np.array([[62, 92, 44], [74, 104, 52], [58, 86, 46]][seed % 3])
    for _ in range(3):
        b = trimesh.creation.icosphere(subdivisions=1)
        s_ = r.uniform(0.28, 0.4)
        b.apply_scale([s_, s_, 0.3 * r.uniform(0.8, 1.1)])
        b.apply_translation([r.normal(0, 0.12), r.normal(0, 0.12), r.uniform(0.6, 0.72)])
        b.vertices += r.normal(0, 0.02, b.vertices.shape)
        parts.append(b); cols.append(green)
    P, C = [], []
    for m, c in zip(parts, cols):
        tri = m.vertices[m.faces].reshape(-1, 3)            # non-indexed (flat faces)
        P.append(np.c_[tri[:, 0], tri[:, 2], -tri[:, 1]])    # Z-up -> glTF Y-up
        cc = np.tile(np.r_[c / 255.0, 1.0], (len(tri), 1))
        if c[1] > c[0] + 10:
            cc[:, :3] *= np.repeat(r.uniform(0.82, 1.12, (len(tri) // 3, 1)), 3, axis=0)
        C.append(np.clip(cc, 0, 1))
    return np.vstack(P).astype(np.float32), np.vstack(C).astype(np.float32)


TREE_VARIANTS = [_tree_variant(i) for i in range(3)]


# ------------------------------------------------------------------ one tile
def build_tile(key, size=TILE, roof_px=None, min_tree=4.0, tag=""):
    roof_px = roof_px or ROOF_PX
    E0, N0 = map(float, key.split("_"))
    my = [e for e, t in tile_of.items()
          if E0 <= float(t.split("_")[0]) < E0 + size and N0 <= float(t.split("_")[1]) < N0 + size]
    faces = []
    for layer, kind in (("toit", "roof"), ("facade", "wall"), ("sp_toit", "roof"), ("sp_facade", "dormer"), ("base", "base")):
        for r in psql_json(f"""SELECT coalesce(json_agg(json_build_object('e',egid::bigint,'g',ST_AsGeoJSON((d).geom)::json)),'[]')
            FROM (SELECT egid, ST_Dump(geom) d FROM bronze_ch.ge_cad_bati3d_{layer} WHERE egid = ANY(ARRAY{my})) x"""):
            faces.append((kind, r["e"], [np.asarray(c, dtype=float) for c in r["g"]["coordinates"]]))
    if not faces:
        return None
    allxyz = np.vstack([r for _, _, rr in faces for r in rr])
    cx0, cy0 = min(allxyz[:, 0].min(), E0) - 2, min(allxyz[:, 1].min(), N0) - 2
    cx1, cy1 = max(allxyz[:, 0].max(), E0 + size) + 2, max(allxyz[:, 1].max(), N0 + size) + 2
    H0 = float(allxyz[:, 2].min())

    # roof texture: SWISSIMAGE 10 cm, resampled to ROOF_PX
    src = [f"/vsicurl/https://data.geo.admin.ch/ch.swisstopo.swissimage-dop10/swissimage-dop10_2023_{e}-{n}/"
           f"swissimage-dop10_2023_{e}-{n}_0.1_2056.tif"
           for e in range(int(cx0 // 1000), int(cx1 // 1000) + 1) for n in range(int(cy0 // 1000), int(cy1 // 1000) + 1)]
    vrt = gdal.BuildVRT(f"/vsimem/{key}.vrt", src)
    ds = gdal.Translate(f"/vsimem/{key}.tif", vrt, projWin=[cx0, cy1, cx1, cy0], xRes=roof_px, yRes=roof_px,
                        bandList=[1, 2, 3], resampleAlg="average")
    ortho = jpeg(Image.fromarray(np.dstack([ds.GetRasterBand(i).ReadAsArray() for i in (1, 2, 3)]).astype(np.uint8)), 82)
    del ds, vrt
    gdal.Unlink(f"/vsimem/{key}.vrt"); gdal.Unlink(f"/vsimem/{key}.tif")
    uv_o = lambda x, y: np.c_[(x - cx0) / (cx1 - cx0), (y - cy0) / (cy1 - cy0)]

    TEX, groups = {}, {}
    feat = {e: i for i, e in enumerate(sorted({e for _, e, _ in faces}))}
    def add(k, P, tri, uv, egid, col=(255, 255, 255)):
        V, UV, FID, TN = groups.setdefault(k, ([], [], [], []))
        V.append(P[tri].reshape(-1, 3)); UV.append(np.asarray(uv)[tri].reshape(-1, 2))
        FID.append(np.full(len(tri) * 3, feat[egid], dtype=np.float32))
        TN.append(np.tile(np.r_[np.asarray(col, dtype=float) / 255.0, 1.0], (len(tri) * 3, 1)))
    for kind, egid, rings in faces:
        if kind == "wall":
            parts = wall_parts(rings, egid, TEX)
            if parts is not None:
                for k, P3, tri, uv, col in parts:
                    add(k, P3, tri, uv, egid, col)
                continue
            kind = "roof"
        P, tri = triangulate(rings)
        if P is None:
            continue
        if kind == "roof":
            tri, _ = orient(P, tri, lambda n: n[:, 2] < 0)
        elif kind == "base":
            tri, _ = orient(P, tri, lambda n: n[:, 2] > 0)
        if kind == "dormer":
            TEX.setdefault("slate", slate_tex()); add("slate", P, tri, np.c_[(P[:, 0] + P[:, 2]) / 2, (P[:, 1] + P[:, 2]) / 2], egid)
        elif kind == "base":
            add("base", P, tri, np.zeros((len(P), 2)), egid)
        else:
            add("roof", P, tri, uv_o(P[:, 0], P[:, 1]), egid)

    local = lambda p: np.c_[p[:, 0] - E0, p[:, 2] - H0, -(p[:, 1] - N0)]     # glTF Y-up
    B = Builder()
    prims = []
    for k, (V, UV, FID, TN) in groups.items():
        if k == "roof":
            m = B.material("roof", ortho)
        elif k == "base":
            m = B.material("base", color=(0.22, 0.22, 0.23, 1.0))
        else:
            m = B.material(k, TEX[k])
        tint = np.vstack(TN) if (k.startswith("up_") or k.startswith("gf_")) else None
        prims.append((m, local(np.vstack(V)), None if k == "base" else np.vstack(UV), np.concatenate(FID), tint))
    B.building_mesh(prims, [int(e) for e in sorted(feat, key=feat.get)])

    # trees: LiDAR class 5 canopy peaks inside THIS tile only (no duplicates across tiles)
    ntrees = 0
    copcs = [f for f in glob.glob(f"{COPC_DIR}/*.copc.laz")
             if (lambda e, n: e < E0 + size + 10 and e + 250 > E0 - 10 and n < N0 + size + 10 and n + 250 > N0 - 10)
             (*map(float, os.path.basename(f)[:-9].split("_")))]
    if copcs:
        bnds = f"([{E0},{E0 + size}],[{N0},{N0 + size}])"
        def raster(cls, how, name):
            pipe = [{"type": "readers.copc", "filename": f, "bounds": bnds} for f in copcs] + [
                    {"type": "filters.merge"}, {"type": "filters.range", "limits": cls},
                    {"type": "writers.gdal", "filename": name, "resolution": 0.5, "output_type": how,
                     "bounds": bnds, "data_type": "float32", "nodata": -9999}]
            subprocess.run(["pdal", "pipeline", "--stdin"], input=json.dumps(pipe), text=True, check=True, capture_output=True)
            dd = gdal.Open(name); a = np.flipud(dd.GetRasterBand(1).ReadAsArray().astype(float)); del dd
            os.remove(name)
            return a
        dtm = raster("Classification[2:2],Classification[16:16]", "mean", f"/tmp/_dtm_{key}.tif")
        m = dtm == -9999
        if not m.all():
            idx = ndimage.distance_transform_edt(m, return_distances=False, return_indices=True); dtm = dtm[tuple(idx)]
            veg = raster("Classification[5:5]", "max", f"/tmp/_veg_{key}.tif")
            h = min(veg.shape[0], dtm.shape[0]); w = min(veg.shape[1], dtm.shape[1]); veg, dtm = veg[:h, :w], dtm[:h, :w]
            chm = ndimage.gaussian_filter(np.clip(np.where(veg == -9999, 0, veg - dtm), 0, 45), 1.0)
            pi, pj = np.nonzero((chm == ndimage.maximum_filter(chm, size=7)) & (chm >= min_tree))
            TX, TY, TH = E0 + (pj + .5) * .5, N0 + (pi + .5) * .5, chm[pi, pj]
            fps = psql_json(f"""SELECT coalesce(json_agg(ST_AsGeoJSON(ST_Buffer(geometry,0.3))::json),'[]')
                FROM bronze_ch.ge_buildings_geo WHERE geometry && ST_MakeEnvelope({E0},{N0},{E0+TILE},{N0+TILE},2056)""")
            if fps and len(TX):
                from shapely.geometry import shape
                from shapely.ops import unary_union
                keep = ~contains_xy(unary_union([shape(g) for g in fps]), TX, TY)
                TX, TY, TH, pi, pj = TX[keep], TY[keep], TH[keep], pi[keep], pj[keep]
            if len(TX):
                dnn = cKDTree(np.c_[TX, TY]).query(np.c_[TX, TY], k=2)[0][:, 1] if len(TX) > 1 else np.full(len(TX), 8.0)
                TR = np.clip(np.minimum(0.55 * dnn, 0.22 * TH + 1.2), 1.2, 9.0); TZ = dtm[pi, pj]
                rng = np.random.default_rng(int(E0 + N0))
                variant = rng.integers(0, len(TREE_VARIANTS), len(TX))
                ang = rng.uniform(0, 2 * np.pi, len(TX))
                for vi, (VP, VC) in enumerate(TREE_VARIANTS):
                    sel = variant == vi
                    if not sel.any():
                        continue
                    tr = local(np.c_[TX[sel], TY[sel], TZ[sel]])
                    sc = np.c_[2 * TR[sel], TH[sel], 2 * TR[sel]]
                    rot = np.c_[np.zeros(sel.sum()), np.sin(ang[sel] / 2), np.zeros(sel.sum()), np.cos(ang[sel] / 2)]
                    B.instanced(f"trees_{vi}", VP, VC, tr, sc, rot)
                ntrees = len(TX)

    path = f"{OUT}/{tag}{key}.glb"
    B.save(path)
    # georeferencing frame for this tile
    O = ecef(E0, N0, H0); ex = ecef(E0 + 1, N0, H0) - O; ey = ecef(E0, N0 + 1, H0) - O; ez = ecef(E0, N0, H0 + 1) - O
    transform = [*ex, 0, *ey, 0, *ez, 0, *O, 1]                      # column-major 4x4
    zmax = float(allxyz[:, 2].max()) - H0 + 40
    cxl, cyl = (cx0 + cx1) / 2 - E0, (cy0 + cy1) / 2 - N0
    hx, hy = (cx1 - cx0) / 2, (cy1 - cy0) / 2
    box = [cxl, cyl, zmax / 2, hx, 0, 0, 0, hy, 0, 0, 0, zmax / 2 + 5]     # local Z-up frame (after glTF Y-up -> Z-up)
    return {"key": tag + key, "uri": f"{tag}{key}.glb", "size": size, "transform": transform, "box": box, "buildings": len(my), "trees": ntrees,
            "bytes": os.path.getsize(path)}


results = []
for i, k in enumerate(tiles if os.environ.get("SKIP_LEAVES") != "1" else [], 1):
    r = build_tile(k)
    if r:
        results.append(r)
        log(f"{i}/{len(tiles)} {k}: {r['buildings']} buildings, {r['trees']} trees, {r['bytes']/1e6:.1f} MB")
parents = sorted({f"{int(float(k.split('_')[0]) // PARENT * PARENT)}_{int(float(k.split('_')[1]) // PARENT * PARENT)}" for k in tiles})
if os.environ.get("SKIP_PARENTS") != "1":
    for k in parents:
        r = build_tile(k, size=PARENT, roof_px=PARENT_ROOF_PX, min_tree=8.0, tag="L1_")
        if r:
            results.append(r)
            log(f"parent {k}: {r['buildings']} buildings, {r['trees']} trees, {r['bytes']/1e6:.1f} MB")
import hashlib
_tag = ("parents_" if os.environ.get("SKIP_LEAVES") == "1" else "leaves_") + (hashlib.md5("_".join(sorted(ONLY)).encode()).hexdigest()[:10] if ONLY else "all")
json.dump(results, open(f"{OUT}/tiles_{_tag}.json", "w"))   # hashed: a joined key list overflows the 255-char filename limit

# ------------------------------------------------------------------ tileset.json with LOD
# root -> 1 km parents (1 m roofs, REPLACE) -> 250 m leaves (20 cm roofs). In 3D Tiles a child's transform is
# RELATIVE to its parent's, so leaves carry inv(parent) @ leaf.
allr = {}
for f in glob.glob(f"{OUT}/tiles_*.json"):
    for r in json.load(open(f)):
        allr[r["key"]] = r
M = lambda t: np.array(t, dtype=float).reshape(4, 4).T          # column-major list -> matrix
L = lambda m: m.T.reshape(-1).tolist()
leaves = [r for r in allr.values() if not r["key"].startswith("L1_")]
pars = {r["key"][3:]: r for r in allr.values() if r["key"].startswith("L1_")}
def leaf_node(r, parent=None):
    t = M(r["transform"]) if parent is None else np.linalg.inv(M(parent["transform"])) @ M(r["transform"])
    return {"transform": L(t), "boundingVolume": {"box": r["box"]}, "geometricError": 0, "content": {"uri": r["uri"]}}
children = []
for pk, pr in sorted(pars.items()):
    pe, pn = map(float, pk.split("_"))
    mine = [r for r in leaves if pe <= float(r["key"].split("_")[0]) < pe + PARENT and pn <= float(r["key"].split("_")[1]) < pn + PARENT]
    node = {"transform": pr["transform"], "boundingVolume": {"box": pr["box"]}, "geometricError": PARENT_GEOM_ERR,
            "refine": "REPLACE", "content": {"uri": pr["uri"]}, "children": [leaf_node(r, pr) for r in sorted(mine, key=lambda r: r["key"])]}
    children.append(node)
orphans = [r for r in leaves if not any(float(r["key"].split("_")[0]) // PARENT * PARENT == float(k.split("_")[0])
                                         and float(r["key"].split("_")[1]) // PARENT * PARENT == float(k.split("_")[1]) for k in pars)]
children += [leaf_node(r) for r in orphans]
Os = np.array([r["transform"][12:15] for r in allr.values()])
center = Os.mean(axis=0); radius = float(np.linalg.norm(Os - center, axis=1).max()) + 1200
tileset = {"asset": {"version": "1.1", "generator": "lamap ge_city3d v2"}, "geometricError": 400,
           "root": {"boundingVolume": {"sphere": [*center.tolist(), radius]}, "geometricError": 200, "refine": "REPLACE", "children": children},
           "extras": {"commune": NO_COMMUNE, "buildings": sum(r["buildings"] for r in leaves), "trees": sum(r["trees"] for r in leaves),
                      "lod": {"parents_1km": len(pars), "leaves_250m": len(leaves), "parent_roof_px_m": PARENT_ROOF_PX, "leaf_roof_px_m": ROOF_PX},
                      "sources": ["SITG bati3d", "BFS RegBL", "swisstopo SWISSIMAGE 10 cm 2023", "SITG LiDAR 2025"]}}
json.dump(tileset, open(f"{OUT}/tileset.json", "w"))
log(f"tileset.json: {len(pars)} parents + {len(leaves)} leaves, {tileset['extras']['buildings']} buildings, "
    f"{tileset['extras']['trees']} trees, {sum(r['bytes'] for r in allr.values())/1e6:.0f} MB")
