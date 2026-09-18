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
ROOF_PX = 0.15                     # roof texture resolution (m/px); 0.1 native, 0.15 keeps tiles light
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
            if part == "gf":
                key = f"gf_{ri % 4}_{st}"
                TEX.setdefault(key, ground_tex(col, ri)); uv = np.c_[P2[:, 0] / 5.0, (P2[:, 1] - zb) / gf]
            else:
                key = f"up_{st}_{ri % len(S['palettes'])}_{ri % 6}"
                TEX.setdefault(key, upper_tex(st, col, ri % 6)); uv = np.c_[P2[:, 0] / S["bay"], (P2[:, 1] - zs) / fh]
            out.append((key, P3, tri, uv))
    return out


# ------------------------------------------------------------------ one tile
def build_tile(key):
    E0, N0 = map(float, key.split("_"))
    my = [e for e, t in tile_of.items() if t == key]
    faces = []
    for layer, kind in (("toit", "roof"), ("facade", "wall"), ("sp_toit", "roof"), ("sp_facade", "dormer")):
        for r in psql_json(f"""SELECT coalesce(json_agg(json_build_object('e',egid::bigint,'g',ST_AsGeoJSON((d).geom)::json)),'[]')
            FROM (SELECT egid, ST_Dump(geom) d FROM bronze_ch.ge_cad_bati3d_{layer} WHERE egid = ANY(ARRAY{my})) x"""):
            faces.append((kind, r["e"], [np.asarray(c, dtype=float) for c in r["g"]["coordinates"]]))
    if not faces:
        return None
    allxyz = np.vstack([r for _, _, rr in faces for r in rr])
    cx0, cy0 = min(allxyz[:, 0].min(), E0) - 2, min(allxyz[:, 1].min(), N0) - 2
    cx1, cy1 = max(allxyz[:, 0].max(), E0 + TILE) + 2, max(allxyz[:, 1].max(), N0 + TILE) + 2
    H0 = float(allxyz[:, 2].min())

    # roof texture: SWISSIMAGE 10 cm, resampled to ROOF_PX
    src = [f"/vsicurl/https://data.geo.admin.ch/ch.swisstopo.swissimage-dop10/swissimage-dop10_2023_{e}-{n}/"
           f"swissimage-dop10_2023_{e}-{n}_0.1_2056.tif"
           for e in range(int(cx0 // 1000), int(cx1 // 1000) + 1) for n in range(int(cy0 // 1000), int(cy1 // 1000) + 1)]
    vrt = gdal.BuildVRT(f"/vsimem/{key}.vrt", src)
    ds = gdal.Translate(f"/vsimem/{key}.tif", vrt, projWin=[cx0, cy1, cx1, cy0], xRes=ROOF_PX, yRes=ROOF_PX,
                        bandList=[1, 2, 3], resampleAlg="average")
    ortho = jpeg(Image.fromarray(np.dstack([ds.GetRasterBand(i).ReadAsArray() for i in (1, 2, 3)]).astype(np.uint8)), 82)
    del ds, vrt
    gdal.Unlink(f"/vsimem/{key}.vrt"); gdal.Unlink(f"/vsimem/{key}.tif")
    uv_o = lambda x, y: np.c_[(x - cx0) / (cx1 - cx0), (y - cy0) / (cy1 - cy0)]

    TEX, groups = {}, {}
    def add(k, P, tri, uv):
        V, F, UV = groups.setdefault(k, ([], [], []))
        n0 = sum(len(x) for x in V); V.append(P); F.append(tri + n0); UV.append(uv)
    for kind, egid, rings in faces:
        if kind == "wall":
            parts = wall_parts(rings, egid, TEX)
            if parts is not None:
                for k, P3, tri, uv in parts:
                    add(k, P3, tri, uv)
                continue
            kind = "roof"
        P, tri = triangulate(rings)
        if P is None:
            continue
        if kind == "dormer":
            TEX.setdefault("slate", slate_tex()); add("slate", P, tri, np.c_[(P[:, 0] + P[:, 2]) / 2, (P[:, 1] + P[:, 2]) / 2])
        else:
            add("roof", P, tri, uv_o(P[:, 0], P[:, 1]))

    local = lambda p: np.c_[p[:, 0] - E0, p[:, 2] - H0, -(p[:, 1] - N0)]     # glTF Y-up
    scene = trimesh.Scene()
    for k, (V, F, UV) in groups.items():
        v = np.vstack(V); f = np.vstack(F); uv = np.vstack(UV)
        v, uv = v[f.ravel()], uv[f.ravel()]; f = np.arange(len(v)).reshape(-1, 3)      # flat shading
        mat = trimesh.visual.material.PBRMaterial(baseColorTexture=ortho if k == "roof" else TEX[k],
                                                  metallicFactor=0.0, roughnessFactor=0.9, doubleSided=True, name=k)
        scene.add_geometry(trimesh.Trimesh(local(v), f, visual=trimesh.visual.TextureVisuals(uv=uv, material=mat),
                                           process=False), node_name=k)

    # trees: LiDAR class 5 canopy peaks inside THIS tile only (no duplicates across tiles)
    ntrees = 0
    copcs = [f for f in glob.glob(f"{COPC_DIR}/*.copc.laz")
             if (lambda e, n: e < E0 + TILE + 10 and e + 250 > E0 - 10 and n < N0 + TILE + 10 and n + 250 > N0 - 10)
             (*map(float, os.path.basename(f)[:-9].split("_")))]
    if copcs:
        bnds = f"([{E0},{E0 + TILE}],[{N0},{N0 + TILE}])"
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
            pi, pj = np.nonzero((chm == ndimage.maximum_filter(chm, size=7)) & (chm >= 4.0))
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
                rng = np.random.default_rng(int(E0 + N0)); blob = trimesh.creation.icosphere(subdivisions=2)
                trunk0 = trimesh.creation.cylinder(radius=1.0, height=1.0, sections=8)
                greens = np.array([[62, 92, 44], [74, 104, 52], [52, 80, 40], [88, 112, 58], [70, 96, 60]])
                V, F, C = [], [], []
                def push(mesh, color, leafy):
                    n0 = sum(len(v) for v in V); V.append(mesh.vertices); F.append(mesh.faces + n0)
                    c = np.tile(np.r_[color, 255], (len(mesh.vertices), 1)).astype(float)
                    if leafy:
                        c[:, :3] *= rng.uniform(0.82, 1.12, (len(c), 1))
                    C.append(np.clip(c, 0, 255).astype(np.uint8))
                for x, y, z, hh, r in zip(TX, TY, TZ, TH, TR):
                    cb = z + hh * 0.38
                    tr = trunk0.copy(); tr.apply_scale([0.12 + 0.012 * hh, 0.12 + 0.012 * hh, (cb - z) + 1.0])
                    tr.apply_translation([x, y, z + ((cb - z) + 1.0) / 2]); push(tr, np.array([84, 66, 50]), False)
                    g = greens[rng.integers(0, len(greens))]; cz = (cb + z + hh) / 2; hz = (z + hh - cb) / 2
                    for _ in range(5):
                        b = blob.copy(); sr = r * rng.uniform(0.55, 0.8)
                        b.apply_scale([sr, sr, hz * rng.uniform(0.55, 0.8)])
                        b.apply_translation([x + rng.normal(0, r * .3), y + rng.normal(0, r * .3), cz + rng.normal(0, hz * .25)])
                        b.vertices += rng.normal(0, sr * 0.07, b.vertices.shape); push(b, g, True)
                scene.add_geometry(trimesh.Trimesh(local(np.vstack(V)), np.vstack(F), vertex_colors=np.vstack(C), process=False),
                                   node_name="trees")
                ntrees = len(TX)

    path = f"{OUT}/{key}.glb"
    scene.export(path)
    # georeferencing frame for this tile
    O = ecef(E0, N0, H0); ex = ecef(E0 + 1, N0, H0) - O; ey = ecef(E0, N0 + 1, H0) - O; ez = ecef(E0, N0, H0 + 1) - O
    transform = [*ex, 0, *ey, 0, *ez, 0, *O, 1]                      # column-major 4x4
    zmax = float(allxyz[:, 2].max()) - H0 + 40
    cxl, cyl = (cx0 + cx1) / 2 - E0, (cy0 + cy1) / 2 - N0
    hx, hy = (cx1 - cx0) / 2, (cy1 - cy0) / 2
    box = [cxl, cyl, zmax / 2, hx, 0, 0, 0, hy, 0, 0, 0, zmax / 2 + 5]     # local Z-up frame (after glTF Y-up -> Z-up)
    return {"key": key, "uri": f"{key}.glb", "transform": transform, "box": box, "buildings": len(my), "trees": ntrees,
            "bytes": os.path.getsize(path)}


results = []
for i, k in enumerate(tiles, 1):
    r = build_tile(k)
    if r:
        results.append(r)
        log(f"{i}/{len(tiles)} {k}: {r['buildings']} buildings, {r['trees']} trees, {r['bytes']/1e6:.1f} MB")
json.dump(results, open(f"{OUT}/tiles_{'_'.join(sorted(ONLY)) or 'all'}.json", "w"))

# ------------------------------------------------------------------ tileset.json (merges every tiles_*.json present)
allr = {}
for f in glob.glob(f"{OUT}/tiles_*.json"):
    for r in json.load(open(f)):
        allr[r["key"]] = r
children = [{"transform": r["transform"], "boundingVolume": {"box": r["box"]}, "geometricError": 0,
             "content": {"uri": r["uri"]}} for r in sorted(allr.values(), key=lambda r: r["key"])]
# root bounding sphere around all tile origins (ECEF), generous radius
Os = np.array([r["transform"][12:15] for r in allr.values()])
center = Os.mean(axis=0); radius = float(np.linalg.norm(Os - center, axis=1).max()) + 400
tileset = {"asset": {"version": "1.1", "generator": "lamap city_commune.py"},
           "geometricError": 400,
           "root": {"boundingVolume": {"sphere": [*center.tolist(), radius]}, "geometricError": 200, "refine": "ADD",
                    "children": children},
           "extras": {"commune": NO_COMMUNE, "buildings": sum(r["buildings"] for r in allr.values()),
                      "trees": sum(r["trees"] for r in allr.values()),
                      "sources": ["SITG bati3d", "BFS RegBL", "swisstopo SWISSIMAGE 10 cm 2023", "SITG LiDAR 2025"]}}
json.dump(tileset, open(f"{OUT}/tileset.json", "w"))
log(f"tileset.json: {len(children)} tiles, {tileset['extras']['buildings']} buildings, {tileset['extras']['trees']} trees, "
    f"{sum(r['bytes'] for r in allr.values())/1e6:.0f} MB")
