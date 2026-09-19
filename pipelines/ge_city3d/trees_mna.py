"""
Tree detection from the SITG 20 cm height model (MNA_HAUTEUR_2025_03, MNS - MNT) instead of LiDAR class 5.

Why: LiDAR class 5 needs the raw point cloud (canton = ~2.1 TB). The height model comes from the SAME 2025 flight,
is one 13 GB COG read by range requests, and already carries every tree's height above ground. Its weakness: it
contains EVERYTHING above ground, not only vegetation — so buildings are masked (footprints +1.5 m, the roof
overhang) and only peaks >= 4 m that stand >= 3.5 m apart are kept.

detect(E0, N0, size) -> dict of arrays: x, y (LV95), h (m above ground), r (crown radius m), z (LN02 ground)
"""
import json, os, subprocess
import numpy as np
from osgeo import gdal
from scipy import ndimage
from scipy.spatial import cKDTree

gdal.UseExceptions()
gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
MNA = "/vsicurl/https://ge.ch/sitg/geodata/SITG/COG-DATA/MNA_HAUTEUR_2025_03.tif"
PX = 0.2
MIN_H, MIN_SEP = 4.0, 3.5
# Vegetation filter, tuned on 6 Geneve-Cite tiles against LiDAR class-5 trees (876 candidates):
#   none                      -> 59 % of detections are real trees, 80 % of LiDAR trees found
#   ExG >= 0.04, rough <= 2.5 -> 92 % real (519 kept, 42 without a LiDAR twin), 73 % found; height vs LiDAR median |d| 0.16 m, 90 % within 0.40 m
# ExG = (2G - R - B) / (R + G + B) on SWISSIMAGE 2023, mean over ~2.2 m; rough = local std of the height model.
# Real crowns: ExG +0.28 median; poles, awnings, structure edges: ExG +0.00 and rougher (1.62 m vs 0.47 m).
EXG_MIN, ROUGH_MAX = 0.04, 2.5
_ds = None


def _mna(E0, N0, size):
    global _ds
    _ds = _ds or gdal.Open(MNA)
    b = _ds.GetRasterBand(1); gt = _ds.GetGeoTransform()
    n = int(round(size / PX))
    c0 = int(round((E0 - gt[0]) / gt[1])); r0 = int(round((gt[3] - (N0 + size)) / -gt[5]))
    a = np.full((n, n), np.nan, np.float32)
    cc0, rr0 = max(c0, 0), max(r0, 0); cc1, rr1 = min(c0 + n, _ds.RasterXSize), min(r0 + n, _ds.RasterYSize)
    if cc1 > cc0 and rr1 > rr0:
        w = b.ReadAsArray(cc0, rr0, cc1 - cc0, rr1 - rr0).astype(np.float32)
        w[(w > 200) | (w < -5)] = np.nan          # >200 m = the documented altitude-leak defect
        a[rr0 - r0: rr1 - r0, cc0 - c0: cc1 - c0] = w
    return np.flipud(a)                           # row 0 = south, like the rest of the builder


def _dtm(E0, N0, size):
    """swissALTI3D 0.5 m (LN02) resampled to the 0.2 m grid, for each tree's ground altitude."""
    tiles = [f"/vsicurl/https://data.geo.admin.ch/ch.swisstopo.swissalti3d/swissalti3d_{y}_{e}-{n}/swissalti3d_{y}_{e}-{n}_0.5_2056_5728.tif"
             for e in range(int(E0 // 1000), int((E0 + size - 1e-6) // 1000) + 1)
             for n in range(int(N0 // 1000), int((N0 + size - 1e-6) // 1000) + 1) for y in (2021,)]
    try:
        vrt = gdal.BuildVRT("/vsimem/dtm.vrt", tiles)
        d = gdal.Warp("/vsimem/dtm.tif", vrt, outputBounds=[E0, N0, E0 + size, N0 + size], xRes=PX, yRes=PX, resampleAlg="bilinear")
        a = np.flipud(d.GetRasterBand(1).ReadAsArray().astype(np.float32)); del d, vrt
        return a
    except Exception:
        return None


def _ortho(E0, N0, size):
    src = [f"/vsicurl/https://data.geo.admin.ch/ch.swisstopo.swissimage-dop10/swissimage-dop10_2023_{e}-{n}/swissimage-dop10_2023_{e}-{n}_0.1_2056.tif"
           for e in range(int(E0 // 1000), int((E0 + size - 1e-6) // 1000) + 1) for n in range(int(N0 // 1000), int((N0 + size - 1e-6) // 1000) + 1)]
    vrt = gdal.BuildVRT("/vsimem/to.vrt", src)
    d = gdal.Translate("/vsimem/to.tif", vrt, projWin=[E0, N0 + size, E0 + size, N0], xRes=PX, yRes=PX, bandList=[1, 2, 3], resampleAlg="average")
    a = np.flipud(np.dstack([d.GetRasterBand(i).ReadAsArray() for i in (1, 2, 3)]).astype(np.float32)); del d, vrt
    return a


def detect(E0, N0, size, footprints_geojson):
    h = _mna(E0, N0, size)
    if np.isnan(h).all():
        return None
    n = h.shape[0]
    mask = np.zeros_like(h, dtype=bool)
    if footprints_geojson:
        gj, tif = f"/tmp/_fp_{os.getpid()}.geojson", f"/tmp/_fp_{os.getpid()}.tif"    # per process: builds run in parallel
        with open(gj, "w") as f:
            json.dump(footprints_geojson, f)
        subprocess.run(["gdal_rasterize", "-q", "-burn", "1", "-init", "0", "-ot", "Byte", "-te", str(E0), str(N0), str(E0 + size), str(N0 + size),
                        "-tr", str(PX), str(PX), gj, tif], check=True, capture_output=True)
        d = gdal.Open(tif); mask = np.flipud(d.GetRasterBand(1).ReadAsArray().astype(bool)); del d
        os.remove(gj); os.remove(tif)
    chm = np.where(np.isnan(h) | mask, 0.0, np.clip(h, 0, 60))
    chm = ndimage.gaussian_filter(chm, 2.5)               # 0.5 m smoothing at 0.2 m pixels
    win = int(round(MIN_SEP / PX)) | 1
    pk = (chm == ndimage.maximum_filter(chm, size=win)) & (chm >= MIN_H)
    pi, pj = np.nonzero(pk)
    if len(pi) == 0:
        return {"x": np.zeros(0), "y": np.zeros(0), "h": np.zeros(0), "r": np.zeros(0), "z": np.zeros(0)}
    X = E0 + (pj + .5) * PX; Y = N0 + (pi + .5) * PX
    # height = the RAW (unsmoothed) max within 0.6 m of the peak, so the apex is not shaved by smoothing
    raw = np.where(np.isnan(h) | mask, 0.0, h)
    H = ndimage.maximum_filter(raw, size=7)[pi, pj]
    # vegetation filter: green in the aerial photo, not a jagged structure edge
    rgb = _ortho(E0, N0, size)
    exg = ndimage.uniform_filter((2 * rgb[..., 1] - rgb[..., 0] - rgb[..., 2]) / (rgb.sum(axis=2) + 1e-6), 11)
    hh = np.where(np.isnan(h), 0.0, h)
    rough = np.sqrt(np.maximum(ndimage.uniform_filter(hh ** 2, 9) - ndimage.uniform_filter(hh, 9) ** 2, 0))
    keep = (exg[pi, pj] >= EXG_MIN) & (rough[pi, pj] <= ROUGH_MAX)
    X, Y, H, pi, pj = X[keep], Y[keep], H[keep], pi[keep], pj[keep]
    if len(X) == 0:
        return {"x": X, "y": Y, "h": H, "r": np.zeros(0), "z": np.zeros(0)}
    dnn = cKDTree(np.c_[X, Y]).query(np.c_[X, Y], k=2)[0][:, 1] if len(X) > 1 else np.full(len(X), 8.0)
    R = np.clip(np.minimum(0.55 * dnn, 0.22 * H + 1.2), 1.2, 9.0)
    dtm = _dtm(E0, N0, size)
    Z = dtm[np.clip(pi, 0, dtm.shape[0] - 1), np.clip(pj, 0, dtm.shape[1] - 1)] if dtm is not None else np.full(len(X), np.nan)
    return {"x": X, "y": Y, "h": H, "r": R, "z": Z}
