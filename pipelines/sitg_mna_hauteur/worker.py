#!/usr/bin/env python3
"""
Stage 2 of sitg_mna_hauteur: one shard of 1 km tiles -> partial accumulators (.npz).

Every statistic that spans tiles is merged from RAW ACCUMULATORS (pixel counts, sums,
maxima, fixed-bin histograms), never from per-tile summaries: a parcel or a building
straddling a tile edge would otherwise get an averaged p95, which is wrong. Same design
as ge_canopy_height_model, proven there on 72,948 parcels.

Hold every GDAL dataset in a variable: gdal.Open(p).GetRasterBand(1).ReadAsArray()
lets the dataset be garbage-collected mid-expression and crashes.
"""
import json, os, subprocess, sys, time
import numpy as np
from osgeo import gdal

gdal.UseExceptions()
for k, v in {"GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR", "GDAL_HTTP_MULTIRANGE": "YES",
             "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES", "GDAL_CACHEMAX": "1024",
             "GDAL_HTTP_MAX_RETRY": "5", "GDAL_HTTP_RETRY_DELAY": "2"}.items():
    gdal.SetConfigOption(k, v)

WORK = os.environ.get("WORK", "work")
SHARD, NSHARDS = int(sys.argv[1]), int(sys.argv[2])
PX = 0.2
VEG_MIN = 3.0
VEG_BINS, VEG_BIN = 600, 0.1        # 0..60 m
B_BINS, B_BIN = 600, 0.25           # 0..150 m
NODATA_ABOVE = 1e30
# SOURCE DEFECT, measured 2026-09-18: MNA_HAUTEUR holds isolated pixels at ~377 m (Eaux-Vives
# lakeshore, 2500999.8/1117884.8 — 16 px) where the terrain model had a hole, so MNS - MNT
# leaked the ABSOLUTE ALTITUDE instead of a height. Nothing in Geneva stands 200 m above
# ground, so such pixels are treated as no-data and counted per tile, never averaged in.
MAX_PLAUSIBLE_M = 200.0

meta = json.load(open(f"{WORK}/tiles.json"))
tiles = [t for i, t in enumerate(meta["tiles"]) if i % NSHARDS == SHARD]
os.makedirs(f"{WORK}/partials", exist_ok=True)
src_ds = gdal.Open("/vsicurl/" + meta["source"])
band = src_ds.GetRasterBand(1)
gt = src_ds.GetGeoTransform()
W, H = src_ds.RasterXSize, src_ds.RasterYSize


def burn(gpkg, attr, bounds, dtype):
    out = f"{WORK}/_{SHARD}_{os.path.basename(gpkg)}.tif"
    cmd = ["gdal_rasterize", "-q", "-ot", dtype, "-init", "0", "-te", *map(str, bounds),
           "-tr", str(PX), str(PX), "-l", "parcels" if "parcels" in gpkg else "bldg"]
    cmd += ["-a", attr] if attr else ["-burn", "1"]
    subprocess.run(cmd + [gpkg, out], check=True, capture_output=True)
    ds = gdal.Open(out)
    a = ds.GetRasterBand(1).ReadAsArray()
    del ds
    os.remove(out)
    return a


def read_window(e, n):
    """1 km window, padded with NaN where it falls outside the raster."""
    size = int(round(1000 / PX))
    c0 = int(round((e * 1000 - gt[0]) / gt[1])); r0 = int(round((gt[3] - (n + 1) * 1000) / -gt[5]))
    arr = np.full((size, size), np.nan, dtype="float32")
    cc0, rr0 = max(c0, 0), max(r0, 0)
    cc1, rr1 = min(c0 + size, W), min(r0 + size, H)
    if cc1 > cc0 and rr1 > rr0:
        a = band.ReadAsArray(cc0, rr0, cc1 - cc0, rr1 - rr0).astype("float32")
        a[a > NODATA_ABOVE] = np.nan
        arr[rr0 - r0: rr1 - r0, cc0 - c0: cc1 - c0] = a
    return arr


def grouped(ids, vals, nbins, binw, k):
    """Per-id pixel count, sum, max (-1 = none), histogram, for compact ids 0..k-1."""
    cnt = np.bincount(ids, minlength=k)
    s = np.bincount(ids, weights=vals, minlength=k)
    mx = np.full(k, -1.0); np.maximum.at(mx, ids, vals)
    b = np.clip((vals / binw).astype("int64"), 0, nbins - 1)
    hist = np.bincount(ids * nbins + b, minlength=k * nbins).reshape(k, nbins).astype("int32")
    return cnt, s, mx, hist


def compact(raw):
    u, inv = np.unique(raw, return_inverse=True)
    return u, inv.astype("int64")


t0 = time.time()
for i, key in enumerate(tiles, 1):
    part = f"{WORK}/partials/{key}.npz"
    if os.path.exists(part):
        continue
    e, n = map(int, key.split("-"))
    bounds = (e * 1000, n * 1000, (e + 1) * 1000, (n + 1) * 1000)
    h = read_window(e, n)
    bad = np.isfinite(h) & (h > MAX_PLAUSIBLE_M)
    h[bad] = np.nan
    valid = np.isfinite(h)
    h = np.where(valid, np.maximum(h, 0.0), 0.0)
    pid = burn(f"{WORK}/parcels.gpkg", "pid", bounds, "Int32")
    buf = burn(f"{WORK}/bldg_buf.gpkg", None, bounds, "Byte").astype(bool)
    fp = burn(f"{WORK}/bldg_fp.gpkg", "fid_b", bounds, "Int32")

    res = {"bad_px": np.int64(bad.sum())}
    # ---- parcels: denominator, vegetation (buffered building mask), built heights
    inp = (pid > 0) & valid
    if inp.any():
        ug, inv = compact(pid[inp])
        k = ug.size
        res["p_ids"] = ug.astype("int32")
        res["p_px"] = np.bincount(inv, minlength=k)
        veg = (h[inp] >= VEG_MIN) & ~buf[inp]
        vc, vs, vm, vh = grouped(inv[veg], h[inp][veg].astype("float64"), VEG_BINS, VEG_BIN, k)
        res.update(p_veg_px=vc, p_veg_sum=vs, p_veg_max=vm, p_veg_hist=vh)
        built = fp[inp] > 0
        bc = np.bincount(inv[built], minlength=k)
        bm = np.full(k, -1.0)
        if built.any():
            np.maximum.at(bm, inv[built], h[inp][built].astype("float64"))
        res["p_built_px"], res["p_built_max"] = bc, bm
    # ---- buildings: roof heights inside the -0.4 m footprint
    inb = (fp > 0) & valid
    if inb.any():
        ub, invb = compact(fp[inb])
        c, s, m, hh = grouped(invb, h[inb].astype("float64"), B_BINS, B_BIN, ub.size)
        res.update(b_ids=ub.astype("int32"), b_px=c, b_sum=s, b_max=m, b_hist=hh)
    np.savez_compressed(part, **res)
    el = time.time() - t0
    print(f"[shard {SHARD}] {i}/{len(tiles)} {key} {el/i:.1f}s/tile", flush=True)
print(f"[shard {SHARD}] done {len(tiles)} tiles in {(time.time()-t0)/60:.1f} min")
