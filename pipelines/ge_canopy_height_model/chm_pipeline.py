#!/usr/bin/env python3
"""
Canopy Height Model (CHM) for canton Geneve — swisstopo DSM - DTM.

CHM = swissSURFACE3D Raster (DSM, surface incl. vegetation + buildings)
    - swissALTI3D          (DTM, bare earth)

Both are LN02 metres, EPSG:2056, 0.5 m, so the datum cancels and the
difference is a true height above ground. No geoid correction anywhere.

Per tile this script:
  1. downloads the DSM/DTM pair (download beats /vsicurl/ 1.7x — we read
     100% of both rasters, so COG range-reads are pure overhead),
  2. validates shape / transform / CRS and SKIPS mismatched pairs,
  3. masks buildings (bronze_ch.ge_buildings_geo buffered 1.5 m, because
     cadastral footprints are the GROUND outline and roofs overhang them),
  4. MASKS FIRST, THEN CLAMPS — post-mask the urban max is ~39.8 m, so the
     50 m ceiling becomes a genuine anomaly detector instead of clipping
     real towers,
  5. writes a UInt16 x10 COG (0..500 = 0.0..50.0 m, nodata 65535),
  6. uploads it to R2, and
  7. reduces per-parcel zonal statistics while the arrays are still in
     memory, emitting a partial .npz that merge_stats.py combines.

p95 is accumulated as a fixed-bin histogram (500 bins x 0.1 m over 0..50 m)
per parcel per tile. Summing bin counts across tiles and reading the
cumulative distribution is exact to the bin width; a per-tile p95 averaged
across tiles would be wrong for any parcel spanning a tile boundary.

Idempotent: a tile whose COG already exists on R2 is skipped unless --force.

R2 IMMUTABILITY — READ THIS BEFORE RE-RUNNING.
The camelote-backups bucket enforces an object-lock policy: a key that
already exists CANNOT be overwritten (PutObject returns HTTP 409
ObjectLockedByBucketPolicy), and the token has no DELETE either. So
--force alone will fail on every tile that already shipped. To recompute,
bump the version prefix as well:

    python chm_pipeline.py --force --version v2

Versions are therefore immutable snapshots by construction, which is a
feature for a derived artefact — v1 stays exactly as the stats were built
from. Update the consumer docs when you bump.
"""
import argparse, json, os, subprocess, sys, time
import numpy as np
from osgeo import gdal, ogr

gdal.UseExceptions()
ogr.UseExceptions()

PX = 0.5                     # source resolution, metres
PX_AREA = PX * PX            # 0.25 m2 per pixel
VEG_MIN_M = 3.0              # trees vs grass/hedges/vehicles
CLAMP_MAX_M = 50.0
NBINS = 500                  # 0.1 m bins over 0..50 m
BIN_M = 0.1
NODATA_OUT = 65535
R2_BASE = "r2-camelote-backups:camelote-backups/lamap-2025/chm"
R2_VERSION = "v1"          # overridden by --version; bucket is write-once, see module docstring


def r2_prefix():
    return f"{R2_BASE}/{R2_VERSION}"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def r2_exists(key):
    r = subprocess.run(["rclone", "lsf", f"{r2_prefix()}/{key}.tif"],
                       capture_output=True, text=True)
    return r.returncode == 0 and r.stdout.strip() != ""


def download(url, dest):
    r = subprocess.run(["curl", "-sS", "--fail", "--retry", "3",
                        "--retry-delay", "2", "-o", dest, url],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"download failed {url}: {r.stderr.strip()[:200]}")


def rasterize(gpkg, layer, attr, bounds, out, dtype="Int32", where=None):
    """Burn a vector layer onto the tile grid. attr=None -> burn 1."""
    xmin, ymin, xmax, ymax = bounds
    cmd = ["gdal_rasterize", "-q", "-ot", dtype, "-init", "0",
           "-te", str(xmin), str(ymin), str(xmax), str(ymax),
           "-tr", str(PX), str(PX), "-l", layer]
    cmd += ["-a", attr] if attr else ["-burn", "1"]
    if where:
        cmd += ["-where", where]
    cmd += [gpkg, out]
    subprocess.run(cmd, check=True, capture_output=True)


def read_band(path):
    """Hold the dataset in a variable — chaining gdal.Open(..).GetRasterBand(..)
    lets the dataset be garbage-collected mid-expression and segfaults."""
    ds = gdal.Open(path)
    if ds is None:
        raise RuntimeError(f"cannot open {path}")
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray()
    nd = band.GetNoDataValue()
    gt, proj = ds.GetGeoTransform(), ds.GetProjection()
    shape = (ds.RasterXSize, ds.RasterYSize)
    del band, ds
    return arr, nd, gt, proj, shape


def write_cog(path, arr_u16, gt, proj):
    mem = gdal.GetDriverByName("MEM").Create(
        "", arr_u16.shape[1], arr_u16.shape[0], 1, gdal.GDT_UInt16)
    mem.SetGeoTransform(gt)
    mem.SetProjection(proj)
    b = mem.GetRasterBand(1)
    b.WriteArray(arr_u16)
    b.SetNoDataValue(NODATA_OUT)
    del b
    gdal.GetDriverByName("COG").CreateCopy(
        path, mem, options=["COMPRESS=DEFLATE", "LEVEL=9", "PREDICTOR=2",
                            "BLOCKSIZE=512", "OVERVIEWS=IGNORE_EXISTING"])
    del mem


def process_tile(t, work, parcels_gpkg, buildings_gpkg, force, skip_upload):
    key = t["key"]
    res = {"key": key, "dsm_year": t["dsm_year"], "dtm_year": t["dtm_year"]}
    bounds = (t["e"] * 1000, t["n"] * 1000, (t["e"] + 1) * 1000, (t["n"] + 1) * 1000)

    if not force and r2_exists(key):
        res["status"] = "skipped_exists"
        return res

    dtm_p = os.path.join(work, f"{key}_dtm.tif")
    dsm_p = os.path.join(work, f"{key}_dsm.tif")
    chm_p = os.path.join(work, f"{key}_chm.tif")
    pid_p = os.path.join(work, f"{key}_pid.tif")
    bld_p = os.path.join(work, f"{key}_bld.tif")

    try:
        download(t["dtm"], dtm_p)
        download(t["dsm"], dsm_p)

        dtm, nd_a, gt, proj, shape_a = read_band(dtm_p)
        dsm, nd_b, gt2, proj2, shape_b = read_band(dsm_p)

        if shape_a != shape_b or gt != gt2 or proj != proj2:
            res["status"] = "skipped_mismatch"
            res["detail"] = f"shape {shape_a} vs {shape_b}; gt_equal={gt==gt2}; crs_equal={proj==proj2}"
            return res

        dtm = dtm.astype("float32")
        dsm = dsm.astype("float32")
        invalid = (dtm == nd_a) | (dsm == nd_b) | ~np.isfinite(dtm) | ~np.isfinite(dsm)
        chm = dsm - dtm

        # --- (3) mask buildings, buffered 1.5 m ---
        rasterize(buildings_gpkg, "bldg", None, bounds, bld_p, dtype="Byte")
        bmask, _, _, _, _ = read_band(bld_p)
        bmask = bmask.astype(bool)
        chm = np.where(bmask, 0.0, chm)
        res["building_px"] = int(bmask.sum())

        # --- (4) mask first, THEN clamp ---
        over = int(np.sum((chm > CLAMP_MAX_M) & ~invalid))
        res["over_50m_px_after_mask"] = over
        if over > 0:
            res["suspect"] = True
        neg = int(np.sum((chm < 0) & ~invalid))
        res["neg_px"] = neg
        chm = np.clip(chm, 0.0, CLAMP_MAX_M)

        # --- (5) COG ---
        u16 = np.where(invalid, NODATA_OUT, np.rint(chm * 10)).astype("uint16")
        write_cog(chm_p, u16, gt, proj)
        res["cog_bytes"] = os.path.getsize(chm_p)

        # --- (6) upload ---
        if not skip_upload:
            up = subprocess.run(
                ["rclone", "copyto", chm_p, f"{r2_prefix()}/{key}.tif"],
                capture_output=True, text=True)
            if up.returncode != 0:
                err = up.stderr.strip()[:300]
                if "ObjectLocked" in err or "409" in err:
                    raise RuntimeError(
                        f"R2 object is immutable and already exists at {r2_prefix()}/{key}.tif — "
                        f"--force cannot overwrite it. Re-run with a new --version (e.g. v2). Raw: {err}")
                raise RuntimeError(f"rclone upload failed: {err}")

        # --- (7) zonal statistics ---
        rasterize(parcels_gpkg, "parcels", "pid", bounds, pid_p, dtype="Int32")
        pid, _, _, _, _ = read_band(pid_p)
        pid = pid.astype("int32")

        inparcel = (pid > 0) & ~invalid
        gpids = np.unique(pid[inparcel])
        if gpids.size == 0:
            res["status"] = "ok_no_parcels"
            res["parcels"] = 0
            return res

        # compact global pid -> local 0..k-1
        lut = np.zeros(int(gpids.max()) + 1, dtype="int32")
        lut[gpids] = np.arange(gpids.size, dtype="int32")
        k = gpids.size

        loc_all = lut[pid[inparcel]]
        parcel_px = np.bincount(loc_all, minlength=k)          # denominator

        veg = inparcel & (chm >= VEG_MIN_M)
        loc_v = lut[pid[veg]]
        vals = chm[veg].astype("float64")

        veg_px = np.bincount(loc_v, minlength=k)
        veg_sum = np.bincount(loc_v, weights=vals, minlength=k)
        veg_max = np.zeros(k, dtype="float64")
        np.maximum.at(veg_max, loc_v, vals)

        bins = np.clip((vals / BIN_M).astype("int32"), 0, NBINS - 1)
        hist = np.bincount(loc_v * NBINS + bins,
                           minlength=k * NBINS).reshape(k, NBINS).astype("int32")

        np.savez_compressed(
            os.path.join(work, "..", "partials", f"{key}.npz"),
            gpids=gpids.astype("int32"), parcel_px=parcel_px.astype("int64"),
            veg_px=veg_px.astype("int64"), veg_sum=veg_sum,
            veg_max=veg_max, hist=hist,
            dsm_year=np.int32(t["dsm_year"]), dtm_year=np.int32(t["dtm_year"]))

        res["status"] = "ok"
        res["parcels"] = int(k)
        return res

    except Exception as e:
        res["status"] = "error"
        res["detail"] = f"{type(e).__name__}: {e}"
        return res
    finally:
        for p in (dtm_p, dsm_p, chm_p, pid_p, bld_p):
            try:
                os.remove(p)
            except OSError:
                pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiles", default="work/tiles.json")
    ap.add_argument("--parcels", default="work/parcels.gpkg")
    ap.add_argument("--buildings", default="work/buildings.gpkg")
    ap.add_argument("--work", default="work")
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--nshards", type=int, default=1)
    ap.add_argument("--version", default="v1",
                    help="R2 prefix version. The bucket is write-once, so recomputing "
                         "existing tiles requires a NEW version, not --force alone.")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args()

    global R2_VERSION
    R2_VERSION = a.version

    tiles = json.load(open(a.tiles))
    mine = [t for i, t in enumerate(tiles) if i % a.nshards == a.shard]
    log(f"shard {a.shard}/{a.nshards}: {len(mine)} of {len(tiles)} tiles")

    out = []
    t0 = time.time()
    for i, t in enumerate(mine, 1):
        r = process_tile(t, a.work, a.parcels, a.buildings, a.force, a.skip_upload)
        out.append(r)
        if r["status"] not in ("ok", "skipped_exists"):
            log(f"  !! {t['key']}: {r['status']} {r.get('detail','')}")
        if i % 10 == 0 or i == len(mine):
            el = time.time() - t0
            log(f"  {i}/{len(mine)}  {el/i:.1f}s/tile  eta {(len(mine)-i)*el/i/60:.1f} min")
    with open(f"logs/shard{a.shard}.json", "w") as f:
        json.dump(out, f, indent=1)
    ok = sum(1 for r in out if r["status"] == "ok")
    log(f"shard {a.shard} done: ok={ok} other={len(out)-ok} in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    sys.exit(main())
