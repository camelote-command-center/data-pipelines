#!/usr/bin/env python3
"""
Merge per-tile partial zonal statistics into one row per parcel.

Parcels straddle tile boundaries, so every quantity must be merged from
raw accumulators, never from per-tile summaries:

  parcel_px / veg_px / veg_sum  -> summed
  veg_max                       -> max
  hist (500 x 0.1 m bins)       -> summed, then p95 read off the
                                   cumulative distribution (exact to the
                                   0.1 m bin width; a per-tile p95
                                   averaged across tiles would be wrong)
  dsm_year / dtm_year           -> modal, weighted by the parcel's pixel
                                   count in each contributing tile
  vintage_mixed                 -> true if contributing tiles disagree

Emits a TSV for COPY into gold_ch.plot_canopy_stats.
"""
import glob, json, os, sys
import numpy as np

NBINS, BIN_M, PX_AREA = 500, 0.1, 0.25
N_PARCELS = 72949            # pid is 1..72949 (row_number over egrid)

part = sorted(glob.glob("partials/*.npz"))
print(f"merging {len(part)} tile partials")

size = N_PARCELS + 1
parcel_px = np.zeros(size, dtype="int64")
veg_px    = np.zeros(size, dtype="int64")
veg_sum   = np.zeros(size, dtype="float64")
veg_max   = np.zeros(size, dtype="float64")
hist      = np.zeros((size, NBINS), dtype="int32")
# vintage votes: {pid: {(dsm,dtm): px}}
votes = {}

for i, p in enumerate(part, 1):
    z = np.load(p)
    g = z["gpids"].astype("int64")
    parcel_px[g] += z["parcel_px"]
    veg_px[g]    += z["veg_px"]
    veg_sum[g]   += z["veg_sum"]
    np.maximum.at(veg_max, g, z["veg_max"])
    hist[g]      += z["hist"]
    dy, ty = int(z["dsm_year"]), int(z["dtm_year"])
    for pid, px in zip(g, z["parcel_px"]):
        if px <= 0:
            continue
        votes.setdefault(int(pid), {})
        k = (dy, ty)
        votes[int(pid)][k] = votes[int(pid)].get(k, 0) + int(px)
    if i % 50 == 0:
        print(f"  {i}/{len(part)}")

covered = np.where(parcel_px > 0)[0]
covered = covered[covered > 0]
print(f"parcels with CHM coverage: {len(covered):,} of {N_PARCELS:,}")

# p95 from the merged histogram, over the >=3 m mask only
cum = np.cumsum(hist[covered], axis=1)
tot = cum[:, -1]
p95 = np.full(len(covered), np.nan)
has = tot > 0
if has.any():
    thresh = 0.95 * tot[has]
    idx = (cum[has] < thresh[:, None]).sum(axis=1)      # first bin reaching 95%
    idx = np.clip(idx, 0, NBINS - 1)
    p95[has] = idx * BIN_M + BIN_M / 2                  # bin centre

# parcel polygon areas + keys, from the same gpkg the pid raster came from
from osgeo import ogr
ogr.UseExceptions()
ds = ogr.Open("work/parcels.gpkg")
lyr = ds.GetLayer("parcels")
meta = {}
for f in lyr:
    meta[f.GetField("pid")] = (f.GetField("egrid"), f.GetField("no_commune"),
                               f.GetField("no_parcelle"), f.GetField("poly_area_m2"))
del lyr, ds
print(f"parcel metadata loaded: {len(meta):,}")

out = "work/canopy_stats.tsv"
n = 0
with open(out, "w") as fh:
    for j, pid in enumerate(covered):
        egrid, noc, nop, poly_area = meta[int(pid)]
        ppx, vpx = int(parcel_px[pid]), int(veg_px[pid])
        area = ppx * PX_AREA
        varea = vpx * PX_AREA
        cover = 100.0 * vpx / ppx if ppx else 0.0
        mean = (veg_sum[pid] / vpx) if vpx else None
        mx = veg_max[pid] if vpx else None
        p = p95[j] if vpx else None
        v = votes.get(int(pid), {})
        (dy, ty) = max(v.items(), key=lambda kv: kv[1])[0] if v else (None, None)
        mixed = len(v) > 1
        fh.write("\t".join([
            egrid, str(noc), str(nop),
            f"{cover:.4f}",
            "\\N" if p is None else f"{p:.2f}",
            "\\N" if mx is None else f"{mx:.2f}",
            "\\N" if mean is None else f"{mean:.2f}",
            f"{varea:.2f}", f"{area:.2f}",
            "\\N" if poly_area is None else f"{poly_area:.2f}",
            str(dy), str(ty), "t" if mixed else "f",
        ]) + "\n")
        n += 1
print(f"wrote {n:,} rows -> {out}")

# sanity gates (acceptance criterion 4)
cov = 100.0 * veg_px[covered] / parcel_px[covered]
print("\n=== sanity ===")
print(f"  canopy_cover_pct > 100 : {(cov > 100).sum()}   (must be 0)")
print(f"  height_max_m     > 50  : {(veg_max[covered] > 50).sum()}   (must be 0)")
print(f"  cover pct  min/median/max: {cov.min():.2f} / {np.median(cov):.2f} / {cov.max():.2f}")
vm = veg_max[covered][veg_px[covered] > 0]
print(f"  height_max_m median/max : {np.median(vm):.2f} / {vm.max():.2f}")
pp = p95[~np.isnan(p95)]
print(f"  height_p95_m  median/max: {np.median(pp):.2f} / {pp.max():.2f}")
print(f"  parcels with zero vegetation: {(veg_px[covered] == 0).sum():,}")
