#!/usr/bin/env python3
"""
Sample the CHM at LV95 points — acceptance criterion 5 (ground truth).

Usage:
    python3 sample_chm.py 2500123.4 1117456.7 [E N ...]
    python3 sample_chm.py --file points.tsv     # E<TAB>N<TAB>expected_m

Reports, per point, the CHM at the exact pixel plus the max within a 1 m
and 2 m radius. The radius matters: a tree crown apex measured by eye in a
3D viewer is rarely the exact 0.5 m pixel you name, so the near-max is the
fairer comparison for a "tallest tree" reading.

Reads the COG straight out of R2 via rclone cat (the bucket is private, so
/vsicurl is not an option).
"""
import os, subprocess, sys
import numpy as np
from osgeo import gdal

gdal.UseExceptions()
R2 = "r2-camelote-backups:camelote-backups/lamap-2025/chm/v1"
CACHE = "work/_sample_cache"
os.makedirs(CACHE, exist_ok=True)


def tile_for(e, n):
    return f"{int(e // 1000)}-{int(n // 1000)}"


def fetch(key):
    p = os.path.join(CACHE, f"{key}.tif")
    if not os.path.exists(p):
        r = subprocess.run(["rclone", "copyto", f"{R2}/{key}.tif", p],
                           capture_output=True, text=True)
        if r.returncode != 0:
            return None
    return p


def sample(e, n):
    key = tile_for(e, n)
    p = fetch(key)
    if not p:
        return key, None, None, None
    ds = gdal.Open(p)
    gt = ds.GetGeoTransform()
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray()
    nd = band.GetNoDataValue()
    del band, ds
    col = int((e - gt[0]) / gt[1])
    row = int((n - gt[3]) / gt[5])
    if not (0 <= row < arr.shape[0] and 0 <= col < arr.shape[1]):
        return key, None, None, None
    v = arr[row, col]
    exact = None if v == nd else v / 10.0

    def near(radius_px):
        r0, r1 = max(0, row - radius_px), min(arr.shape[0], row + radius_px + 1)
        c0, c1 = max(0, col - radius_px), min(arr.shape[1], col + radius_px + 1)
        w = arr[r0:r1, c0:c1].astype("float32")
        w = w[w != nd]
        return None if w.size == 0 else float(w.max()) / 10.0

    return key, exact, near(2), near(4)   # 2 px = 1 m, 4 px = 2 m


def main():
    pts = []
    if len(sys.argv) > 2 and sys.argv[1] == "--file":
        for line in open(sys.argv[2]):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            f = line.split()
            pts.append((float(f[0]), float(f[1]),
                        float(f[2]) if len(f) > 2 else None))
    else:
        a = [float(x) for x in sys.argv[1:]]
        pts = [(a[i], a[i + 1], None) for i in range(0, len(a) - 1, 2)]
    if not pts:
        sys.exit(__doc__)

    print(f"{'E':>10} {'N':>10} {'tile':>10} {'exact':>7} {'max r=1m':>9} "
          f"{'max r=2m':>9} {'expect':>7} {'delta(r=1m)':>12}")
    deltas = []
    for e, n, exp in pts:
        key, ex, m1, m2 = sample(e, n)
        d = "" if (exp is None or m1 is None) else f"{m1 - exp:+.2f}"
        if exp is not None and m1 is not None:
            deltas.append(m1 - exp)
        f = lambda v: "  n/a" if v is None else f"{v:7.2f}"
        print(f"{e:10.1f} {n:10.1f} {key:>10} {f(ex)} {f(m1)} {f(m2)} "
              f"{'    n/a' if exp is None else f'{exp:7.2f}'} {d:>12}")
    if deltas:
        a = np.array(deltas)
        print(f"\nn={a.size}  mean {a.mean():+.2f} m  median {np.median(a):+.2f} m  "
              f"MAE {np.abs(a).mean():.2f} m  range [{a.min():+.2f}, {a.max():+.2f}]")
        print("Agreement within about 1-2 m validates the chain. A consistent")
        print("one-signed offset points at DSM/DTM misalignment or a vintage gap")
        print("between what was measured on screen and what the CHM was built from.")


if __name__ == "__main__":
    main()
