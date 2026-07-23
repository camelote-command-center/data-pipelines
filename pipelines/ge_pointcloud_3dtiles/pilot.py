#!/usr/bin/env python3
"""
Point-cloud pilot — 10 tiles, Cologny + Collonge-Bellerive (Vesenaz).

Per tile:
  COPC -> filters.hag_dem (swissALTI3D)          height above ground, per point
       -> filters.colorization (SWISSIMAGE 2023) true colour
       -> filters.range                           drop class 7 noise
       -> writers.las PDRF 7 + HeightAboveGround extra dim

hag_dem is used rather than hag_nn/hag_delaunay: measured 17.6 s vs 47.2 s vs
101.6 s on tile 2502-1117, agreeing to ~0.15 m, and it derives ground from
swisstopo's own DTM rather than from the cloud's class-2 points, so it does
not degrade where ground is occluded. NOTE it floors at 0 (hag_nn permits
negatives) — the absence of negative HAG is expected, not a bug.

Class 6 (building) is NOT dropped here — it is kept in the LAZ and filtered
in Cesium via ${classification}, so the toggle costs nothing to expose.
Class 26 (undocumented, 4.6% of points in the 2025 re-flight) is likewise
kept and left for the frontend to hide by default.

Ortho is 2023 while the cloud is 2025: a tree felled in 2024 is coloured from
imagery that still shows it. Accepted.
"""
import json, os, subprocess, sys, time

TILES = json.load(open("pilot_tiles.json"))
DTM = json.load(open("dtm_urls.json"))
WORK = "pilot_work"
os.makedirs(WORK, exist_ok=True)

ORTHO = ("/vsicurl/https://data.geo.admin.ch/ch.swisstopo.swissimage-dop10/"
         "swissimage-dop10_2023_{key}/swissimage-dop10_2023_{key}_0.1_2056.tif")


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def fetch(url, dest, expect=None):
    """Resume guard MUST verify the expected byte count, not merely that a file
    exists. A killed run leaves a truncated COPC; PDAL then reads it, emits
    almost nothing, and exits 0 — a silent corruption that looks like success."""
    if os.path.exists(dest):
        have = os.path.getsize(dest)
        if (expect is not None and have == expect) or (expect is None and have > 10000):
            return
        os.remove(dest)
    r = sh(["curl", "-sS", "--fail", "--retry", "3", "-o", dest, url])
    if r.returncode != 0:
        raise RuntimeError(f"download failed {url}: {r.stderr[:200]}")
    if expect is not None and os.path.getsize(dest) != expect:
        raise RuntimeError(f"size mismatch {dest}: got {os.path.getsize(dest)} want {expect}")


def build(t, decimate):
    key = t["key"]
    tag = "q4" if decimate else "full"
    out = f"{WORK}/{key}_{tag}.laz"
    if os.path.exists(out) and os.path.getsize(out) > 10000:
        return out, 0.0

    copc = f"{WORK}/{key}.copc.laz"
    dtm = f"{WORK}/{key}_dtm.tif"
    fetch(t["href"], copc, t.get("bytes"))
    fetch(DTM[key], dtm)

    stages = [
        {"type": "readers.copc", "filename": copc},
        {"type": "filters.hag_dem", "raster": dtm},
        {"type": "filters.colorization", "raster": ORTHO.format(key=key),
         "dimensions": "Red:1:1.0, Green:2:1.0, Blue:3:1.0"},
        {"type": "filters.range", "limits": "Classification![7:7]"},
    ]
    if decimate:
        stages.append({"type": "filters.decimation", "step": 4})
    stages.append({"type": "writers.las", "filename": out, "compression": "laszip",
                   "extra_dims": "HeightAboveGround=float32",
                   "minor_version": 4, "dataformat_id": 7})

    pj = f"{WORK}/{key}_{tag}.json"
    json.dump(stages, open(pj, "w"))
    t0 = time.time()
    r = sh(["pdal", "pipeline", pj])
    if r.returncode != 0:
        raise RuntimeError(f"pdal failed {key} {tag}: {r.stderr[:400]}")
    if os.path.getsize(out) < 100000:
        raise RuntimeError(f"{key} {tag}: output is {os.path.getsize(out)} bytes — "
                           f"pdal exited 0 but produced no points (corrupt input?)")
    return out, time.time() - t0


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    for decimate, tag in ((False, "full"), (True, "q4")):
        if which != "both" and which != tag:
            continue
        tot = 0.0
        for i, t in enumerate(TILES, 1):
            out, el = build(t, decimate)
            tot += el
            sz = os.path.getsize(out) / 1e6
            print(f"  [{tag}] {i:2d}/{len(TILES)} {t['key']}  {el:5.1f}s  {sz:6.1f} MB", flush=True)
        print(f"  [{tag}] TOTAL pdal {tot/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
