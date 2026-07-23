#!/usr/bin/env python3
"""
Stage 2 of the pilot — per-commune 3D Tiles from the attributed LAZ.

Per-commune rather than canton-wide (Phase 1 recommendation, accepted):
independently buildable, independently fixable, and the map already knows
which commune the user is in.

Spec is 1.0 / pnts, deliberately. 3D Tiles 1.1 was tested and rejected:
py3dtiles 12.1.1 crashes with KeyError on --extra-fields HeightAboveGround
(correct case) and silently drops the field when given heightaboveground
(lower case). Even when it completes, classification arrives as a raw glTF
vertex attribute _CLASSIFICATION with no EXT_structural_metadata and no
schema, which Cesium's declarative styling cannot address via
${classification}. So 1.1 costs the height ramp AND the class filter.

EVERY conversion is asserted on afterwards: --extra-fields is case-sensitive
and fails with only a warning, so a tileset can ship silently missing the
attribute the whole feature depends on.
"""
import glob, json, os, struct, subprocess, sys, time

WORK = "pilot_work"
OUT = "pilot_tilesets"
PY3D = ".venv/bin/py3dtiles"

# dominant commune per pilot tile (from ST_Intersection area against
# bronze_ch.ge_communes_geo)
COMMUNE = {
    "2502-1117": "cologny", "2502-1118": "cologny", "2502-1119": "cologny",
    "2503-1118": "cologny", "2503-1119": "cologny", "2503-1120": "cologny",
    "2503-1121": "collonge-bellerive", "2503-1122": "collonge-bellerive",
    "2504-1122": "collonge-bellerive", "2504-1123": "collonge-bellerive",
}
REQUIRED = {"HeightAboveGround", "classification"}


def batch_table_keys(pnts_path):
    b = open(pnts_path, "rb").read()
    _, _, ftj, ftb, btj, _ = struct.unpack("<IIIIII", b[4:28])
    if not btj:
        return set()
    return set(json.loads(b[28 + ftj + ftb: 28 + ftj + ftb + btj]).keys())


def assert_attributes(tileset_dir):
    """Fail loudly if the batch table is missing anything the styling needs."""
    pn = glob.glob(f"{tileset_dir}/**/*.pnts", recursive=True)
    if not pn:
        raise RuntimeError(f"{tileset_dir}: no pnts produced")
    keys = batch_table_keys(sorted(pn, key=os.path.getsize)[-1])
    missing = REQUIRED - keys
    if missing:
        raise RuntimeError(
            f"{tileset_dir}: BATCH TABLE MISSING {sorted(missing)} (has {sorted(keys)}). "
            f"--extra-fields is case-sensitive against the LAS dimension name and only warns.")
    return keys, len(pn)


def build(commune, tag):
    files = sorted(f"{WORK}/{k}_{tag}.laz" for k, c in COMMUNE.items()
                   if c == commune and os.path.exists(f"{WORK}/{k}_{tag}.laz"))
    if not files:
        return None
    dest = f"{OUT}/{tag}/{commune}"
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    t0 = time.time()
    r = subprocess.run([PY3D, "convert", *files, "--out", dest,
                        "--srs_in", "2056", "--srs_out", "4978",
                        "--extra-fields", "HeightAboveGround",
                        "--extra-fields", "classification",
                        "--jobs", "8", "--overwrite"],
                       capture_output=True, text=True)
    el = time.time() - t0
    if r.returncode != 0:
        raise RuntimeError(f"{commune}/{tag} convert failed: {r.stderr[-500:]}")
    for line in r.stdout.splitlines():
        if "does not have the field" in line:
            raise RuntimeError(f"{commune}/{tag}: py3dtiles dropped a field — {line.strip()}")
    keys, npnts = assert_attributes(dest)
    size = sum(os.path.getsize(f) for f in glob.glob(f"{dest}/**/*", recursive=True)
               if os.path.isfile(f))
    print(f"  [{tag}] {commune:20s} {len(files)} tiles  {el:6.1f}s  "
          f"{size/1e6:7.1f} MB  {npnts:5d} pnts  batch_table={sorted(keys)}", flush=True)
    return {"commune": commune, "density": tag, "tiles": len(files),
            "seconds": round(el, 1), "bytes": size, "pnts": npnts}


def main():
    os.makedirs(OUT, exist_ok=True)
    res = []
    for tag in ("full", "q4"):
        for commune in sorted(set(COMMUNE.values())):
            r = build(commune, tag)
            if r:
                res.append(r)
    json.dump(res, open("tileset_results.json", "w"), indent=1)
    for tag in ("full", "q4"):
        s = [r for r in res if r["density"] == tag]
        if s:
            print(f"  [{tag}] TOTAL {sum(r['bytes'] for r in s)/1e6:.1f} MB, "
                  f"{sum(r['pnts'] for r in s):,} pnts files, "
                  f"{sum(r['seconds'] for r in s)/60:.1f} min")


if __name__ == "__main__":
    sys.exit(main())
