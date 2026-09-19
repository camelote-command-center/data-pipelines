#!/usr/bin/env python3
"""Per-km draw range: wrap every top-level node of a commune tileset in an empty tile with the same box and
geometricError PARENT_RANGE_ERR, so each 1 km parent is drawn only within its OWN range (~2.9 km at MSSE 16 on a
900 px view). Without it, a commune's parents were all drawn as soon as the camera was inside the commune's big
bounding sphere — i.e. over any neighbouring commune (measured: whole canton 43 fps vs 59.5 for one commune).
Idempotent. Usage: wrap_parents.py <commune tileset.json> ..."""
import json, sys
PARENT_RANGE_ERR = 60.0
ROOT_ERR = 100000.0          # the commune root has no content: always refine into the per-km wrappers


def wrap(t):
    kids = []
    for c in t["root"]["children"]:
        if c.get("extras", {}).get("wrapper"):
            kids.append(c); continue
        inner = dict(c); tr = inner.pop("transform", None)
        w = {"boundingVolume": c["boundingVolume"], "geometricError": PARENT_RANGE_ERR, "refine": "REPLACE",
             "children": [inner], "extras": {"wrapper": True}}
        if tr is not None:
            w["transform"] = tr
        kids.append(w)
    t["root"]["children"] = kids
    t["root"]["geometricError"] = ROOT_ERR; t["geometricError"] = ROOT_ERR
    return t


if __name__ == "__main__":
    for f in sys.argv[1:]:
        json.dump(wrap(json.load(open(f))), open(f, "w"))
    print(f"wrapped {len(sys.argv) - 1} tilesets")
