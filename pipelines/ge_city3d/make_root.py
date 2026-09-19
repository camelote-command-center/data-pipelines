#!/usr/bin/env python3
"""Canton root tileset: one child per commune tileset (external tileset content), ADD refinement."""
import json, os, sys
import numpy as np
OUT = sys.argv[1]
kids, spheres, stats = [], [], {"buildings": 0, "trees": 0}
for c in sorted(os.listdir(OUT), key=lambda x: (len(x), x)):
    f = f"{OUT}/{c}/tileset.json"
    if not c.isdigit() or not os.path.exists(f):
        continue
    t = json.load(open(f))
    sph = t["root"]["boundingVolume"]["sphere"]
    spheres.append(sph)
    for k in stats:
        stats[k] += t["extras"][k]
    kids.append({"boundingVolume": {"sphere": sph}, "geometricError": t["geometricError"], "content": {"uri": f"{c}/tileset.json"}})
S = np.array(spheres); center = S[:, :3].mean(axis=0)
radius = float((np.linalg.norm(S[:, :3] - center, axis=1) + S[:, 3]).max())
root = {"asset": {"version": "1.1", "generator": "lamap ge_city3d v4"}, "geometricError": 4000,
        "root": {"boundingVolume": {"sphere": [*center.tolist(), radius]}, "geometricError": 2000, "refine": "ADD", "children": kids},
        "extras": {"communes": len(kids), **stats}}
json.dump(root, open(f"{OUT}/tileset.json", "w"))
print(f"canton root: {len(kids)} communes, {stats['buildings']} buildings, {stats['trees']} trees")
