#!/usr/bin/env python3
"""Convert v4 GLBs built with two property tables (building, tree) to the single 'feature' table of glb_writer:
egid, height_m, crown_m, kind. Tree instance feature ids are shifted by the building count, in place.
Usage: convert_single_table.py <glb> ...   (idempotent: already-converted files are skipped)"""
import sys
import numpy as np
import pygltflib as G
from concurrent.futures import ProcessPoolExecutor

DT = {"UINT32": np.uint32, "FLOAT32": np.float32, "UINT8": np.uint8}
COLS = {"egid": "UINT32", "height_m": "FLOAT32", "crown_m": "FLOAT32", "kind": "UINT8"}


def conv(path):
    g = G.GLTF2().load(path); blob = bytearray(g.binary_blob())
    md = (g.extensions or {}).get("EXT_structural_metadata")
    if not md or [t["class"] for t in md["propertyTables"]] == ["feature"]:
        return "skip"
    classes = md["schema"]["classes"]
    def col(t, k):
        bv = g.bufferViews[t["properties"][k]["values"]]
        ct = classes[t["class"]]["properties"][k]["componentType"]
        return np.frombuffer(bytes(blob[bv.byteOffset: bv.byteOffset + bv.byteLength]), dtype=DT[ct]).copy()
    T = {t["class"]: t for t in md["propertyTables"]}
    b, t = T.get("building"), T.get("tree")
    nb, nt = (b["count"] if b else 0), (t["count"] if t else 0)
    rows = {"egid": [], "height_m": [], "crown_m": [], "kind": []}
    if b:
        rows["egid"].append(col(b, "egid")); rows["height_m"].append(col(b, "roof_height_m") if "roof_height_m" in b["properties"] else np.full(nb, -1, np.float32))
        rows["crown_m"].append(np.zeros(nb, np.float32)); rows["kind"].append(np.zeros(nb, np.uint8))
    if t:
        rows["egid"].append(np.zeros(nt, np.uint32)); rows["height_m"].append(col(t, "height_m"))
        rows["crown_m"].append(col(t, "crown_m")); rows["kind"].append(np.ones(nt, np.uint8))
    n = nb + nt
    # shift tree instance feature ids by nb (float32 accessor, rewritten in place)
    for node in g.nodes:
        ext = (node.extensions or {})
        if "EXT_instance_features" in ext:
            a = g.accessors[ext["EXT_mesh_gpu_instancing"]["attributes"]["_FEATURE_ID_0"]]
            bv = g.bufferViews[a.bufferView]; o = bv.byteOffset + (a.byteOffset or 0)
            v = np.frombuffer(bytes(blob[o: o + 4 * a.count]), dtype=np.float32) + nb
            blob[o: o + 4 * a.count] = v.astype(np.float32).tobytes()
            ext["EXT_instance_features"]["featureIds"] = [{"featureCount": n, "attribute": 0, "propertyTable": 0}]
    for m in g.meshes:
        for p in m.primitives:
            if p.extensions and "EXT_mesh_features" in p.extensions:
                p.extensions["EXT_mesh_features"]["featureIds"] = [{"featureCount": n, "attribute": 0, "propertyTable": 0}]
    props = {}
    for k, ct in COLS.items():
        data = np.concatenate(rows[k]).astype(DT[ct]).tobytes()
        while len(blob) % 8: blob.append(0)
        g.bufferViews.append(G.BufferView(buffer=0, byteOffset=len(blob), byteLength=len(data))); blob += data
        props[k] = {"values": len(g.bufferViews) - 1}
    while len(blob) % 4: blob.append(0)
    g.extensions["EXT_structural_metadata"] = {
        "schema": {"id": "lamap_city", "classes": {"feature": {"properties": {k: {"type": "SCALAR", "componentType": ct} for k, ct in COLS.items()}}}},
        "propertyTables": [{"class": "feature", "count": n, "properties": props}]}
    g.buffers[0].byteLength = len(blob); g.set_binary_blob(bytes(blob)); g.save_binary(path)
    return f"{nb}+{nt}"


if __name__ == "__main__":
    with ProcessPoolExecutor(8) as ex:
        res = list(ex.map(conv, sys.argv[1:], chunksize=16))
    print({"converted": sum(r != "skip" for r in res), "skipped": res.count("skip")})
