#!/usr/bin/env python3
"""
Fetch the 3 FEDERAL KbS sub-registers (VBS / BAZL / BAV) over the VD envelope.

⚠️ WHY QUADTREE TILING, not a single call:
   api3 `identify` silently CAPS at ~201 results — limit=500 and limit=2000 both
   return 201 for ch.vbs.* and ch.bav.*. It is a CAP, not a count, and it comes
   back with HTTP 200 and no warning. A single-call fetch would silently
   under-ingest. So: split any tile that comes back at/over the cap into 4 and
   recurse until every leaf is under it.

⚠️ These layers are POINT geometry on GeoAdmin (verified: geometryFormat=geojson
   returns Point for every feature, with distinct real coordinates + katasternummer).
   That is the register's own representation — site locations, not parcel polygons.

geo.admin.ch is federal and UNGATED: reachable from a laptop, no VPS, no Freigabe
(unlike the cantonal agsgc.map.vd.ch).

Emits NDJSON: {"registre":..., "id":..., "attributes":{...}, "geometry":{...}}
"""
import json, sys, urllib.parse, urllib.request, time

BASE = "https://api3.geo.admin.ch/rest/services/api/MapServer/identify"
CAP = 200               # observed server-side cap (returns 201 when saturated)
VD_BBOX = (2494000, 1114000, 2585000, 1200000)   # VD envelope, LV95
MIN_SPAN = 250          # metres — stop splitting below this

LAYERS = {
    "militaire":           "ch.vbs.kataster-belasteter-standorte-militaer",
    "aeroports":           "ch.bazl.kataster-belasteter-standorte-zivilflugplaetze",
    "transports_publics":  "ch.bav.kataster-belasteter-standorte-oev",
}


def get_json(url, timeout=90, retries=6):
    last = None
    for a in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception as e:
            last = e
            time.sleep(min(30, 2 ** a))
    raise RuntimeError(f"GET failed: {last}")


def query(layer, bbox):
    b = ",".join(str(int(v)) for v in bbox)
    p = {"geometry": b, "geometryType": "esriGeometryEnvelope", "layers": "all:" + layer,
         "mapExtent": b, "imageDisplay": "100,100,96", "tolerance": "0", "sr": "2056",
         "returnGeometry": "true", "geometryFormat": "geojson", "limit": "200"}
    return get_json(f"{BASE}?" + urllib.parse.urlencode(p)).get("results") or []


def harvest(layer, bbox, out, seen, depth=0):
    res = query(layer, bbox)
    xmin, ymin, xmax, ymax = bbox
    span = min(xmax - xmin, ymax - ymin)
    # At/over the cap AND still splittable ⇒ the tile is saturated: recurse.
    if len(res) >= CAP and span > MIN_SPAN:
        mx, my = (xmin + xmax) / 2, (ymin + ymax) / 2
        for sub in ((xmin, ymin, mx, my), (mx, ymin, xmax, my),
                    (xmin, my, mx, ymax), (mx, my, xmax, ymax)):
            harvest(layer, sub, out, seen, depth + 1)
        return
    if len(res) >= CAP:
        print(f"    ⚠️ tile still saturated at min span {bbox} — possible truncation", flush=True)
    for f in res:
        fid = f.get("featureId") or f.get("id")
        key = (layer, str(fid))
        if key in seen:
            continue
        seen.add(key)
        out.append(f)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/federal_kbs.ndjson"
    manifest = {}
    with open(path, "w", encoding="utf-8") as fh:
        for registre, layer in LAYERS.items():
            out, seen = [], set()
            t0 = time.time()
            harvest(layer, VD_BBOX, out, seen)
            gtypes = {}
            for f in out:
                g = f.get("geometry") or {}
                gtypes[g.get("type")] = gtypes.get(g.get("type"), 0) + 1
                fh.write(json.dumps({
                    "registre": registre,
                    "id": f.get("featureId") or f.get("id"),
                    "attributes": f.get("properties") or f.get("attributes") or {},
                    "geometry": g,
                }, ensure_ascii=False) + "\n")
            manifest[registre] = {"features": len(out), "geom_types": gtypes,
                                  "secs": round(time.time() - t0)}
            print(f"  {registre:20s} {len(out):5d} features  {gtypes}  ({manifest[registre]['secs']}s)",
                  flush=True)
    print("MANIFEST " + json.dumps(manifest, ensure_ascii=False))


if __name__ == "__main__":
    main()
