#!/usr/bin/env python3
"""
One-shot VD RDPPF theme acquisition — FETCH ONLY, no DB, no secrets.

Runs on a VPS (canton-VD agsgc.map.vd.ch is only reachable from there).
Emits NDJSON to a file per layer:
    {"layer":"vd.zone_reservee","oid":410881,"attributes":{...},"geometry":{...}}

canton-VD ArcGIS pins maxRecordCount=1 (one feature per response, the service
ignores resultRecordCount), so we drive by explicit OBJECTID from
returnIdsOnly — which also gives the authoritative source parity count and
avoids the resultOffset drift you get if the layer mutates mid-run.
"""
import json, sys, time, threading, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer"
LAYERS = {
    35:  "vd.zone_reservee",
    118: "vd.zone_protection_eau",
    119: "vd.secteur_protection_eau",
    120: "vd.aire_alimentation",
    116: "vd.site_pollue",
}
WORKERS = 6           # polite: canton server, keep concurrency modest
MAX_RETRIES = 6

try:
    import requests
    _S = requests.Session()
    _S.headers.update({"Accept-Encoding": "gzip"})
    def get_json(url, timeout=40):
        r = _S.get(url, timeout=timeout); r.raise_for_status(); return r.json()
except ImportError:
    def get_json(url, timeout=40):
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())

def get_json_retry(url):
    last = None
    for a in range(MAX_RETRIES):
        try:
            return get_json(url)
        except Exception as e:
            last = e
            time.sleep(min(30, 2 ** a))
    raise RuntimeError(f"GET failed after {MAX_RETRIES}: {last} url={url}")

def all_ids(layer_id):
    u = f"{BASE}/{layer_id}/query?" + urllib.parse.urlencode(
        {"where": "1=1", "returnIdsOnly": "true", "f": "json"})
    return get_json_retry(u).get("objectIds") or []

def fetch_one(layer_id, oid):
    u = f"{BASE}/{layer_id}/query?" + urllib.parse.urlencode({
        "where": f"OBJECTID={oid}", "outFields": "*", "returnGeometry": "true",
        "outSR": "2056", "f": "json"})
    d = get_json_retry(u)
    fs = d.get("features") or []
    return fs[0] if fs else None

def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/vd_themes.ndjson"
    manifest = {}
    lock = threading.Lock()
    with open(out_path, "w", encoding="utf-8") as out:
        for lid, lname in LAYERS.items():
            ids = all_ids(lid)
            manifest[lname] = {"source_ids": len(ids), "fetched": 0, "empty": 0}
            print(f"[{lname}] source reports {len(ids)} objectIds", flush=True)
            t0 = time.time()
            done = [0]

            def work(oid, lid=lid, lname=lname):
                f = fetch_one(lid, oid)
                rec = {"layer": lname, "oid": oid,
                       "attributes": (f or {}).get("attributes"),
                       "geometry": (f or {}).get("geometry")}
                with lock:
                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    if f is None:
                        manifest[lname]["empty"] += 1
                    else:
                        manifest[lname]["fetched"] += 1
                    done[0] += 1
                    if done[0] % 250 == 0:
                        el = time.time() - t0
                        print(f"[{lname}] {done[0]}/{len(ids)}  {el:.0f}s", flush=True)

            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                list(ex.map(work, ids))
            out.flush()
            print(f"[{lname}] DONE fetched={manifest[lname]['fetched']} "
                  f"empty={manifest[lname]['empty']} in {time.time()-t0:.0f}s", flush=True)

    with open(out_path + ".manifest.json", "w") as m:
        json.dump(manifest, m, indent=2)
    print("MANIFEST " + json.dumps(manifest), flush=True)

if __name__ == "__main__":
    main()
