#!/usr/bin/env python3
"""
Stage 3 of sitg_mna_hauteur: merge worker partials -> upsert the two bronze tables ->
PATCH freshness PER DATASET CODE (never by workflow_file).

Upsert, never truncate: rows are keyed on egrid / footprint_id and replaced in place.
A partial (single-commune) run therefore updates only its own rows and leaves the rest.
p95 is read off the MERGED histogram (exact to the bin width), not averaged across tiles.
"""
import glob, io, json, os, subprocess, sys, urllib.request, datetime
import numpy as np
from osgeo import ogr

ogr.UseExceptions()
WORK = os.environ.get("WORK", "work")
PG = os.environ.get("RE_LLM_PG_URI") or sys.exit("RE_LLM_PG_URI not set")
CMD_URL, CMD_KEY = os.environ.get("CAMELOTE_DATA_SUPABASE_URL"), os.environ.get("CAMELOTE_DATA_SUPABASE_SERVICE_KEY")
RUN_URL = os.environ.get("GITHUB_RUN_URL", "")
PX_AREA = 0.04
VEG_BIN, B_BIN = 0.1, 0.25
meta = json.load(open(f"{WORK}/tiles.json"))
# Partial (single-commune) runs only publish that commune's rows: its tiles cover every one
# of its parcels completely, whereas a neighbour's parcel on a border tile is only partly read.
COMMUNE = int(os.environ["COMMUNE"]) if os.environ.get("COMMUNE") else None


def psql(sql, stdin=None):
    r = subprocess.run(["psql", PG, "-v", "ON_ERROR_STOP=1", "-q", "-tA", "-c" if stdin is None else "-f",
                        sql if stdin is None else "-"], input=stdin, capture_output=True, text=True,
                       env={**os.environ, "PGOPTIONS": "-c client_min_messages=error"})
    if r.returncode:
        sys.exit(f"psql failed: {r.stderr[:600]}")
    return r.stdout.strip()


def pctl(hist, q, binw):
    cum = np.cumsum(hist, axis=1); tot = cum[:, -1]
    idx = (cum < (q * tot)[:, None]).sum(axis=1)
    return np.where(tot > 0, np.minimum(idx, hist.shape[1] - 1) * binw + binw / 2, np.nan)


def acc(d, key, n, dtype, fill=0):
    if key not in d:
        d[key] = np.full(n, fill, dtype)
    return d[key]


parts = sorted(glob.glob(f"{WORK}/partials/*.npz"))
missing = set(meta["tiles"]) - {os.path.basename(p)[:-4] for p in parts}
if missing:
    sys.exit(f"{len(missing)} tiles have no partial (e.g. {sorted(missing)[:5]}) — refusing to publish a partial canton")

# Hold OGR datasets in variables: ogr.Open(p).GetLayer(0) frees the dataset mid-expression.
p_ds = ogr.Open(f"{WORK}/parcels.gpkg"); pl = p_ds.GetLayer(0)
b_ds = ogr.Open(f"{WORK}/bldg_fp.gpkg"); fpl = b_ds.GetLayer(0)
NP = int(pl.GetFeatureCount()) + 1
NB = max(f.GetField("fid_b") for f in fpl) + 1
P = {}; B = {}; BAD = 0
for p in parts:
    z = np.load(p)
    BAD += int(z["bad_px"]) if "bad_px" in z else 0
    if "p_ids" in z:
        g = z["p_ids"].astype("int64")
        acc(P, "px", NP, "int64")[g] += z["p_px"]
        acc(P, "vpx", NP, "int64")[g] += z["p_veg_px"]
        acc(P, "vsum", NP, "float64")[g] += z["p_veg_sum"]
        np.maximum.at(acc(P, "vmax", NP, "float64", -1.0), g, z["p_veg_max"])
        acc(P, "bpx", NP, "int64")[g] += z["p_built_px"]
        np.maximum.at(acc(P, "bmax", NP, "float64", -1.0), g, z["p_built_max"])
        if "vh" not in P:
            P["vh"] = np.zeros((NP, z["p_veg_hist"].shape[1]), "int32")
        P["vh"][g] += z["p_veg_hist"]
    if "b_ids" in z:
        g = z["b_ids"].astype("int64")
        acc(B, "px", NB, "int64")[g] += z["b_px"]
        acc(B, "sum", NB, "float64")[g] += z["b_sum"]
        np.maximum.at(acc(B, "max", NB, "float64", -1.0), g, z["b_max"])
        if "h" not in B:
            B["h"] = np.zeros((NB, z["b_hist"].shape[1]), "int32")
        B["h"][g] += z["b_hist"]

run_id = psql(f"INSERT INTO bronze_ch.ge_mna_hauteur_runs (source_url, source_bytes, source_last_modified, tiles, github_run_url) "
              f"VALUES ('{meta['source']}', {meta['bytes']}, '{meta['last_modified']}', {len(meta['tiles'])}, "
              f"NULLIF('{RUN_URL}','')) RETURNING run_id;").splitlines()[0]
lm = meta["last_modified"]
S = lambda v: "\\N" if v is None else str(v)
N = lambda v: "\\N" if (v is None or not np.isfinite(v)) else f"{v:.2f}"

# ---- parcels
pl.ResetReading()
pmeta = {f.GetField("pid"): (f.GetField("egrid"), f.GetField("no_commune"), f.GetField("no_parcelle")) for f in pl}
idx = np.where(P.get("px", np.zeros(1)) > 0)[0]
p95 = pctl(P["vh"][idx], 0.95, VEG_BIN) if idx.size else []
buf = io.StringIO()
for j, i in enumerate(idx):
    egrid, noc, nop = pmeta[int(i)]
    px, vpx = int(P["px"][i]), int(P["vpx"][i])
    cover = min(100.0, 100.0 * vpx / px)
    if COMMUNE and noc != COMMUNE:
        continue
    buf.write("\t".join([egrid, S(noc), S(nop), f"{cover:.3f}",
        N(p95[j] if vpx else None), N(P["vmax"][i] if vpx else None), N(P["vsum"][i] / vpx if vpx else None),
        f"{vpx*PX_AREA:.2f}", f"{int(P['bpx'][i])*PX_AREA:.2f}", N(P["bmax"][i] if P["bpx"][i] else None),
        f"{px*PX_AREA:.2f}", lm, run_id]) + "\n")
n_parcels = buf.getvalue().count("\n")
psql(None, stdin="""
-- ON COMMIT DROP in one transaction: re-LLM's SESSION POOLER reuses server connections, so a plain
-- temp table from an earlier call survives into the next client ("relation s already exists").
BEGIN;
CREATE TEMP TABLE s (LIKE bronze_ch.ge_mna_hauteur_parcel_stats INCLUDING DEFAULTS) ON COMMIT DROP;
\\copy s (egrid,no_commune,no_parcelle,canopy_cover_pct,veg_height_p95_m,veg_height_max_m,veg_height_mean_m,vegetated_area_m2,built_area_m2,built_height_max_m,parcel_area_m2,source_last_modified,run_id) FROM STDIN
""" + buf.getvalue() + """\\.
INSERT INTO bronze_ch.ge_mna_hauteur_parcel_stats SELECT * FROM s
ON CONFLICT (egrid) DO UPDATE SET no_commune=EXCLUDED.no_commune, no_parcelle=EXCLUDED.no_parcelle,
 canopy_cover_pct=EXCLUDED.canopy_cover_pct, veg_height_p95_m=EXCLUDED.veg_height_p95_m, veg_height_max_m=EXCLUDED.veg_height_max_m,
 veg_height_mean_m=EXCLUDED.veg_height_mean_m, vegetated_area_m2=EXCLUDED.vegetated_area_m2, built_area_m2=EXCLUDED.built_area_m2,
 built_height_max_m=EXCLUDED.built_height_max_m, parcel_area_m2=EXCLUDED.parcel_area_m2,
 source_last_modified=EXCLUDED.source_last_modified, run_id=EXCLUDED.run_id, computed_at=now();
COMMIT;
""")

# ---- buildings
fpl.ResetReading()
fmeta = {f.GetField("fid_b"): (f.GetField("egid"), f.GetField("no_commune"), f.GetField("no_parcelle")) for f in fpl}
bidx = np.where(B.get("px", np.zeros(1)) > 0)[0]
b95, b50 = (pctl(B["h"][bidx], 0.95, B_BIN), pctl(B["h"][bidx], 0.50, B_BIN)) if bidx.size else ([], [])
buf = io.StringIO()
for j, i in enumerate(bidx):
    egid, noc, nop = fmeta[int(i)]
    px = int(B["px"][i])
    if COMMUNE and noc != COMMUNE:
        continue
    buf.write("\t".join([str(int(i)), S(egid), S(noc), S(nop), f"{px*PX_AREA:.2f}",
        N(B["max"][i]), N(b95[j]), N(b50[j]), N(B["sum"][i] / px), lm, run_id]) + "\n")
n_bldg = buf.getvalue().count("\n")
psql(None, stdin="""
-- ON COMMIT DROP in one transaction: re-LLM's SESSION POOLER reuses server connections, so a plain
-- temp table from an earlier call survives into the next client ("relation s already exists").
BEGIN;
CREATE TEMP TABLE s (LIKE bronze_ch.ge_mna_hauteur_building_heights INCLUDING DEFAULTS) ON COMMIT DROP;
\\copy s (footprint_id,egid,no_commune,no_parcelle,roof_area_m2,height_max_m,height_p95_m,height_p50_m,height_mean_m,source_last_modified,run_id) FROM STDIN
""" + buf.getvalue() + """\\.
INSERT INTO bronze_ch.ge_mna_hauteur_building_heights SELECT * FROM s
ON CONFLICT (footprint_id) DO UPDATE SET egid=EXCLUDED.egid, no_commune=EXCLUDED.no_commune, no_parcelle=EXCLUDED.no_parcelle,
 roof_area_m2=EXCLUDED.roof_area_m2, height_max_m=EXCLUDED.height_max_m, height_p95_m=EXCLUDED.height_p95_m,
 height_p50_m=EXCLUDED.height_p50_m, height_mean_m=EXCLUDED.height_mean_m,
 source_last_modified=EXCLUDED.source_last_modified, run_id=EXCLUDED.run_id, computed_at=now();
COMMIT;
""")
psql(f"UPDATE bronze_ch.ge_mna_hauteur_runs SET finished_at=now(), parcels={n_parcels}, buildings={n_bldg}, "
     f"status='{'ok' if not os.environ.get('COMMUNE') else 'ok_partial'}' WHERE run_id='{run_id}'")
print(f"source-defect pixels (>200 m, treated as no-data): {BAD:,}")
print(f"upserted {n_parcels:,} parcels, {n_bldg:,} buildings (run {run_id})")


def patch(code, count):
    if not (CMD_URL and CMD_KEY) or os.environ.get("COMMUNE"):
        print(f"  [no PATCH for {code}: {'partial run' if os.environ.get('COMMUNE') else 'no credentials'}]")
        return
    total = int(psql(f"SELECT count(*) FROM bronze_ch.{code}"))
    body = json.dumps({"last_acquired_at": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "record_count": total, "last_error": None}).encode()
    req = urllib.request.Request(f"{CMD_URL}/rest/v1/datasets?code=eq.{code}", data=body, method="PATCH",
          headers={"apikey": CMD_KEY, "Authorization": f"Bearer {CMD_KEY}", "Content-Type": "application/json",
                   "Prefer": "return=representation"})
    with urllib.request.urlopen(req, timeout=60) as r:
        rows = json.load(r)
    if not rows:
        sys.exit(f"PATCH {code} matched 0 rows — dataset not registered")   # fail closed on zero-row match
    print(f"  PATCH {code} -> {total:,} rows")


patch("ge_mna_hauteur_parcel_stats", n_parcels)
patch("ge_mna_hauteur_building_heights", n_bldg)
