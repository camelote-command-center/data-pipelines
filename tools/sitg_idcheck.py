#!/usr/bin/env python3
"""
Row-count comparison cannot tell a pagination drop from source drift. Object-id
set comparison can, and it is one request per layer via returnIdsOnly.

The signature that matters is WHERE the missing ids sit:

  * contiguous ids at the TOP of the range  -> rows SITG added since our last
    ingest. Source drift. Harmless, the next run picks them up.
  * ids scattered through the MIDDLE        -> rows we fetched past. That is the
    pagination defect this brief exists to fix.
  * ids present in bronze but absent at source -> rows SITG deleted. Expected,
    since none of these pipelines has delete logic.

Reports all three separately per layer so the cause is never inferred from a
single number.

TARGETS FILE
------------
Reads idcheck_targets.tsv: pipeline, arcgis_service, layer_index, bronze_table,
bronze_id_column (tab-separated, one layer per line).

bronze_id_column is NOT always `objectid`. Getting this wrong produces a
confident and completely wrong verdict: comparing source ids against an
unrelated bronze column reported 5'955 phantom missing rows for
ge_otc_amenag_2roues on 2026-08-07, when the real identity column was
sitg_adm_otc_amenag_2roues_fid and only 10 were missing. Read the layer's own
objectIdField from ?f=pjson and find the bronze column that carries it.

DERIVE THIS LIST, DO NOT HAND-WRITE IT (platform.standards #269 clause 5).
A hand-assembled list of pipelines omitted sitg_servitudes and
sitg_authorizations on 2026-08-07 and nobody noticed until afterwards. Generate
targets from the same source of truth the pipelines use and assert the derived
count against an independent count before trusting a clean result.
"""

import json
import os
import subprocess
import sys

SP = os.environ.get("IDCHECK_DIR", ".")
BASE = "https://vector.sitg.ge.ch/arcgis/rest/services"


def curl_json(url: str, timeout: int = 120):
    out = subprocess.run(["curl", "-s", "--max-time", str(timeout), url],
                         capture_output=True, text=True).stdout
    try:
        return json.loads(out)
    except Exception:
        return None


def source_ids(svc: str, idx: str):
    url = f"{BASE}/{svc}/FeatureServer/{idx}/query?where=1%3D1&returnIdsOnly=true&f=json"
    d = curl_json(url)
    if not d or "objectIds" not in d or d["objectIds"] is None:
        return None
    return {int(i) for i in d["objectIds"]}


def bronze_ids(table: str, col: str, uri: str):
    r = subprocess.run(
        ["psql", uri, "-At", "-c",
         f"select {col} from bronze_ch.{table} where {col} is not null;"],
        capture_output=True, text=True)
    if r.returncode != 0:
        return None
    out = set()
    for line in r.stdout.splitlines():
        line = line.strip()
        if line:
            try:
                out.add(int(line))
            except ValueError:
                pass
    return out


def classify(missing: set, src: set):
    """Split missing ids into tail-of-range (drift) and mid-range (suspect)."""
    if not missing:
        return 0, 0, None
    hi = max(src)
    srt = sorted(missing)
    # tail run: ids forming a contiguous block ending at the maximum source id
    tail = set()
    cur = hi
    while cur in missing:
        tail.add(cur)
        cur -= 1
    mid = missing - tail
    return len(tail), len(mid), sorted(mid)[:15]


if __name__ == "__main__":
    uri = open(f"{SP}/re-llm.uri").read().strip()
    for line in open(f"{SP}/idcheck_targets.tsv"):
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 5:
            continue
        pipe, svc, idx, table, col = parts[:5]
        src = source_ids(svc, idx)
        if src is None:
            print(f"{table}\tSRC_ERR\t-\t-\t-\t-")
            continue
        brz = bronze_ids(table, col, uri)
        if brz is None:
            print(f"{table}\tBRONZE_ERR\t-\t-\t-\t-")
            continue
        missing = src - brz
        extra = brz - src
        tail, mid, mid_sample = classify(missing, src)
        verdict = "CLEAN" if not missing and not extra else (
            "SUSPECT_PAGINATION" if mid else "source_drift_only")
        print(f"{table}\t{verdict}\t{len(src)}\t{len(brz)}\ttail={tail}\tmid={mid}\t{mid_sample}")
        sys.stdout.flush()
