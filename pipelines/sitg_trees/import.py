#!/usr/bin/env python3
"""
SITG tree cadastre (Geneva) — bronze import into re-LLM.

Two layers from the SITG ArcGIS REST FeatureServer:
  1. SIPV_ICA_ARBRE_ISOLE      239'167 pts  inventaire cantonal des arbres isolés
  2. FFP_ARBRES_REMARQUABLES       594 pts  recensement des arbres remarquables

THE THIRD LAYER IS DELIBERATELY ABSENT
    SIPV_ICA_ABATTAGE_SEV_PTS (felling authorisations) is licensed
    "Accès libre, usage privé / A* non commercial" AND "Uniquement pour
    l'affichage dans la carte interactive dédiée". Either clause alone excludes
    us: Lamap and LBI are commercial products, and the dedicated-map clause
    forbids re-display outside SITG's own viewer. Verified on the SITG catalogue
    2026-08-07. Do not add it without a written licence variation from the
    Ville de Genève.

WHY THIS CARRIES ITS OWN FETCH LOOP
    Same reason as pipelines/sitg_forest: shared/sitg_arcgis.py lower-cases and
    snake_cases field names and stringifies every value, which would turn the
    integer measurements into text and lose the NULL/0 distinction that the
    felling-authorisation flag depends on. The shared helper's pagination is now
    stable (orderByFields was added 2026-08-07, commit d68d562), so that is no
    longer a reason to avoid it -- the type coercion is.

WHY f=geojson
    f=json returns ArcGIS geometry; for points that is harmless, but geojson
    keeps this parser identical in shape to sitg_forest and lets the ingest RPC
    use ST_GeomFromGeoJSON unchanged. outSR=2056 means coordinates arrive in
    LV95 and are stamped, never transformed.

SAFETY
    - Source count asserted against the paginated total before anything is
      written. A mismatch aborts the layer rather than landing partial data.
    - A row-count move over 20 percent against the previous run aborts the
      layer BEFORE the upsert. On a quarterly cadence a silent regression would
      otherwise sit undetected for three months. Override with --force-delta.
    - Conflict key is a deterministic CONTENT HASH (position at 1 cm + species +
      size), computed server-side. Not id_arbre: 0 on 262 of 594 remarquables
      rows and genuinely duplicated on isole. Not globalid or objectid either --
      SITG regenerates both on wholesale republish (bug f69a9dcb, measured ZERO
      globalid overlap across two publications of the remarquables layer), and
      the SITG metadata says OBJECTID is not a permanent identifier.
    - Rows absent from a run are found by last_run_id, not by shipping every key
      seen back to the server.
    - UPSERT only via SECURITY DEFINER RPCs. Never TRUNCATE, never DELETE. Rows
      absent from a run are soft-deleted and un-delete if they reappear.

Environment:
    RE_LLM_SUPABASE_URL              re-LLM REST URL
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY service_role key
    CAMELOTE_SUPABASE_URL            pixxels_data REST URL (dataset freshness)
    CAMELOTE_SUPABASE_KEY            pixxels_data service key
"""

import argparse
import json
import os
import sys
import time
import uuid

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.freshness import get_dataset_meta, update_dataset_meta  # noqa: E402

BASE = "https://vector.sitg.ge.ch/arcgis/rest/services"

PAGE_SIZE = 2000
MAX_RETRIES = 5
RETRY_BACKOFF = 2
DELTA_ABORT_PCT = 20.0

LAYERS = [
    {
        "name": "Inventaire cantonal des arbres isolés",
        "service": "SIPV_ICA_ARBRE_ISOLE",
        "code": "ge_sipv_arbre_isole",
        "table": "ge_sipv_arbre_isole",
        "rpc": "upsert_ge_sipv_arbre_isole_batch",
        "batch_size": 500,
    },
    {
        "name": "Recensement des arbres remarquables",
        "service": "FFP_ARBRES_REMARQUABLES",
        "code": "ge_ffp_arbres_remarquables",
        "table": "ge_ffp_arbres_remarquables",
        "rpc": "upsert_ge_ffp_arbres_remarquables_batch",
        "batch_size": 500,
    },
]


# ──────────────────────────────────────────────────────────────
# HTTP
# ──────────────────────────────────────────────────────────────

def _get_json(url: str, params: dict) -> dict:
    last = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=180)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "error" in data:
                err = data["error"]
                raise RuntimeError(f"ArcGIS error {err.get('code')}: {err.get('message')}")
            return data
        except Exception as e:  # noqa: BLE001
            last = e
            wait = RETRY_BACKOFF ** (attempt + 1)
            print(f"    request error ({e}); retry {attempt + 1}/{MAX_RETRIES} in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"giving up after {MAX_RETRIES} attempts: {last}")


def layer_url(service: str) -> str:
    """
    The Hosted FeatureServer path, not the MapServer path.

    Both resolve, but only the FeatureServer declares objectIdField. The
    MapServer endpoint returns objectIdField = null, which means there is no
    stable sort key and resultOffset paging is not deterministic. Using it would
    reintroduce exactly the defect fixed in bug 618396a4.
    """
    return f"{BASE}/Hosted/{service}/FeatureServer/0"


def object_id_field(service: str) -> str:
    meta = _get_json(f"{layer_url(service)}", {"f": "pjson"})
    oid = meta.get("objectIdField")
    if not oid:
        raise RuntimeError(
            f"{service}: layer declares no objectIdField, so pagination has no "
            f"stable sort key. Refusing to page without one."
        )
    return oid


def source_count(service: str) -> int:
    d = _get_json(f"{layer_url(service)}/query",
                  {"where": "1=1", "returnCountOnly": "true", "f": "json"})
    n = d.get("count")
    if n is None:
        raise RuntimeError(f"{service}: returnCountOnly gave no count")
    return int(n)


def fetch_features(service: str) -> list[dict]:
    oid = object_id_field(service)
    total = source_count(service)
    print(f"    ordering by {oid}; {total:,} declared")

    out: list[dict] = []
    offset = 0
    while offset < total:
        d = _get_json(f"{layer_url(service)}/query", {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": 2056,
            "orderByFields": oid,
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "f": "geojson",
        })
        feats = d.get("features") or []
        if not feats:
            break
        out.extend(feats)
        offset += PAGE_SIZE
        if (offset // PAGE_SIZE) % 20 == 0 or offset >= total:
            print(f"    fetched {len(out):,}/{total:,}")
    return out


def call_rpc(url: str, key: str, fn: str, payload: dict):
    r = requests.post(
        f"{url.rstrip('/')}/rest/v1/rpc/{fn}",
        headers={"apikey": key, "Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"},
        data=json.dumps(payload), timeout=300,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"rpc {fn} -> HTTP {r.status_code}: {r.text[:400]}")
    return r.json()


def bronze_count(url: str, key: str, table: str) -> int | None:
    try:
        r = requests.head(
            f"{url.rstrip('/')}/rest/v1/{table}?select=count&deleted_at=is.null",
            headers={"apikey": key, "Authorization": f"Bearer {key}",
                     "Accept-Profile": "bronze_ch", "Prefer": "count=exact"},
            timeout=60,
        )
        cr = r.headers.get("content-range", "")
        if "/" in cr:
            return int(cr.split("/")[1])
    except Exception as e:  # noqa: BLE001
        print(f"    warning: could not read bronze count: {e}")
    return None


# ──────────────────────────────────────────────────────────────
# One layer
# ──────────────────────────────────────────────────────────────

def process_layer(cfg, run_id, re_url, re_key, cc_url, cc_key, force_delta) -> dict:
    service = cfg["service"]
    print(f"\n{'━' * 68}\n  {cfg['name']}  ({service})\n{'━' * 68}")

    declared = source_count(service)
    print(f"    source returnCountOnly: {declared:,}")

    # ── Delta gate, BEFORE any write ──────────────────────────
    meta = get_dataset_meta(cc_url, cc_key, cfg["code"]) or {}
    previous = meta.get("record_count")
    if previous:
        move = abs(declared - previous) / previous * 100.0
        print(f"    previous run: {previous:,}  ->  delta {declared - previous:+,} ({move:.1f}%)")
        if move > DELTA_ABORT_PCT and not force_delta:
            raise RuntimeError(
                f"{service}: source count moved {move:.1f}% against the previous run "
                f"({previous:,} -> {declared:,}), over the {DELTA_ABORT_PCT}% gate. "
                f"Nothing was written. Review at source, then re-run with --force-delta."
            )
    else:
        print("    previous run: none recorded (first load)")

    # ── Fetch + count assertion ───────────────────────────────
    feats = fetch_features(service)
    if len(feats) != declared:
        raise RuntimeError(
            f"{service}: paginated total {len(feats):,} != declared {declared:,}. "
            f"Aborting rather than landing a partial layer."
        )
    print(f"    count assertion OK: {len(feats):,}")

    rows = [{"attrs": f.get("properties") or {}, "geom": f.get("geometry")} for f in feats]
    no_geom = sum(1 for r in rows if r["geom"] is None)
    if no_geom:
        print(f"    warning: {no_geom:,} features carry no geometry")

    before = bronze_count(re_url, re_key, cfg["table"])

    # ── Upsert in batches ─────────────────────────────────────
    received = upserted = collapsed = 0
    bs = cfg["batch_size"]
    for i in range(0, len(rows), bs):
        res = call_rpc(re_url, re_key, cfg["rpc"],
                       {"p_rows": rows[i:i + bs], "p_run_id": run_id})
        row = res[0] if isinstance(res, list) and res else (res or {})
        received += row.get("received", 0)
        upserted += row.get("upserted", 0)
        collapsed += row.get("collapsed", 0)
        if (i // bs) % 20 == 0 or i + bs >= len(rows):
            print(f"    upserted {min(i + bs, len(rows)):>7,}/{len(rows):,}")

    # ── Soft-delete anything not stamped by this run ──────────
    dele = call_rpc(re_url, re_key, "mark_ge_trees_absent",
                    {"p_table": cfg["table"], "p_run_id": run_id})
    dele = (dele[0] if isinstance(dele, list) and dele else dele) or {}
    soft_deleted = dele.get("soft_deleted", 0)

    after = bronze_count(re_url, re_key, cfg["table"])

    print(f"\n    received:      {received:,}")
    print(f"    upserted:      {upserted:,}")
    print(f"    collapsed:     {collapsed:,}   (collapsed onto an existing content key)")
    print(f"    soft-deleted:  {soft_deleted:,}")
    print(f"    rows before:   {before if before is not None else 'unknown'}")
    print(f"    rows after:    {after if after is not None else 'unknown'}")

    if collapsed:
        print(f"    NOTE: {collapsed:,} rows collapsed onto an existing content key "
              f"inside one batch. Expected and correct for the known duplicate "
              f"records (10 pairs on isole, 5 on remarquables at 2026-08-08): the "
              f"source publishes the same tree twice. A CHANGE in this number means "
              f"a new duplicate or a real key collision -- investigate.")

    if after is not None:
        update_dataset_meta(cc_url, cc_key, cfg["code"], record_count=after, status="active")

    return {"service": service, "declared": declared, "fetched": len(feats),
            "upserted": upserted, "collapsed": collapsed,
            "soft_deleted": soft_deleted, "before": before, "after": after}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", action="append", help="Service name (repeatable). Default: all.")
    ap.add_argument("--force-delta", action="store_true", help="Bypass the 20%% delta gate.")
    args = ap.parse_args()

    re_url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    re_key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    cc_url = os.environ.get("CAMELOTE_SUPABASE_URL", "")
    cc_key = os.environ.get("CAMELOTE_SUPABASE_KEY", "")
    if not re_url or not re_key:
        print("ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required")
        return 1

    run_id = str(uuid.uuid4())
    selected = [c for c in LAYERS if not args.layer or c["service"] in args.layer]

    print("=" * 68)
    print("  SITG tree cadastre -> re-LLM bronze_ch")
    print(f"  layers: {len(selected)}")
    print(f"  run_id: {run_id}")
    print("=" * 68)

    results, failures = [], []
    for cfg in selected:
        try:
            results.append(process_layer(cfg, run_id, re_url, re_key, cc_url, cc_key, args.force_delta))
        except Exception as e:  # noqa: BLE001
            print(f"\n    FAILED: {e}")
            failures.append((cfg["service"], str(e)))

    print("\n" + "=" * 68 + "\n  SUMMARY\n" + "=" * 68)
    for r in results:
        print(f"  {r['service']:<28} src={r['declared']:>8,}  after={str(r['after']):>8}  "
              f"collapsed={r['collapsed']:>3}  soft-deleted={r['soft_deleted']:>3}")
    for svc, err in failures:
        print(f"  {svc:<28} FAILED: {err[:110]}")

    if failures:
        print(f"\n  {len(failures)} layer(s) failed.")
        return 1
    print(f"\n  All {len(results)} layer(s) OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
