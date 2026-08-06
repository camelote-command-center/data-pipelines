#!/usr/bin/env python3
"""
Geneva forest layers (SITG) — bronze import into re-LLM.

Five layers from the SITG ArcGIS REST FeatureServer:
  1. FFP_CADASTRE_FORET        polygon   factual surveyed forest extent
  2. RDPPF_DISTANCES_FORET_S   polygon   legally registered 20 m surface
  3. RDPPF_DISTANCES_FORET_L   polyline  same restriction, line geometry
  4. FFP_LISIERES_FORESTIERES  polyline  boundary survey procedures
  5. FFP_FONCTION_PDF          polygon   plan directeur forestier function (optional)

WHY REST AND NOT THE SHP BUNDLES
    RDPPF_DISTANCES_FORET_S publishes no download bundle at all, so REST is the
    only uniform path across all five, and it hands back native LV95 with no
    shapefile reprojection step.

WHY f=geojson AND NOT f=json
    f=json returns ArcGIS rings, and the naive rings-to-GeoJSON conversion used
    elsewhere in this repo treats every ring after the first as a hole. That is
    wrong for multipart polygons and would corrupt the forest area totals that
    Phase 2 depends on. f=geojson makes the server do the conversion correctly.
    It also preserves the original ArcGIS field names (OBJECTID, EREBID, ...),
    which is what the bronze upsert RPCs expect, and returns dates as epoch
    milliseconds.

WHY NOT shared/sitg_arcgis.py
    That helper lower-cases and snake_cases every field name and stringifies
    every value, and it does not send orderByFields, so its pagination is not
    stable. Changing it would alter behaviour for eleven other live pipelines.
    This parser therefore carries its own fetch loop. The missing orderByFields
    in the shared helper is a real latent defect for those pipelines and is
    logged separately rather than fixed silently here.

SAFETY
    - Source count is asserted against the paginated total before anything is
      written. A mismatch aborts the layer rather than landing partial data.
    - A row-count move of more than 20 percent against the previous run aborts
      the layer BEFORE the upsert, so the anomaly is surfaced for review rather
      than landed. On a quarterly cadence a silent regression would otherwise
      sit undetected for three months. Override with --force-delta.
    - UPSERT only, via SECURITY DEFINER RPCs. Never TRUNCATE, never DELETE.
      Rows missing from a run are soft-deleted (deleted_at), and reappear with
      deleted_at reset to NULL.

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

# resultRecordCount stays under the service MaxRecordCount of 4000.
PAGE_SIZE = 2000
MAX_RETRIES = 5
RETRY_BACKOFF = 2

# Abort a layer if its source count moves more than this against the last run.
DELTA_ABORT_PCT = 20.0

LAYERS = [
    {
        "name": "Cadastre forestier",
        "service": "FFP_CADASTRE_FORET",
        "code": "ge_ffp_cadastre_foret",
        "table": "ge_ffp_cadastre_foret",
        "rpc": "upsert_ge_ffp_cadastre_foret_batch",
        "batch_size": 200,
        "optional": False,
    },
    {
        "name": "RDPPF distances foret (surface)",
        "service": "RDPPF_DISTANCES_FORET_S",
        "code": "ge_rdppf_distances_foret_s",
        "table": "ge_rdppf_distances_foret_s",
        "rpc": "upsert_ge_rdppf_distances_foret_s_batch",
        "batch_size": 200,
        "optional": False,
    },
    {
        "name": "RDPPF distances foret (ligne)",
        "service": "RDPPF_DISTANCES_FORET_L",
        "code": "ge_rdppf_distances_foret_l",
        "table": "ge_rdppf_distances_foret_l",
        "rpc": "upsert_ge_rdppf_distances_foret_l_batch",
        "batch_size": 200,
        "optional": False,
    },
    {
        "name": "Lisieres forestieres",
        "service": "FFP_LISIERES_FORESTIERES",
        "code": "ge_ffp_lisieres_forestieres",
        "table": "ge_ffp_lisieres_forestieres",
        "rpc": "upsert_ge_ffp_lisieres_forestieres_batch",
        "batch_size": 200,
        "optional": False,
    },
    {
        "name": "Fonction PDF",
        "service": "FFP_FONCTION_PDF",
        "code": "ge_ffp_fonction_pdf",
        "table": "ge_ffp_fonction_pdf",
        "rpc": "upsert_ge_ffp_fonction_pdf_batch",
        "batch_size": 200,
        "optional": True,
    },
]


# ──────────────────────────────────────────────────────────────
# HTTP
# ──────────────────────────────────────────────────────────────

def _get_json(url: str, params: dict) -> dict:
    """GET with retry and exponential backoff. Raises on final failure."""
    last = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=120)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF ** (attempt + 1)
                print(f"    retry {attempt + 1}/{MAX_RETRIES - 1} in {wait}s: {e}")
                time.sleep(wait)
    raise RuntimeError(f"GET failed after {MAX_RETRIES} attempts: {last}")


def resolve_layer_id(service: str) -> int:
    """
    Read the real layer id from the FeatureServer.

    Never assume 0. Fail loudly if the service exposes more than one layer
    rather than silently taking the first.
    """
    d = _get_json(f"{BASE}/{service}/FeatureServer", {"f": "pjson"})
    if "error" in d:
        raise RuntimeError(f"{service}: {d['error'].get('message')}")
    layers = d.get("layers") or []
    tables = d.get("tables") or []
    if len(layers) != 1:
        raise RuntimeError(
            f"{service}: expected exactly 1 layer, found {len(layers)} "
            f"({[(l.get('id'), l.get('name')) for l in layers]}). "
            f"Resolve by hand rather than guessing."
        )
    if tables:
        print(f"    note: {service} also exposes {len(tables)} table(s); ignoring")
    return int(layers[0]["id"])


def source_count(service: str, layer_id: int) -> int:
    d = _get_json(
        f"{BASE}/{service}/FeatureServer/{layer_id}/query",
        {"where": "1=1", "returnCountOnly": "true", "f": "json"},
    )
    if "count" not in d:
        raise RuntimeError(f"{service}: returnCountOnly gave no count ({d})")
    return int(d["count"])


def fetch_features(service: str, layer_id: int) -> list[dict]:
    """
    Paginate the whole layer.

    orderByFields=OBJECTID is mandatory: without a stable sort the server is
    free to return rows in a different order per page and resultOffset
    pagination silently skips and duplicates rows.
    """
    out: list[dict] = []
    offset = 0
    while True:
        d = _get_json(
            f"{BASE}/{service}/FeatureServer/{layer_id}/query",
            {
                "where": "1=1",
                "outFields": "*",
                "outSR": 2056,
                "f": "geojson",
                "returnGeometry": "true",
                "resultRecordCount": PAGE_SIZE,
                "resultOffset": offset,
                "orderByFields": "OBJECTID",
            },
        )
        feats = d.get("features") or []
        out.extend(feats)
        print(f"    page offset={offset:>6} -> {len(feats):>5} features (total {len(out):,})")
        if len(feats) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return out


# ──────────────────────────────────────────────────────────────
# Upsert
# ──────────────────────────────────────────────────────────────

def call_rpc(url: str, key: str, fn: str, payload: dict) -> list[dict]:
    r = requests.post(
        f"{url.rstrip('/')}/rest/v1/rpc/{fn}",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=300,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"rpc {fn} -> HTTP {r.status_code}: {r.text[:400]}")
    return r.json()


def bronze_count(url: str, key: str, table: str) -> int | None:
    """Live row count from bronze_ch via PostgREST, excluding soft-deleted rows."""
    try:
        r = requests.head(
            f"{url.rstrip('/')}/rest/v1/{table}?select=count&deleted_at=is.null",
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Accept-Profile": "bronze_ch",
                "Prefer": "count=exact",
            },
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

def process_layer(cfg: dict, run_id: str, re_url: str, re_key: str,
                  cc_url: str, cc_key: str, force_delta: bool) -> dict:
    service = cfg["service"]
    print(f"\n{'━' * 68}\n  {cfg['name']}  ({service})\n{'━' * 68}")

    layer_id = resolve_layer_id(service)
    print(f"    layer id: {layer_id}")

    declared = source_count(service, layer_id)
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
                f"Nothing was written. Review the layer at source, then re-run with "
                f"--force-delta if the move is genuine."
            )
    else:
        print("    previous run: none recorded (first load)")

    # ── Fetch + count assertion ───────────────────────────────
    feats = fetch_features(service, layer_id)
    if len(feats) != declared:
        raise RuntimeError(
            f"{service}: paginated total {len(feats):,} != declared {declared:,}. "
            f"Aborting rather than landing a partial layer."
        )
    print(f"    count assertion OK: {len(feats):,}")

    rows = [
        {"attrs": f.get("properties") or {}, "geom": f.get("geometry")}
        for f in feats
    ]
    missing_geom = sum(1 for r in rows if r["geom"] is None)
    if missing_geom:
        print(f"    warning: {missing_geom:,} features have no geometry")

    before = bronze_count(re_url, re_key, cfg["table"])

    # ── Upsert in batches ─────────────────────────────────────
    received = skipped = collapsed = affected = 0
    bs = cfg["batch_size"]
    for i in range(0, len(rows), bs):
        chunk = rows[i:i + bs]
        res = call_rpc(re_url, re_key, cfg["rpc"], {"p_rows": chunk, "p_run_id": run_id})
        row = res[0] if isinstance(res, list) and res else (res or {})
        received += row.get("received", 0)
        skipped += row.get("skipped_no_geom", 0)
        collapsed += row.get("collapsed", 0)
        affected += row.get("affected", 0)
        print(f"    upserted {min(i + bs, len(rows)):>5}/{len(rows):,}")

    # ── Soft-delete anything not seen this run ────────────────
    deleted = call_rpc(re_url, re_key, "ge_forest_soft_delete_missing",
                       {"p_table": cfg["table"], "p_run_id": run_id})
    if isinstance(deleted, list):
        deleted = deleted[0] if deleted else 0

    after = bronze_count(re_url, re_key, cfg["table"])

    print(f"\n    received:      {received:,}")
    print(f"    no geometry:   {skipped:,}   (excluded: no area, no location, nothing to compute)")
    print(f"    collapsed:     {collapsed:,}   (duplicate primary keys within a batch)")
    print(f"    affected:      {affected:,}")
    print(f"    soft-deleted:  {deleted:,}")
    print(f"    rows before:   {before if before is not None else 'unknown'}")
    print(f"    rows after:    {after if after is not None else 'unknown'}")

    if skipped:
        print(f"    NOTE: {skipped:,} source features carry no geometry and were excluded. "
              f"They are counted here on every run, never dropped silently.")
    if collapsed:
        print(f"    NOTE: {collapsed:,} rows shared a primary key with another row in "
              f"the same batch and were collapsed. Investigate before trusting counts.")

    if after is not None:
        update_dataset_meta(cc_url, cc_key, cfg["code"],
                            record_count=after, status="active")

    return {
        "service": service, "declared": declared, "fetched": len(feats),
        "received": received, "skipped": skipped, "collapsed": collapsed,
        "affected": affected, "deleted": deleted, "before": before, "after": after,
    }


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", action="append",
                    help="Service name to run (repeatable). Default: all.")
    ap.add_argument("--skip-optional", action="store_true",
                    help="Skip layers marked optional (FFP_FONCTION_PDF).")
    ap.add_argument("--force-delta", action="store_true",
                    help="Bypass the 20 percent row-count delta gate.")
    args = ap.parse_args()

    re_url = os.environ.get("RE_LLM_SUPABASE_URL", "")
    re_key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    cc_url = os.environ.get("CAMELOTE_SUPABASE_URL", "")
    cc_key = os.environ.get("CAMELOTE_SUPABASE_KEY", "")

    if not re_url or not re_key:
        print("ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required")
        return 1

    run_id = str(uuid.uuid4())
    selected = [
        c for c in LAYERS
        if (not args.layer or c["service"] in args.layer)
        and not (args.skip_optional and c["optional"])
    ]

    print("=" * 68)
    print("  SITG Geneva forest layers -> re-LLM bronze_ch")
    print(f"  run_id: {run_id}")
    print(f"  layers: {len(selected)}")
    print("=" * 68)

    results, failures = [], []
    for cfg in selected:
        try:
            results.append(process_layer(cfg, run_id, re_url, re_key,
                                         cc_url, cc_key, args.force_delta))
        except Exception as e:  # noqa: BLE001
            print(f"\n    FAILED: {e}")
            failures.append((cfg["service"], str(e)))

    print("\n" + "=" * 68)
    print("  SUMMARY")
    print("=" * 68)
    for r in results:
        print(f"  {r['service']:<30} src={r['declared']:>6,}  "
              f"after={str(r['after']):>7}  no-geom={r['skipped']:>3}  "
              f"collapsed={r['collapsed']:>3}  soft-deleted={r['deleted']:>3}")
    for svc, err in failures:
        print(f"  {svc:<30} FAILED: {err[:110]}")

    if failures:
        print(f"\n  {len(failures)} layer(s) failed.")
        return 1
    print(f"\n  All {len(results)} layer(s) OK. run_id {run_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
