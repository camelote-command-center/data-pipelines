#!/usr/bin/env python3
"""
vd_batiment_rcb — canton-VD building cadastre (RCB) ingest.

Source : agsgc.map.vd.ch ArcGIS REST  layer 39 (vd.batiment_rcb)
Bronze : bronze_ch.vd_batiment_rcb
Cadence: monthly (1st @ 04:00 UTC on VPS1)

Run shape:
  python3 -m pipelines.vd_enrichment.vd_batiment_rcb.run [--dry-run] [--limit N]

Env:
  SUPABASE_DB_URI   — session_pooler_uri from supabase-registry (re-llm entry)
  PARSER_HOST       — e.g. 'vps-145.223.82.190'  (defaults to socket.gethostname())
"""
from __future__ import annotations
import argparse
import logging
import os
import socket
import sys
import time
from datetime import datetime, timezone

from pipelines.vd_enrichment._shared.arcgis_query import (
    ArcGISConfig, iter_features, esri_to_wkt
)
from pipelines.vd_enrichment._shared.upsert import (
    UpsertResult, get_conn, upsert_batch, soft_delete_missing,
    record_run_start, record_run_finish,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("vd_batiment_rcb")

DATASET_CODE = "vd_batiment_rcb"
SOURCE_LAYER = "vd.batiment_rcb"
TABLE = "bronze_ch.vd_batiment_rcb"
PK_COLS = ("source_layer", "arcgis_objectid")

# Column order must match the INSERT statement. Tracking cols (last_seen_at, deleted_at)
# are appended automatically by upsert_batch.
COLUMNS = (
    "egid","no_cadastr","categorie_txt","classe_txt","statut_txt","cons_annee",
    "cons_perio_txt","surface_m2","nb_niv_tot",
    "chauf1_sys_txt","chauf1_nrg_txt","chauf2_sys_txt","chauf2_nrg_txt",
    "eau1_sys_txt","eau1_nrg_txt","eau2_sys_txt","eau2_nrg_txt",
    "sre_m2","abri_pci","no_camac","arcgis_objectid","geometry",
    "source_layer","canton_code","first_seen_at",
)


def _row_from_feature(f: dict, run_started_at) -> tuple:
    a = f.get("attributes") or {}
    g = f.get("geometry")
    wkt = esri_to_wkt(g)
    return (
        a.get("EGID"), a.get("NO_CADASTR"), a.get("CATEGORIE_TXT"), a.get("CLASSE_TXT"),
        a.get("STATUT_TXT"), a.get("CONS_ANNEE"), a.get("CONS_PERIO_TXT"),
        a.get("SURFACE"), a.get("NB_NIV_TOT"),
        a.get("CHAUF1_SYS_TXT"), a.get("CHAUF1_NRG_TXT"),
        a.get("CHAUF2_SYS_TXT"), a.get("CHAUF2_NRG_TXT"),
        a.get("EAU1_SYS_TXT"),   a.get("EAU1_NRG_TXT"),
        a.get("EAU2_SYS_TXT"),   a.get("EAU2_NRG_TXT"),
        a.get("SRE"), a.get("ABRI_PCI"), a.get("NO_CAMAC"),
        a.get("OBJECTID"), wkt,
        SOURCE_LAYER, "VD", run_started_at,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Pull and parse but do not write to bronze.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after N features (for smoke tests).")
    args = ap.parse_args()

    host = os.environ.get("PARSER_HOST") or f"vps-{socket.gethostname()}"
    run_started_at = datetime.now(timezone.utc)

    conn = get_conn() if not args.dry_run else None
    run_id = record_run_start(conn, DATASET_CODE, host) if conn else -1
    log.info(f"Run id={run_id}  host={host}  started_at={run_started_at.isoformat()}  dry_run={args.dry_run}")

    cfg = ArcGISConfig(layer_id=39, layer_name=SOURCE_LAYER, page_size=1000)
    batch: list[tuple] = []
    result = UpsertResult()
    seen = 0
    t0 = time.time()
    try:
        for f in iter_features(cfg):
            batch.append(_row_from_feature(f, run_started_at))
            seen += 1
            if len(batch) >= 1000:
                if conn:
                    upsert_batch(conn, TABLE, PK_COLS, COLUMNS, batch, run_started_at)
                    result.inserted += len(batch)
                batch.clear()
            if args.limit and seen >= args.limit:
                break
        if batch and conn:
            upsert_batch(conn, TABLE, PK_COLS, COLUMNS, batch, run_started_at)
            result.inserted += len(batch)

        # Soft-delete rows not seen this pass
        if conn:
            result.soft_deleted = soft_delete_missing(
                conn, TABLE, "source_layer", [SOURCE_LAYER], run_started_at
            )

        elapsed = time.time() - t0
        log.info(f"Done. features={seen} upserted={result.inserted} "
                 f"soft_deleted={result.soft_deleted} elapsed={elapsed:.1f}s")
        if conn:
            record_run_finish(conn, run_id, result, "success")
    except Exception as e:
        log.exception("Parser failed")
        if conn:
            record_run_finish(conn, run_id, result, "failed", str(e)[:1000])
        sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
