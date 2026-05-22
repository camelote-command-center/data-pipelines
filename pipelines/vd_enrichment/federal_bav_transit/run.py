#!/usr/bin/env python3
"""
federal_bav_transit — federal BAV public-transit stops (national, ~29K).

Source : data.geo.admin.ch STAC asset for ch.bav.haltestellen-oev.
         haltestellen-oev_2056_fr.csv.zip / PointExploitation.csv
         CSV in EPSG:2056, French labels, ~29K rows. Refreshed by BAV ~weekly.

Bronze : bronze_ch.federal_bav_transit
Cadence: quarterly (Jan/Apr/Jul/Oct 1st @ 05:30 UTC on VPS3)
         Monthly run on same VPS is idempotent via UPSERT.

canton_code resolution: deliberately left NULL by the parser. BAV's
Commune_Numero field is a BAV-internal commune ID, NOT the federal BFS number
(confirmed 2026-05-22 dry-run: BAV says Nendaz Commune_Numero=6024 but federal
BFS=12518). Post-backfill, run:

    UPDATE bronze_ch.federal_bav_transit b
       SET canton_code = c.canton_code
      FROM bronze_ch.federal_communes c
     WHERE b.canton_code IS NULL AND b.geometry IS NOT NULL
       AND ST_Contains(c.geometry, b.geometry);

ST_Contains against federal_communes GIST index handles 29K rows in seconds.
"""
from __future__ import annotations
import argparse, json, logging, os, socket, sys, time
from datetime import datetime, timezone

from pipelines.vd_enrichment._shared.federal_query import (
    FederalCSVConfig, iter_csv_rows
)
from pipelines.vd_enrichment._shared.upsert import (
    UpsertResult, get_conn, upsert_batch, soft_delete_missing,
    record_run_start, record_run_finish,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("federal_bav_transit")

DATASET_CODE = "federal_bav_transit"
SOURCE_LAYER = "ch.bav.haltestellen-oev"
TABLE = "bronze_ch.federal_bav_transit"
PK_COLS = ("source_layer", "didok")

COLUMNS = (
    "didok", "name", "mode", "abbr",
    "validity_from", "validity_to", "accessibility",
    "gtfs_stop_id", "raw_attributes", "geometry", "canton_code",
    "source_layer", "first_seen_at",
)


def _parse_yyyymmdd(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    if len(s) != 8 or not s.isdigit():
        return None
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"




def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after N features")
    args = ap.parse_args()

    host = os.environ.get("PARSER_HOST") or f"vps-{socket.gethostname()}"
    run_started_at = datetime.now(timezone.utc)

    conn = get_conn() if (not args.dry_run or os.environ.get("SUPABASE_DB_URI")) else None
    run_id = record_run_start(conn, DATASET_CODE, host) if conn and not args.dry_run else -1
    log.info(f"Run id={run_id} host={host} started_at={run_started_at.isoformat()} dry_run={args.dry_run} table={TABLE}")

    cfg = FederalCSVConfig(
        stac_collection=SOURCE_LAYER,
        asset_glob="*_2056_fr.csv.zip",
        csv_filename="PointExploitation.csv",
    )
    log.info(f"SOURCE_ENDPOINT: STAC collection {SOURCE_LAYER} → *_2056_fr.csv.zip / PointExploitation.csv")

    batch: list[tuple] = []
    result = UpsertResult()
    seen = 0
    first_feature_logged = False
    t0 = time.time()

    try:
        for row in iter_csv_rows(cfg):
            didok = (row.get("Numero") or "").strip()
            if not didok:
                continue
            # coords
            try:
                e = float(row.get("E") or "")
                n = float(row.get("N") or "")
                wkt = f"SRID=2056;POINT({e} {n})"
            except (TypeError, ValueError):
                wkt = None
            # canton_code intentionally NULL — populated post-backfill via spatial join.
            tuple_row = (
                didok,
                (row.get("Nom") or "").strip() or None,
                (row.get("MoyenTransport_Designation") or "").strip().lower() or None,
                (row.get("Abreviation") or "").strip() or None,
                _parse_yyyymmdd(row.get("Validite_DebutValidite")),
                _parse_yyyymmdd(row.get("Validite_FinValidite")),
                None,
                None,
                json.dumps({k: v for k, v in row.items() if v}, ensure_ascii=False),
                wkt,
                None,                              # canton_code -> NULL (post-backfill UPDATE)
                SOURCE_LAYER,
                run_started_at,
            )
            batch.append(tuple_row)
            seen += 1

            if not first_feature_logged:
                log.info("FIRST_FEATURE_DEBUG " + json.dumps({
                    "source_endpoint": f"STAC:{SOURCE_LAYER}/PointExploitation.csv",
                    "source_layer": SOURCE_LAYER,
                    "raw_csv_keys": list(row.keys()),
                    "raw_csv_sample": {k: v for k, v in list(row.items())[:18]},
                    "parsed_columns": list(COLUMNS),
                    "parsed_row_preview": [str(v)[:160] if v is not None else None for v in tuple_row][:13],
                    "note_canton": "NULL by design — populated post-backfill via ST_Contains spatial join (see module docstring)",
                }, default=str, ensure_ascii=False))
                first_feature_logged = True

            if len(batch) >= 500:
                if conn and not args.dry_run:
                    upsert_batch(conn, TABLE, PK_COLS, COLUMNS, batch, run_started_at)
                    result.inserted += len(batch)
                batch.clear()
            if args.limit and seen >= args.limit:
                log.info(f"Limit {args.limit} reached")
                break

        if batch and conn and not args.dry_run:
            upsert_batch(conn, TABLE, PK_COLS, COLUMNS, batch, run_started_at)
            result.inserted += len(batch)

        if conn and not args.dry_run:
            result.soft_deleted = soft_delete_missing(
                conn, TABLE, "source_layer", [SOURCE_LAYER], run_started_at
            )

        elapsed = time.time() - t0
        log.info(f"Done. features={seen} upserted={result.inserted} "
                 f"soft_deleted={result.soft_deleted} elapsed={elapsed:.1f}s")
        if conn and not args.dry_run:
            record_run_finish(conn, run_id, result, "success")
    except Exception as e:
        log.exception("Parser failed")
        if conn and not args.dry_run:
            record_run_finish(conn, run_id, result, "failed", str(e)[:1000])
        sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
