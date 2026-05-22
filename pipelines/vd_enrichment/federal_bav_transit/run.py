#!/usr/bin/env python3
"""
federal_bav_transit — federal BAV public-transit stops (national, ~31K).

Source : GeoAdmin api3.geo.admin.ch  /find  layer ch.bav.haltestellen-oev
Bronze : bronze_ch.federal_bav_transit
Cadence: quarterly (Jan/Apr/Jul/Oct 1st @ 05:30 UTC on VPS3)
         (monthly run on same VPS is idempotent via UPSERT.)

Strategy: walk Switzerland bounding box on a 50km × 50km grid (federal_query.py).
Each feature's canton_code derived via PostGIS spatial join against
bronze_ch.federal_communes (a single per-feature query, batched).
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime, timezone

from pipelines.vd_enrichment._shared.federal_query import (
    FederalConfig, GEOADMIN_FIND, iter_features
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


def _resolve_canton(conn, wkt: str | None) -> str | None:
    if not wkt or not conn:
        return None
    with conn.cursor() as cur:
        cur.execute("""
            SELECT canton FROM bronze_ch.federal_communes
             WHERE geometry IS NOT NULL
               AND ST_Contains(geometry, ST_GeomFromEWKT(%s))
             LIMIT 1
        """, (wkt,))
        row = cur.fetchone()
    return row[0] if row else None


def _row_from_feature(f: dict, run_started_at, canton_code: str | None) -> tuple | None:
    attrs = f.get("attributes", {}) or {}
    didok = attrs.get("didok") or attrs.get("number") or f.get("featureId")
    if not didok:
        return None
    geom = f.get("geometry")
    # GeoAdmin /find returns GeoJSON-shaped geometry when geometryFormat=geojson
    wkt = None
    if geom and "coordinates" in geom and geom.get("type") == "Point":
        c = geom["coordinates"]
        wkt = f"SRID=2056;POINT({c[0]} {c[1]})"
    elif geom and "x" in geom and "y" in geom:
        wkt = f"SRID=2056;POINT({geom['x']} {geom['y']})"
    return (
        str(didok),
        attrs.get("name") or attrs.get("Haltestellenname"),
        (attrs.get("mode") or attrs.get("verkehrsmittel") or "").lower() or None,
        attrs.get("abkuerzung") or attrs.get("abbr"),
        attrs.get("gueltig_von") or attrs.get("validity_from"),
        attrs.get("gueltig_bis") or attrs.get("validity_to"),
        attrs.get("accessibility") or attrs.get("rollstuhlgaengig"),
        attrs.get("sloid") or attrs.get("gtfs_stop_id"),
        json.dumps(attrs, ensure_ascii=False, default=str),
        wkt,
        canton_code,
        SOURCE_LAYER,
        run_started_at,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after N features total")
    args = ap.parse_args()

    host = os.environ.get("PARSER_HOST") or f"vps-{socket.gethostname()}"
    run_started_at = datetime.now(timezone.utc)

    # Even in --dry-run we need a (read-only) connection for the canton spatial-join.
    # If the env var is missing we skip the canton resolution and log NULL.
    conn = get_conn() if (not args.dry_run or os.environ.get("SUPABASE_DB_URI")) else None
    run_id = record_run_start(conn, DATASET_CODE, host) if conn and not args.dry_run else -1
    log.info(f"Run id={run_id} host={host} started_at={run_started_at.isoformat()} dry_run={args.dry_run}")

    cfg = FederalConfig(layer=SOURCE_LAYER)
    log.info(f"SOURCE_ENDPOINT: {GEOADMIN_FIND}  layer={cfg.layer}")

    batch: list[tuple] = []
    result = UpsertResult()
    seen = 0
    t0 = time.time()
    canton_counts: dict[str | None, int] = {}
    first_feature_logged = False

    try:
        for f in iter_features(cfg):
            attrs = f.get("attributes", {}) or {}
            didok = attrs.get("didok") or attrs.get("number") or f.get("featureId")
            geom = f.get("geometry")
            wkt = None
            if geom and "coordinates" in geom and geom.get("type") == "Point":
                c = geom["coordinates"]
                wkt = f"SRID=2056;POINT({c[0]} {c[1]})"
            elif geom and "x" in geom and "y" in geom:
                wkt = f"SRID=2056;POINT({geom['x']} {geom['y']})"

            canton_code = _resolve_canton(conn, wkt) if conn else None
            row = _row_from_feature(f, run_started_at, canton_code)
            if row is None:
                continue
            batch.append(row)
            seen += 1
            canton_counts[canton_code] = canton_counts.get(canton_code, 0) + 1

            if not first_feature_logged:
                log.info("FIRST_FEATURE_DEBUG " + json.dumps({
                    "source_endpoint": GEOADMIN_FIND,
                    "source_layer": SOURCE_LAYER,
                    "raw_attributes_keys": list(attrs.keys()),
                    "raw_attributes": {k: str(v)[:80] for k, v in list(attrs.items())[:20]},
                    "raw_geometry": geom,
                    "resolved_canton": canton_code,
                    "parsed_columns": COLUMNS,
                    "parsed_row_preview": [str(v)[:160] if v is not None else None for v in row][:13],
                }, default=str, ensure_ascii=False))
                first_feature_logged = True

            if len(batch) >= 500:
                if conn and not args.dry_run:
                    upsert_batch(conn, TABLE, PK_COLS, COLUMNS, batch, run_started_at)
                    result.inserted += len(batch)
                batch.clear()
            if args.limit and seen >= args.limit:
                log.info(f"Limit {args.limit} reached; breaking")
                break

        if batch and conn and not args.dry_run:
            upsert_batch(conn, TABLE, PK_COLS, COLUMNS, batch, run_started_at)
            result.inserted += len(batch)

        if conn and not args.dry_run:
            result.soft_deleted = soft_delete_missing(
                conn, TABLE, "source_layer", [SOURCE_LAYER], run_started_at
            )

        elapsed = time.time() - t0
        log.info(f"CANTON_DISTRIBUTION {json.dumps(canton_counts)}")
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
