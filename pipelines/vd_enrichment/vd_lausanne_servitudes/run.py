#!/usr/bin/env python3
"""
vd_lausanne_servitudes — Lausanne public-servitudes ingest (8 source layers).

Source : map.lausanne.ch WFS, 8 layers consolidated into bronze_ch.vd_lausanne_servitudes
         with source_layer + geom_kind discriminators.
Bronze : bronze_ch.vd_lausanne_servitudes
Cadence: monthly (1st @ 04:00 UTC on VPS1)

The 8 source layers:
  bdcad_servitudes_passages_pub_{line,point,surf}
  bdcad_servitudes_passages_canalisations_pub_{line,point,surf}
  bdcad_servitudes_usage_pub_{line,surf}            (no _point variant)

For each, iterate WFS GetFeature, set source_layer per row, dedupe by
(source_layer, source_pk) via PK + ON CONFLICT in UPSERT.
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

from pipelines.vd_enrichment._shared.wfs_query import (
    WFSConfig, WFS_BASE, iter_features, geojson_geom_to_ewkt
)
from pipelines.vd_enrichment._shared.upsert import (
    UpsertResult, get_conn, upsert_batch, soft_delete_missing,
    record_run_start, record_run_finish,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("vd_lausanne_servitudes")

DATASET_CODE = "vd_lausanne_servitudes"
TABLE = "bronze_ch.vd_lausanne_servitudes"
PK_COLS = ("source_layer", "source_pk")

SOURCE_LAYERS = [
    "bdcad_servitudes_passages_pub_line",
    "bdcad_servitudes_passages_pub_point",
    "bdcad_servitudes_passages_pub_surf",
    "bdcad_servitudes_passages_canalisations_pub_line",
    "bdcad_servitudes_passages_canalisations_pub_point",
    "bdcad_servitudes_passages_canalisations_pub_surf",
    "bdcad_servitudes_usage_pub_line",
    "bdcad_servitudes_usage_pub_surf",
]

COLUMNS = (
    "type_txt", "nom", "id_rf", "lien_idgo",
    "source_pk", "geometry",
    "source_layer", "geom_kind",
    "canton_code", "first_seen_at",
)


def _geom_kind_from_layer(source_layer: str) -> str:
    """Last underscore segment: 'line' | 'point' | 'surf'."""
    return source_layer.rsplit("_", 1)[-1]


def _row_from_feature(f: dict, source_layer: str, run_started_at) -> tuple | None:
    props = f.get("properties", {}) or {}
    geom = f.get("geometry")
    source_pk = f.get("id") or props.get("gml:id") or props.get("id_rf") or props.get("nom")
    if not source_pk:
        return None
    wkt = geojson_geom_to_ewkt(geom, srid=2056)
    return (
        props.get("type"),
        props.get("nom"),
        props.get("id_rf"),
        props.get("lien_idgo"),
        str(source_pk),
        wkt,
        source_layer,
        _geom_kind_from_layer(source_layer),
        "VD",
        run_started_at,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after N features total across all source layers")
    args = ap.parse_args()

    host = os.environ.get("PARSER_HOST") or f"vps-{socket.gethostname()}"
    run_started_at = datetime.now(timezone.utc)

    conn = get_conn() if not args.dry_run else None
    run_id = record_run_start(conn, DATASET_CODE, host) if conn else -1
    log.info(f"Run id={run_id} host={host} started_at={run_started_at.isoformat()} dry_run={args.dry_run}")

    result = UpsertResult()
    total_seen = 0
    per_layer_counts: dict[str, int] = {}
    t0 = time.time()
    first_feature_logged = False

    try:
        for source_layer in SOURCE_LAYERS:
            cfg = WFSConfig(layer_name=source_layer, srs=2056,
                            max_features=args.limit if args.limit else None)
            endpoint = (WFS_BASE
                        + f"&SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature"
                        + f"&TYPENAME=ms:{source_layer}&OUTPUTFORMAT=geojson&SRSNAME=EPSG:2056")
            log.info(f"SOURCE_ENDPOINT[{source_layer}]: {endpoint}")

            layer_seen = 0
            batch: list[tuple] = []
            for f in iter_features(cfg):
                row = _row_from_feature(f, source_layer, run_started_at)
                if row is None:
                    continue
                batch.append(row)
                layer_seen += 1
                total_seen += 1
                if not first_feature_logged:
                    log.info("FIRST_FEATURE_DEBUG " + json.dumps({
                        "source_endpoint": endpoint,
                        "source_layer": source_layer,
                        "geom_kind": _geom_kind_from_layer(source_layer),
                        "raw_properties": f.get("properties", {}),
                        "raw_geometry_type": (f.get("geometry") or {}).get("type"),
                        "parsed_columns": COLUMNS,
                        "parsed_row": [str(v)[:200] if v is not None else None for v in batch[-1]],
                    }, default=str, ensure_ascii=False))
                    first_feature_logged = True
                if len(batch) >= 1000:
                    if conn:
                        upsert_batch(conn, TABLE, PK_COLS, COLUMNS, batch, run_started_at)
                        result.inserted += len(batch)
                    batch.clear()
                if args.limit and total_seen >= args.limit:
                    break

            if batch and conn:
                upsert_batch(conn, TABLE, PK_COLS, COLUMNS, batch, run_started_at)
                result.inserted += len(batch)
            per_layer_counts[source_layer] = layer_seen
            log.info(f"LAYER_DONE[{source_layer}] features={layer_seen}  total_so_far={total_seen}")
            if args.limit and total_seen >= args.limit:
                log.info(f"Limit {args.limit} reached; stopping layer iteration")
                break

        if conn:
            result.soft_deleted = soft_delete_missing(
                conn, TABLE, "source_layer", SOURCE_LAYERS, run_started_at
            )

        elapsed = time.time() - t0
        log.info(f"PER_LAYER_COUNTS {json.dumps(per_layer_counts)}")
        log.info(f"Done. total_features={total_seen} upserted={result.inserted} "
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
