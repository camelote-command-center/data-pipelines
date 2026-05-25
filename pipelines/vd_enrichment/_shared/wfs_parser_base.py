"""
Shared base for Lausanne WFS parsers (Variant B).

Same shape as arcgis_parser_base but driven by wfs_query.iter_features (GML 3.1.1).
Single-layer and multi-layer cases both use this — multi-layer parsers iterate
layer configs and call this once per layer.
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import socket
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Sequence

from .wfs_query import WFSConfig, WFS_BASE, iter_features, geojson_geom_to_ewkt
from .upsert import (
    UpsertResult, get_conn, upsert_batch, soft_delete_missing,
    record_run_start, record_run_finish,
)


@dataclass
class WFSLayerSpec:
    source_layer: str


def run_wfs_parser(
    *,
    dataset_code: str,
    table: str,
    pk_cols: Sequence[str],
    columns: Sequence[str],
    layers: Sequence[WFSLayerSpec],
    row_mapper: Callable[[dict, str, datetime], tuple | None],
    parser_module: str,
) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger(parser_module)

    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    host = os.environ.get("PARSER_HOST") or f"vps-{socket.gethostname()}"
    run_started_at = datetime.now(timezone.utc)

    conn = get_conn() if not args.dry_run else None
    run_id = record_run_start(conn, dataset_code, host) if conn else -1
    log.info(f"Run id={run_id} host={host} started_at={run_started_at.isoformat()} "
             f"dry_run={args.dry_run} table={table}")

    result = UpsertResult()
    total_seen = 0
    per_layer_counts: dict[str, int] = {}
    t0 = time.time()
    first_feature_logged = False
    multi_layer = len(layers) > 1

    try:
        for spec in layers:
            cfg = WFSConfig(layer_name=spec.source_layer, srs=2056,
                            max_features=args.limit if args.limit else None)
            endpoint = (WFS_BASE
                        + f"&SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature"
                        + f"&TYPENAME=ms:{spec.source_layer}&SRSNAME=EPSG:2056")
            log.info(f"SOURCE_ENDPOINT[{spec.source_layer}]: {endpoint}")
            layer_seen = 0
            batch: list[tuple] = []
            for f in iter_features(cfg):
                row = row_mapper(f, spec.source_layer, run_started_at)
                if row is None:
                    continue
                batch.append(row)
                layer_seen += 1
                total_seen += 1
                if not first_feature_logged:
                    log.info("FIRST_FEATURE_DEBUG " + json.dumps({
                        "source_endpoint": endpoint,
                        "source_layer": spec.source_layer,
                        "raw_properties": f.get("properties", {}),
                        "raw_geometry_type": (f.get("geometry") or {}).get("type"),
                        "parsed_columns": list(columns),
                        "parsed_row": [str(v)[:200] if v is not None else None for v in row],
                    }, default=str, ensure_ascii=False))
                    first_feature_logged = True
                if len(batch) >= 500:
                    if conn:
                        upsert_batch(conn, table, pk_cols, columns, batch, run_started_at)
                        result.inserted += len(batch)
                    batch.clear()
                if args.limit and total_seen >= args.limit:
                    break
            if batch and conn:
                upsert_batch(conn, table, pk_cols, columns, batch, run_started_at)
                result.inserted += len(batch)
            per_layer_counts[spec.source_layer] = layer_seen
            if multi_layer:
                log.info(f"LAYER_DONE[{spec.source_layer}] features={layer_seen}  total_so_far={total_seen}")
            if args.limit and total_seen >= args.limit:
                log.info(f"Limit {args.limit} reached; stopping layer iteration")
                break

        if conn:
            result.soft_deleted = soft_delete_missing(
                conn, table, "source_layer", [s.source_layer for s in layers], run_started_at
            )

        elapsed = time.time() - t0
        if multi_layer:
            log.info(f"PER_LAYER_COUNTS {json.dumps(per_layer_counts)}")
        log.info(f"Done. total_features={total_seen} upserted={result.inserted} "
                 f"soft_deleted={result.soft_deleted} elapsed={elapsed:.1f}s")
        if conn:
            record_run_finish(conn, run_id, result, "success")
        return 0
    except Exception as e:
        log.exception("Parser failed")
        if conn:
            record_run_finish(conn, run_id, result, "failed", str(e)[:1000])
        return 1
    finally:
        if conn:
            conn.close()
