"""
Shared base for canton-VD ArcGIS REST parsers (Variant A).

Each Variant A parser is a thin config-only entry point that calls run_arcgis_parser()
with its layer_id, source_layer, table, primary key columns, column tuple, and a
mapper function (raw feature → row tuple).

Single-layer and multi-layer cases both use this — multi-layer parsers iterate
layer configs and call this once per (layer_id, source_layer) pair.
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

from .arcgis_query import ArcGISConfig, ARCGIS_BASE, iter_features, esri_to_wkt
from .upsert import (
    UpsertResult, get_conn, upsert_batch, soft_delete_missing,
    record_run_start, record_run_finish,
)


@dataclass
class LayerSpec:
    layer_id: int
    source_layer: str


def run_arcgis_parser(
    *,
    dataset_code: str,
    table: str,                                # 'bronze_ch.vd_xxx'
    pk_cols: Sequence[str],                    # ('source_layer','arcgis_objectid')
    columns: Sequence[str],                    # bronze column order (incl. trailing source_layer, canton_code, first_seen_at)
    layers: Sequence[LayerSpec],               # 1+ layer specs
    row_mapper: Callable[[dict, str, datetime], tuple | None],
    parser_module: str,                        # for log identification
) -> int:
    """
    Generic Variant A driver. Returns process exit code (0 on success, 1 on parser error).
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger(parser_module)

    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after N features total across all configured layers")
    ap.add_argument("--where", type=str, default=None,
                    help="ArcGIS WHERE clause override (default: 1=1). "
                         "Use for chunked parallel runs, e.g. 'MOD(OBJECTID,10)=0'.")
    ap.add_argument("--shard-tag", type=str, default=None,
                    help="Tag in the run log to identify which shard this process is.")
    args = ap.parse_args()

    host = os.environ.get("PARSER_HOST") or f"vps-{socket.gethostname()}"
    run_started_at = datetime.now(timezone.utc)

    conn = get_conn() if not args.dry_run else None
    run_id = record_run_start(conn, dataset_code, host) if conn else -1
    log.info(f"Run id={run_id} host={host} started_at={run_started_at.isoformat()} "
             f"dry_run={args.dry_run} table={table} "
             f"where={args.where or '1=1'} shard_tag={args.shard_tag or '-'}")

    result = UpsertResult()
    total_seen = 0
    per_layer_counts: dict[str, int] = {}
    t0 = time.time()
    first_feature_logged = False
    multi_layer = len(layers) > 1

    try:
        for spec in layers:
            endpoint = f"{ARCGIS_BASE}/{spec.layer_id}/query"
            log.info(f"SOURCE_ENDPOINT[{spec.source_layer}]: {endpoint}")
            cfg = ArcGISConfig(layer_id=spec.layer_id, layer_name=spec.source_layer,
                               page_size=1000, where=args.where or "1=1")
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
                        "raw_attributes": f.get("attributes", {}),
                        "raw_geometry_keys": list((f.get("geometry") or {}).keys()),
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

        # Skip soft-delete in sharded mode: each shard only touches its own slice,
        # so the "rows untouched this run" set spuriously includes other shards' rows.
        # Soft-delete is the responsibility of an unshardged "reaper" pass run after
        # all shards complete.
        if conn and not args.where:
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
