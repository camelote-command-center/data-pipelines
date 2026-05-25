"""
Bronze UPSERT + soft-delete refresh helper.

Hard rules enforced here:
  - UPSERT only, never TRUNCATE.
  - Soft-delete via deleted_at on rows not seen in current refresh pass.
  - On UPSERT: last_seen_at = run_started_at, deleted_at = NULL.
  - run_started_at is fixed at parser start; all rows in this run share it.

Postgres connection: psycopg2 with `session_pooler_uri` from env (SUPABASE_DB_URI).
"""
from __future__ import annotations
import logging
import os
import time
from dataclasses import dataclass
from typing import Iterable, Sequence

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


@dataclass
class UpsertResult:
    inserted: int = 0
    updated: int = 0
    soft_deleted: int = 0


def get_conn():
    uri = os.environ.get("SUPABASE_DB_URI") or os.environ.get("RE_LLM_DB_URI")
    if not uri:
        raise RuntimeError(
            "SUPABASE_DB_URI (or RE_LLM_DB_URI) env var required. "
            "Use the session_pooler_uri from supabase-projects.json — NEVER db.<ref>.supabase.co."
        )
    return psycopg2.connect(uri, connect_timeout=15)


def upsert_batch(
    conn,
    table: str,                          # e.g. 'bronze_ch.vd_batiment_rcb'
    pk_cols: Sequence[str],              # e.g. ('source_layer','arcgis_objectid')
    columns: Sequence[str],              # all non-tracking columns + source_layer
    rows: Sequence[tuple],
    run_started_at,
) -> int:
    """
    UPSERT a batch into bronze. Sets last_seen_at = run_started_at, deleted_at = NULL,
    on every touched row. Returns row-affected count (insert+update).
    """
    if not rows:
        return 0

    col_list = ", ".join(columns)
    excluded_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c not in pk_cols
    )
    pk_list = ", ".join(pk_cols)
    sql = f"""
        INSERT INTO {table} ({col_list}, last_seen_at, deleted_at)
        VALUES %s
        ON CONFLICT ({pk_list}) DO UPDATE SET
          {excluded_set},
          last_seen_at = EXCLUDED.last_seen_at,
          deleted_at   = NULL
    """
    # extend each row tuple with (run_started_at, NULL)
    extended = [(*r, run_started_at, None) for r in rows]
    # Build per-column placeholders. Wrap any column named 'geometry' with
    # ST_MakeValid(...) so invalid source-side topology can't get into bronze
    # (mitigation for canton-VD polygon-validity issues — see bug 81dee985 +
    # migration 20260522000002). ST_MakeValid is shape-preserving for typical
    # invalid topologies (self-intersections, ring orientation).
    def _ph(col: str) -> str:
        return "ST_MakeValid(%s::geometry)" if col == "geometry" else "%s"
    template = "(" + ", ".join(_ph(c) for c in columns) + ", %s, %s)"

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, extended, template=template, page_size=500)
    conn.commit()
    return len(rows)


def soft_delete_missing(
    conn,
    table: str,
    source_layer_col: str,
    source_layer_values: Sequence[str],
    run_started_at,
) -> int:
    """
    Mark rows not touched in the current run (last_seen_at < run_started_at) as deleted.
    """
    sql = f"""
        UPDATE {table}
           SET deleted_at = %s
         WHERE {source_layer_col} = ANY(%s)
           AND last_seen_at < %s
           AND deleted_at IS NULL
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_started_at, list(source_layer_values), run_started_at))
        affected = cur.rowcount
    conn.commit()
    return affected


def record_run_start(conn, dataset_code: str, host: str) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bronze_ch.vd_enrichment_runs (dataset_code, host, status)
            VALUES (%s, %s, 'running')
            RETURNING id
        """, (dataset_code, host))
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def record_run_finish(conn, run_id: int, result: UpsertResult, status: str, error: str | None = None):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE bronze_ch.vd_enrichment_runs
               SET finished_at = now(),
                   rows_inserted = %s,
                   rows_updated = %s,
                   rows_softdeleted = %s,
                   status = %s,
                   error_message = %s
             WHERE id = %s
        """, (result.inserted, result.updated, result.soft_deleted, status, error, run_id))
    conn.commit()
