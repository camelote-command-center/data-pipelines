from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from . import __version__ as PARSER_VERSION


def connect(url: str) -> psycopg.Connection:
    return psycopg.connect(url, autocommit=False)


def get_communes_geneva_lv95(db_url: str) -> list[dict]:
    """Fetch GE communes with LV95 boundary geometry from lamap_db."""
    rows = []
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT commune_bfs, commune_name, canton_code,
                   ST_AsGeoJSON(ST_Transform(geometry, 2056))::text AS geom_lv95
            FROM ref.communes
            WHERE canton_code = 'GE'
            ORDER BY commune_bfs
            """
        )
        for bfs, name, canton, geom in cur.fetchall():
            rows.append({
                "commune_bfs": bfs,
                "commune_name": name,
                "canton_code": canton,
                "boundary_lv95_geojson": json.loads(geom),
            })
    return rows


def load_commune_results(db_url: str, commune_bfs: int, commune_name: str, pdf_rec: dict, pages: list[dict], features_geojson_by_page: dict[int, list[dict]]) -> dict:
    """Upsert source, pages, features for one commune/PDF. Atomic per commune.
    features_geojson_by_page: {page_number: [feature_row, ...]}"""
    counts = {"sources": 0, "pages": 0, "features": 0}
    with connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bronze_ch.pdcom_sources (
                    commune_bfs, commune_name, canton_code, source_url, source_path,
                    pdf_filename, pdf_sha256, pdf_size_bytes, pdf_page_count,
                    parsed_at, parser_version, manifest_json
                )
                VALUES (%s,%s,'GE',%s,%s,%s,%s,%s,%s, now(), %s, %s)
                ON CONFLICT (commune_bfs, pdf_sha256) DO UPDATE SET
                    parsed_at = EXCLUDED.parsed_at,
                    parser_version = EXCLUDED.parser_version,
                    manifest_json = EXCLUDED.manifest_json,
                    source_path = EXCLUDED.source_path,
                    pdf_page_count = EXCLUDED.pdf_page_count
                RETURNING id
                """,
                (
                    commune_bfs, commune_name,
                    pdf_rec.get("source_url"),
                    pdf_rec["path"],
                    pdf_rec["filename"],
                    pdf_rec["sha256"],
                    pdf_rec["size_bytes"],
                    pdf_rec.get("page_count"),
                    PARSER_VERSION,
                    Jsonb(pdf_rec.get("manifest", {})),
                ),
            )
            source_id = cur.fetchone()[0]
            counts["sources"] = 1

            # Clean existing pages/features for this source (idempotent re-run)
            cur.execute("DELETE FROM bronze_ch.pdcom_features WHERE page_id IN (SELECT id FROM bronze_ch.pdcom_pages WHERE source_id = %s)", (source_id,))
            cur.execute("DELETE FROM bronze_ch.pdcom_pages WHERE source_id = %s", (source_id,))

            page_id_by_number: dict[int, str] = {}
            for p in pages:
                cur.execute(
                    """
                    INSERT INTO bronze_ch.pdcom_pages (
                        source_id, page_number, page_type, map_theme, map_title,
                        legend_json, drawing_count, has_raster, georef_confidence, extraction_status
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                    """,
                    (
                        source_id, p["page_number"], p["page_type"], p.get("map_theme"),
                        p.get("map_title"),
                        Jsonb(p.get("legend_json")) if p.get("legend_json") is not None else None,
                        p.get("drawing_count"), p.get("has_raster"),
                        p.get("georef_confidence"), p.get("extraction_status"),
                    ),
                )
                page_id_by_number[p["page_number"]] = cur.fetchone()[0]
                counts["pages"] += 1

            rows = []
            for page_num, feats in features_geojson_by_page.items():
                page_id = page_id_by_number.get(page_num)
                if not page_id:
                    continue
                for f in feats:
                    rows.append((
                        page_id, commune_bfs, f["map_theme"], f["label"], f["slug"],
                        f.get("color"), f["fill_type"],
                        json.dumps(f["geometry"]),
                        f.get("confidence"),
                        Jsonb(f.get("properties", {})),
                    ))
            if rows:
                # v0.4: chunk large inserts so Supabase pooler doesn't drop the
                # connection on PDFs with tens of thousands of features.
                CHUNK = 2000
                stmt = """
                INSERT INTO bronze_ch.pdcom_features (
                    page_id, commune_bfs, map_theme, layer_label, layer_slug,
                    source_color, fill_type, geometry, georef_confidence, properties
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 2056), %s, %s)
                """
                for start in range(0, len(rows), CHUNK):
                    cur.executemany(stmt, rows[start:start + CHUNK])
                counts["features"] = len(rows)
        conn.commit()
    return counts


def refresh_matviews(db_url: str) -> None:
    """Refresh silver + gold matviews on re-LLM after bronze loads finish."""
    with connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT gold_ch.refresh_pdcom_zones()")
        conn.commit()


def run_distribute(db_url: str) -> list[tuple]:
    """Invoke run_sync('weekly') on re-LLM to push gold_ch.pdcom_zones → all 3 consumers."""
    with connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT source, target, mode, rows_synced FROM gold_ch.run_sync('weekly')")
        rows = cur.fetchall()
        conn.commit()
    return rows


def count_ref_pdcom_zones(db_url: str) -> int:
    with connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM ref.pdcom_zones")
        return cur.fetchone()[0]


def count_silver_pdcom_zones(db_url: str) -> int:
    with connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM silver_ch.pdcom_zones")
        return cur.fetchone()[0]
