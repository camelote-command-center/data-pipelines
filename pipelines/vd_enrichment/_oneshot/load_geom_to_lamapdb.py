#!/usr/bin/env python3
"""
Load-by-replace (per canton) re-LLM gold_ch.export_*  ->  lamap_db ref.*

Scope: writes ONLY ref.rdppf_national, ref.zones_national_geom, ref.rdppf_national_geom.

Mechanism: client-side streamed load — same rationale as the 2026-07-15c load:
  * creates NO lamap_db object (no staging permitted),
  * puts NO re-LLM credential onto lamap_db (dblink would embed the password in SQL
    executed there, visible in pg_stat_activity / server logs).
Server-side named cursor on the re-LLM side so 600k+ rows never land in memory at once.

Load-by-replace: DELETE WHERE canton='VD' then INSERT, all inside ONE transaction per
table — so a failure rolls back to the previous contents rather than leaving a hole.
Geometry moves as EWKT and is re-parsed with ST_GeomFromEWKT on the target.

The IDENTITY `id` on both geometry targets is never inserted.
"""
import os, sys
import psycopg2, psycopg2.extras

RE_LLM = os.environ["RE_LLM_DB_URI"]
LAMAP  = os.environ["LAMAP_DB_URI"]
BATCH  = 2000

JOBS = [
    {
        "name": "ref.rdppf_national",
        "src": """
            SELECT egrid, canton_code, commune_bfs, theme, sous_type, is_restrictive,
                   libelle, overlap_m2, statut_juridique, date_entree_vigueur, updated_at
            FROM gold_ch.export_rdppf_national
        """,
        "cols": ["egrid", "canton", "commune_bfs", "theme", "sous_type", "is_restrictive",
                 "libelle", "overlap_m2", "statut_juridique", "date_entree_vigueur", "updated_at"],
        "geom_idx": None,
    },
    {
        # target has NO date_entree_vigueur and NO zone_id -> dropped
        "name": "ref.zones_national_geom",
        "src": """
            SELECT canton, zone_affectation, zone_primaire, zone_synthetique,
                   commune_bfs, statut_juridique, ST_AsEWKT(geometry), updated_at
            FROM gold_ch.export_zones_national_geom
        """,
        "cols": ["canton", "zone_affectation", "zone_primaire", "zone_synthetique",
                 "commune_bfs", "statut_juridique", "geom", "updated_at"],
        "geom_idx": 6,
    },
    {
        # target has NO statut_juridique and NO object_id -> dropped
        "name": "ref.rdppf_national_geom",
        "src": """
            SELECT canton, theme, sous_type, is_restrictive, libelle, commune_bfs,
                   geom_type, ST_AsEWKT(geometry), updated_at
            FROM gold_ch.export_rdppf_national_geom
        """,
        "cols": ["canton", "theme", "sous_type", "is_restrictive", "libelle", "commune_bfs",
                 "geom_type", "geom", "updated_at"],
        "geom_idx": 7,
    },
]


def main():
    src = psycopg2.connect(RE_LLM, connect_timeout=20)
    dst = psycopg2.connect(LAMAP, connect_timeout=20)
    dst.autocommit = False
    try:
        for job in JOBS:
            cols, gi = job["cols"], job["geom_idx"]
            tmpl = "(" + ", ".join(
                "ST_GeomFromEWKT(%s)" if i == gi else "%s" for i in range(len(cols))
            ) + ")"
            sql = f'INSERT INTO {job["name"]} ({", ".join(cols)}) VALUES %s'

            with dst.cursor() as dc:
                dc.execute(f"SET LOCAL statement_timeout = '1800s'")
                dc.execute(f"DELETE FROM {job['name']} WHERE canton = 'VD'")
                deleted = dc.rowcount

                cur = src.cursor(name=f'st_{job["name"].replace(".", "_")}')
                cur.itersize = BATCH
                cur.execute(job["src"])
                total = 0
                while True:
                    rows = cur.fetchmany(BATCH)
                    if not rows:
                        break
                    psycopg2.extras.execute_values(dc, sql, rows, template=tmpl, page_size=500)
                    total += len(rows)
                    if total % 100000 == 0:
                        print(f"  {job['name']}: {total:,}", flush=True)
                cur.close()
            dst.commit()
            print(f"✅ {job['name']}: deleted {deleted:,} VD, inserted {total:,}", flush=True)
    except Exception:
        dst.rollback()
        raise
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    main()
