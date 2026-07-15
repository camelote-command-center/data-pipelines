#!/usr/bin/env python3
"""
Load the two VD national exports  re-LLM gold_ch.export_*  ->  lamap_db ref.*_national

Mechanism: client-side streamed UPSERT.
  * Creates NO lamap_db object (brief: cleared for these two tables ONLY, no staging).
  * Puts NO re-LLM credential into lamap_db (dblink would embed the password in the
    executed SQL, visible in pg_stat_activity and server logs on lamap_db).
  * Server-side named cursor on the re-LLM side so 650k rows never land in memory at once.
UPSERT only — never TRUNCATE. Re-runnable.

Column mapping notes:
  * export `canton_code` -> target `canton`   (value 'VD')
  * export `parcel_number` is DROPPED — the targets have no such column.
"""
import os, sys
import psycopg2, psycopg2.extras

RE_LLM = os.environ["RE_LLM_DB_URI"]
LAMAP  = os.environ["LAMAP_DB_URI"]
BATCH  = 5000

JOBS = [
    {
        "name": "ref.plot_zoning_national",
        "src": """
            SELECT egrid, canton_code, commune_bfs, zone_affectation, zone_primaire,
                   zone_synthetique, ius, cos, spb, cm, igt, statut_juridique,
                   date_entree_vigueur, updated_at
            FROM gold_ch.export_plot_zoning_national
        """,
        "cols": ["egrid", "canton", "commune_bfs", "zone_affectation", "zone_primaire",
                 "zone_synthetique", "ius", "cos", "spb", "cm", "igt", "statut_juridique",
                 "date_entree_vigueur", "updated_at"],
        "conflict": "(egrid)",
    },
    {
        "name": "ref.rdppf_national",
        "src": """
            SELECT egrid, canton_code, commune_bfs, theme, sous_type, is_restrictive,
                   libelle, overlap_m2, statut_juridique, date_entree_vigueur, updated_at
            FROM gold_ch.export_rdppf_national
        """,
        "cols": ["egrid", "canton", "commune_bfs", "theme", "sous_type", "is_restrictive",
                 "libelle", "overlap_m2", "statut_juridique", "date_entree_vigueur", "updated_at"],
        # must match the target's expression index exactly:
        #   uq_rdppf_national_key UNIQUE, btree (egrid, theme, COALESCE(sous_type, ''::text))
        "conflict": "(egrid, theme, COALESCE(sous_type, ''::text))",
    },
]


def main():
    src = psycopg2.connect(RE_LLM, connect_timeout=20)
    dst = psycopg2.connect(LAMAP, connect_timeout=20)
    dst.autocommit = False
    try:
        for job in JOBS:
            cols = job["cols"]
            keycols = {"egrid", "theme", "sous_type"}
            upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in keycols)
            sql = (f'INSERT INTO {job["name"]} ({", ".join(cols)}) VALUES %s '
                   f'ON CONFLICT {job["conflict"]} DO UPDATE SET {upd}')

            cur = src.cursor(name=f'stream_{job["name"].replace(".", "_")}')
            cur.itersize = BATCH
            cur.execute(job["src"])

            total = 0
            with dst.cursor() as dc:
                while True:
                    rows = cur.fetchmany(BATCH)
                    if not rows:
                        break
                    psycopg2.extras.execute_values(dc, sql, rows, page_size=1000)
                    total += len(rows)
                    if total % 50000 == 0:
                        print(f"  {job['name']}: {total:,} upserted", flush=True)
            dst.commit()
            cur.close()
            print(f"✅ {job['name']}: {total:,} rows upserted", flush=True)
    except Exception:
        dst.rollback()
        raise
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    main()
