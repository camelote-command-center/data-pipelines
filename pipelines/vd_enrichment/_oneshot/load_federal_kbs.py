#!/usr/bin/env python3
"""Load federal KbS NDJSON -> bronze_ch.federal_kbs_sites. UPSERT, never TRUNCATE.
Geometry is MIXED point+polygon -> ST_MakeValid only (NEVER CollectionExtract: it
returns EMPTY for points)."""
import json, os, sys, datetime
import psycopg2, psycopg2.extras

NDJSON, DB = sys.argv[1], os.environ["RE_LLM_DB_URI"]
RUN = datetime.datetime.now(datetime.timezone.utc)
LAYER = {"militaire":"ch.vbs.kataster-belasteter-standorte-militaer",
         "aeroports":"ch.bazl.kataster-belasteter-standorte-zivilflugplaetze",
         "transports_publics":"ch.bav.kataster-belasteter-standorte-oev"}

def geojson_to_ewkt(g):
    if not g or not g.get("type"): return None
    return "SRID=2056;" + json.dumps(g)   # placeholder, replaced below

rows, nogeom = [], {}
for line in open(NDJSON, encoding="utf-8"):
    r = json.loads(line)
    g, a = r.get("geometry"), r.get("attributes") or {}
    if not g or not g.get("coordinates"):
        nogeom[r["registre"]] = nogeom.get(r["registre"],0)+1; continue
    rows.append((
        psycopg2.extras.Json(a), r["registre"], str(r["id"]),
        a.get("katasternummer"), a.get("statusaltlv_fr"), a.get("standorttyp_fr"),
        a.get("untersuchungsmassnahmen_fr"), a.get("url_fr"),
        json.dumps(g), LAYER[r["registre"]],
    ))

cols = ["raw_data","registre","feature_id","katasternummer","statut","standorttyp",
        "untersuchungsmassnahmen","url_fiche","geometry","source_layer"]
tmpl = "(" + ", ".join(
    "ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056))" if c=="geometry" else "%s"
    for c in cols) + ", %s, %s)"
upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in ("registre","feature_id"))
sql = (f'INSERT INTO bronze_ch.federal_kbs_sites ({", ".join(cols)}, last_seen_at, deleted_at) '
       f'VALUES %s ON CONFLICT (registre, feature_id) DO UPDATE SET {upd}, '
       f'last_seen_at=EXCLUDED.last_seen_at, deleted_at=NULL')
conn = psycopg2.connect(DB, connect_timeout=20)
with conn.cursor() as cur:
    psycopg2.extras.execute_values(cur, sql, [(*r, RUN, None) for r in rows], template=tmpl, page_size=200)
conn.commit()
with conn.cursor() as cur:
    cur.execute("UPDATE bronze_ch.federal_kbs_sites SET deleted_at=%s WHERE last_seen_at<%s AND deleted_at IS NULL",(RUN,RUN))
    sd = cur.rowcount
conn.commit(); conn.close()
print(f"upserted {len(rows)} | soft-deleted {sd} | skipped-no-geom {nogeom}")
