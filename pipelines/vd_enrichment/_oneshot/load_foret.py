import json, os, sys, datetime
import psycopg2, psycopg2.extras
NDJSON, DB = sys.argv[1], os.environ["RE_LLM_DB_URI"]
RUN = datetime.datetime.now(datetime.timezone.utc)
def ms(v):
    if v in (None,""): return None
    try: return datetime.datetime.fromtimestamp(int(v)/1000.0, datetime.timezone.utc)
    except Exception: return None
def ewkt(g):
    if not g: return None
    if g.get("paths"):
        parts = [", ".join(f"{p[0]} {p[1]}" for p in path) for path in g["paths"] if len(path) >= 2]
        return f"SRID=2056;MULTILINESTRING({','.join('('+p+')' for p in parts)})" if parts else None
    if g.get("rings"):
        polys = [f"(({', '.join(f'{p[0]} {p[1]}' for p in r)}))" for r in g["rings"] if len(r) >= 4]
        return f"SRID=2056;MULTIPOLYGON({','.join(polys)})" if polys else None
    return None
rows, skip = [], 0
for line in open(NDJSON, encoding="utf-8"):
    r = json.loads(line); a = r.get("attributes") or {}; w = ewkt(r.get("geometry"))
    if not w: skip += 1; continue
    rows.append((psycopg2.extras.Json(a), r["layer"], a.get("OBJECTID"), a.get("CODE"),
                 a.get("DESIGNATION") or a.get("NAME"), a.get("STATUT_JURIDIQUE"), ms(a.get("DATE_EV")), w))
cols = ["raw_data","source_layer","arcgis_objectid","code","designation","statut_juridique","date_entree_vigueur","geometry"]
# ST_MakeValid only — NEVER CollectionExtract(,3): returns EMPTY for the /489 lines
tmpl = "(" + ", ".join("ST_MakeValid(%s::geometry)" if c=="geometry" else "%s" for c in cols) + ", %s, %s)"
upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in ("source_layer","arcgis_objectid"))
sql = (f'INSERT INTO bronze_ch.vd_foret ({", ".join(cols)}, last_seen_at, deleted_at) VALUES %s '
       f'ON CONFLICT (source_layer, arcgis_objectid) DO UPDATE SET {upd}, last_seen_at=EXCLUDED.last_seen_at, deleted_at=NULL')
conn = psycopg2.connect(DB, connect_timeout=20)
with conn.cursor() as cur:
    psycopg2.extras.execute_values(cur, sql, [(*r, RUN, None) for r in rows], template=tmpl, page_size=300)
conn.commit(); conn.close()
print(f"upserted {len(rows)} | skipped-no-geom {skip}")
