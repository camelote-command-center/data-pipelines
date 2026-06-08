import json, sys, re, unicodedata, psycopg, collections
sys.path.insert(0,'src')
from pdcom_parser.ingest import match_pdf_to_commune
from pathlib import Path
RELLM="postgresql://postgres.znrvddgmczdqoucmykij:SUpolkmn098%24@aws-1-eu-west-1.pooler.supabase.com:5432/postgres"
LAMAP="postgresql://postgres.fckdwddgtdbvhzloejni:SUpolkmn098%24@aws-1-eu-central-2.pooler.supabase.com:5432/postgres"
prof={o['pdf']:o for o in json.load(open("/tmp/profile.json")) if 'archetype' in o}

# resolve commune per pdf
with psycopg.connect(LAMAP) as c, c.cursor() as cur:
    cur.execute("SELECT commune_bfs,commune_name,canton_code, ST_AsGeoJSON(ST_Transform(geometry,2056)) FROM ref.communes WHERE canton_code='GE'")
    communes=[{"commune_bfs":r[0],"commune_name":r[1],"canton_code":r[2]} for r in cur.fetchall()]
D=Path("/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese")
pdf2comm={}
for pdf in prof:
    try:
        m=match_pdf_to_commune(D/pdf, communes)
        pdf2comm[pdf]=m.commune_name
    except Exception: pdf2comm[pdf]=None

# current gold rows per commune
with psycopg.connect(RELLM) as c, c.cursor() as cur:
    cur.execute("SELECT commune_name,category_key,geom_type,source_color,feature_count FROM gold_ch.pdcom_urbanisation_consolidated")
    gold=collections.defaultdict(list)
    for nm,k,gt,col,fc in cur.fetchall(): gold[nm].append((k,gt,col,fc))

def is_grey(h):
    if not h or len(h)!=7: return False
    r,g,b=int(h[1:3],16),int(h[3:5],16),int(h[5:7],16); return abs(r-g)<14 and abs(g-b)<14 and abs(r-b)<14
def is_baseish(h):  # grey/white/black/near-base = NOT a planning fill
    if not h: return True
    if is_grey(h): return True
    return h in ('#ffffff','#000000','#010101','#fefefe')

# Mislabel + impoverishment per commune (communes with current data)
print(f"{'commune':<20}{'arch':<26}{'poly_rows':>9}{'line_rows':>9}{'base_poly':>9}  flags")
rows_summary=[]
for nm in sorted(gold):
    rws=gold[nm]
    poly=[r for r in rws if r[1]=='polygon']; line=[r for r in rws if r[1]=='line']
    base_poly=[r for r in poly if is_baseish(r[2])]
    # find archetype of this commune's pdfs
    archs=set(prof[p]['archetype'] for p,cm in pdf2comm.items() if cm==nm)
    flags=[]
    if len(poly)==0: flags.append("NO-POLYGONS(all-suppressed-lines)")
    if base_poly: flags.append(f"{len(base_poly)}-base/grey-poly-mislabeled")
    # pale yellow as zone_5?
    for k,gt,col,fc in poly:
        if col and col.lower() in ('#f6f6ca','#f3efb9','#ffff40','#ede80c','#fde9d9','#ffec83') and k=='zone_5':
            flags.append(f"paleyellow->{k}(likely potentiel_densification)")
    arch=";".join(sorted(archs)) or "?"
    print(f"{nm:<20}{arch[:25]:<26}{len(poly):>9}{len(line):>9}{len(base_poly):>9}  {'; '.join(flags)}")
    rows_summary.append((nm,arch,len(poly),len(line),len(base_poly),flags))

# communes with PDFs but NO current data (fully impoverished)
have=set(gold); allcomm=set(v for v in pdf2comm.values() if v)
print("\nCommunes with synthese PDF but ZERO current rows:", sorted(allcomm-have))
print(f"\nTotals: {len(gold)} communes have rows; "
      f"{sum(1 for r in rows_summary if r[2]==0)} have NO displayable polygons; "
      f"{sum(1 for r in rows_summary if r[4]>0)} have base/grey mislabeled polygons")
json.dump({"pdf2comm":pdf2comm,"summary":[(r[0],r[1],r[2],r[3],r[4],r[5]) for r in rows_summary]}, open("/tmp/findings.json","w"), ensure_ascii=False)
