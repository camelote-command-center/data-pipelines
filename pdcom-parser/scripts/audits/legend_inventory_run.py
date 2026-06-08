"""Full legend inventory across the extractable corpus (vector-forms +
vector-flat-dense + vector-flat-sparse). Diagnostic only — no writes."""
import sys, json, fitz, re, unicodedata
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from legend_reader import read_legend
from pdcom_parser.ingest import match_pdf_to_commune
import psycopg
LAMAP="postgresql://postgres.fckdwddgtdbvhzloejni:SUpolkmn098%24@aws-1-eu-central-2.pooler.supabase.com:5432/postgres"
D=Path("/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese")
prof={o['pdf']:o for o in json.load(open("/tmp/profile.json")) if 'archetype' in o}
BUCKET={'vector-forms(hidden-fills)','vector-flat-dense','vector-flat-sparse'}
MOBI=re.compile(r'(pieton|piéton|cyclable|mobilit[eé] douce|itin[eé]raire|passerelle|chemins pour|r[eé]seau pi[eé]ton|tpg|navette|voie verte|travers[eé]e)',re.I)
def norm(s):
    s=unicodedata.normalize('NFD',s or ''); return ''.join(c for c in s if unicodedata.category(c)!='Mn').lower()
with psycopg.connect(LAMAP) as c, c.cursor() as cur:
    cur.execute("SELECT commune_bfs,commune_name,canton_code FROM ref.communes WHERE canton_code='GE'")
    communes=[{"commune_bfs":r[0],"commune_name":r[1],"canton_code":r[2]} for r in cur.fetchall()]
inv=[]; mobility=[]; unreadable=[]
for pdf,p in sorted(prof.items()):
    if p['archetype'] not in BUCKET: continue
    path=D/pdf
    if not path.exists(): continue
    try: commune=match_pdf_to_commune(path, communes).commune_name or "?"
    except Exception: commune="?"
    try:
        doc=fitz.open(path); ent=read_legend(doc[0],doc)
        txt=norm(doc[0].get_text())
        labels=[e['label'] for e in ent]
        joined=norm(" ".join(labels))+" "+txt[:2000]
        mob=len(MOBI.findall(joined)); urb=sum(joined.count(w) for w in ('densif','quartier','zone a bat','agricol','patrimoin','centralit','extension','village','protege','domaine'))
        if ent==[] :
            unreadable.append({"commune":commune,"pdf":pdf,"archetype":p['archetype']}); continue
        is_mob = mob>=3 and mob>urb
        if is_mob: mobility.append({"commune":commune,"pdf":pdf,"mobi_hits":mob,"urb_hits":urb})
        inv.append({"commune":commune,"pdf":pdf,"archetype":p['archetype'],"mobility":is_mob,
                    "n_entries":len(ent),"entries":ent})
    except Exception as e:
        unreadable.append({"commune":commune,"pdf":pdf,"archetype":p['archetype'],"error":str(e)})
json.dump({"inventory":inv,"mobility":mobility,"unreadable":unreadable}, open("scripts/audits/legend_inventory.json","w"), ensure_ascii=False, indent=1)
print(f"inventory communes: {len(inv)} | mobility-excluded: {len(mobility)} | legend-unreadable: {len(unreadable)}")
print("\nMOBILITY-EXCLUDED:")
for m in mobility: print(f"  {m['commune']:<18} {m['pdf']} (mobi={m['mobi_hits']} urb={m['urb_hits']})")
print("\nLEGEND-UNREADABLE (0 entries after hardening):")
for u in unreadable: print(f"  {u['commune']:<18} {u['pdf']} [{u['archetype']}]")
print("\nINVENTORY SAMPLE (first 3 communes):")
for it in inv[:3]:
    print(f"  == {it['commune']} ({it['n_entries']} entries, {it['archetype']}) ==")
    for e in it['entries'][:8]: print(f"      {e['color']} {e['kind']:<6} fc={e['feature_count']:<5} {e['label'][:55]!r}")
