"""REOPEN audit step 1b/2 (2026-06-06): detailed per-layer fill+stroke
breakdown for the OCG-positive files, AFTER the indirect-Resources fix.
Diagnostic only. Classifies layers as category-polygon vs reference vs line."""
import sys, re, json
from pathlib import Path
sys.path.insert(0, str(Path("/Users/a/data-pipelines/pdcom-parser/src")))
import fitz
from pdcom_parser import ocg_layers as O

DIR = Path("/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese")
FILES = ["pdcom_cologny_2e_synthese.pdf","pdcom_dardagny.pdf",
         "pdcom_meinier_plan-synthese.pdf","pdcom_puplinge_plan-synthese-final.pdf",
         "pdcom_puplinge_plan-synthese-final-v2.pdf","pdcom_geneve_2e_atlas_transition.pdf"]

REF_PAT = re.compile(r"(cadastr|b[aâ]ti|fond|pochage|limite|l[eé]gend|legende|rep[eè]re|"
                     r"cours d'eau|liaison|pi[eé]ton|voie verte|navette|tc md|noeud|"
                     r"plan site communes|en t[eê]te|mobilit|masque|blanc|trame|stylo|"
                     r"impression|mise en page|arcgis|calque|dom|cad_)", re.I)

def detail(fn):
    doc = fitz.open(DIR/fn)
    # fills per layer (all pages)
    fills = {}
    for pno in range(doc.page_count):
        res = O.extract_layer_polys(doc[pno], doc, skip_geometry=set())
        for nm,d in res.items():
            fills[nm] = fills.get(nm,0) + len([p for p in d["polys"] if len(p)>=3])
    # stroke-capture: pass every layer as stroke_layer to count polylines too
    allnames = set(fills) | {v.get("name") for v in doc.get_ocgs().values() if v.get("name")}
    strokes = {}
    for pno in range(doc.page_count):
        res = O.extract_layer_polys(doc[pno], doc, skip_geometry=set(), stroke_layers=allnames)
        for nm,d in res.items():
            tot=len(d["polys"]); strokes[nm]=strokes.get(nm,0)+max(0,tot-fills.get(nm,0))
    doc.close()
    return fills, strokes

for fn in FILES:
    fills,strokes = detail(fn)
    cats = {nm:fills[nm] for nm in fills if fills[nm]>0 and not REF_PAT.search(nm)}
    print(f"\n### {fn}")
    print(f"  category-polygon layers (fills, non-ref): {len(cats)}")
    for nm,c in sorted(cats.items(), key=lambda k:-k[1])[:15]:
        print(f"      fills={c:<6} {nm}")
    # show top fill layers regardless (to see if ref names dominate)
    other = {nm:fills[nm] for nm in fills if fills[nm]>0 and REF_PAT.search(nm)}
    if other:
        top = sorted(other.items(), key=lambda k:-k[1])[:5]
        print(f"  (ref/skip fill layers: {', '.join(f'{n}={c}' for n,c in top)})")
