"""REOPEN audit (2026-06-06): fresh per-file OCG layer-tagging detection across
EVERY GE synthesis PDF. Uses the recursive detector (extract_layer_polys, which
walks page content streams AND nested Form XObjects, tracking the OCG mc_stack
across recursion). Proves per-file whether /OC layer-tagged painted geometry
exists -- NOT a color test. Diagnostic only; no DB writes.

Output: /tmp/ocg_enum.jsonl (one JSON per file) + a printed summary.
"""
import sys, json, re
from pathlib import Path
sys.path.insert(0, str(Path("/Users/a/data-pipelines/pdcom-parser/src")))
import fitz
from pdcom_parser import ocg_layers as O

PDF_DIR = Path("/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese")

# Reference / non-category layer names (normalized substring match) -> excluded
# from "category geometry" but still reported. Mirrors taxonomy id 132 reference set.
REF_PAT = re.compile(r"(cadastr|b[aâ]ti|fond|pochage|limite|l[eé]gend|legende|"
                     r"rep[eè]re|cours d'eau|liaison|pi[eé]ton|voie verte|cyclab|"
                     r"navette|tc md|noeud|plan site communes|en t[eê]te|mobilit|"
                     r"masque|blanc|trame)", re.I)

def commune_of(fn: str) -> str:
    s = fn[len("pdcom_"):] if fn.startswith("pdcom_") else fn
    s = s.rsplit(".pdf", 1)[0]
    # strip trailing sheet/descriptor tokens
    s = re.split(r"_(?:\d|2e|0\d|carte|plan|synth|z5|sz5|strategie|rapport|compl|"
                 r"village|guide|zone|bel-air|concept|atlas|pdcp|plans|cp-|"
                 r"densification|classement|image)", s, 1, flags=re.I)[0]
    return s.strip("_-").replace("_", " ").title()

def enum_pdf(path: Path) -> dict:
    rec = {"file": path.name, "commune": commune_of(path.name), "error": None}
    try:
        doc = fitz.open(path)
        rec["n_pages"] = doc.page_count
        ocgs = doc.get_ocgs()  # {xref: {name,...}}
        rec["ocg_defined"] = sorted({v.get("name") for v in ocgs.values() if v.get("name")})
        rec["n_ocg_defined"] = len(rec["ocg_defined"])
        # recursive detection over all pages; aggregate painted-path counts per layer
        layer_counts = {}
        for pno in range(doc.page_count):
            page = doc[pno]
            res = O.extract_layer_polys(page, doc, skip_geometry=set())
            for nm, d in res.items():
                layer_counts[nm] = layer_counts.get(nm, 0) + len(d["polys"])
        rec["layer_paths"] = dict(sorted(layer_counts.items(), key=lambda kv: -kv[1]))
        cat = {nm: c for nm, c in layer_counts.items()
               if c > 0 and not REF_PAT.search(nm)}
        rec["category_layers"] = dict(sorted(cat.items(), key=lambda kv: -kv[1]))
        rec["n_category_layers"] = len(cat)
        rec["category_paths_total"] = sum(cat.values())
        # has_ocg_layers: OCGs defined AND at least one named layer carries painted geometry
        rec["has_ocg_layers"] = bool(layer_counts) and len(rec["ocg_defined"]) > 0
        rec["has_category_geometry"] = rec["n_category_layers"] > 0
        doc.close()
    except Exception as ex:
        rec["error"] = f"{type(ex).__name__}: {ex}"
    return rec

def main():
    pdfs = sorted(PDF_DIR.glob("pdcom_*.pdf"))
    out = open("/tmp/ocg_enum.jsonl", "w")
    print(f"{'FILE':<58} {'pg':>2} {'OCG':>4} {'catL':>4} {'catPaths':>8}  has_cat")
    print("-" * 95)
    for p in pdfs:
        r = enum_pdf(p)
        out.write(json.dumps(r, ensure_ascii=False) + "\n"); out.flush()
        if r["error"]:
            print(f"{r['file']:<58} ERR {r['error'][:40]}")
        else:
            print(f"{r['file']:<58} {r['n_pages']:>2} {r['n_ocg_defined']:>4} "
                  f"{r['n_category_layers']:>4} {r['category_paths_total']:>8}  "
                  f"{'Y' if r['has_category_geometry'] else 'n'}")
    out.close()
    print("\nwrote /tmp/ocg_enum.jsonl")

if __name__ == "__main__":
    main()
