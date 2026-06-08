"""REOPEN audit — per-file PROOF pass (2026-06-06). For EVERY GE synthesis PDF,
prove the layer-tagging status from first principles (not the old audit):
  n_ocg        = doc.get_ocgs()  (catalog /OCProperties/OCGs; layer tagging POSSIBLE?)
  oc_markers   = count of '/OC' BDC in all content streams (layer tagging USED?)
  vec_drawings = page0 vector path count (get_drawings)
  images       = page0 image count
Classification:
  layer_tagged   : n_ocg>0 and oc_markers>0
  flat_vector    : n_ocg==0, vec_drawings large, few images
  raster_scan    : images dominate, vec_drawings small
A file cannot have /OC tagging without n_ocg>0 (PDF spec: OCGs declared in catalog),
so n_ocg==0 + oc_markers==0 PROVES no layer tagging. Diagnostic only."""
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path("/Users/a/data-pipelines/pdcom-parser/src")))
import fitz

DIR = Path("/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese")

def classify(path):
    r={"file":path.name}
    try:
        doc=fitz.open(path)
        r["n_ocg"]=len({v.get("name") for v in doc.get_ocgs().values() if v.get("name")})
        ocm=0
        for pno in range(doc.page_count):
            try: ocm += doc[pno].read_contents().count(b"/OC ")
            except Exception: pass
        r["oc_markers"]=ocm
        p0=doc[0]
        try: r["vec"]=len(p0.get_drawings())
        except Exception: r["vec"]=-1
        r["img"]=len(p0.get_images(full=True))
        doc.close()
        if r["n_ocg"]>0 and ocm>0: r["cls"]="layer_tagged"
        elif r["img"]>=1 and r["vec"]<200: r["cls"]="raster_scan"
        elif r["vec"]>=200: r["cls"]="flat_vector"
        else: r["cls"]="sparse/other"
    except Exception as ex:
        r["err"]=f"{type(ex).__name__}: {ex}"
    return r

rows=[classify(p) for p in sorted(DIR.glob("pdcom_*.pdf"))]
open("/tmp/ocg_proof.jsonl","w").write("\n".join(json.dumps(x,ensure_ascii=False) for x in rows))
from collections import Counter
c=Counter(x.get("cls","ERR") for x in rows)
print("CLASSIFICATION TOTALS:", dict(c))
print(f"\n{'FILE':<58}{'nOCG':>5}{'/OC':>6}{'vec':>8}{'img':>5}  class")
for x in rows:
    if "err" in x: print(f"{x['file']:<58} ERR {x['err'][:30]}"); continue
    print(f"{x['file']:<58}{x['n_ocg']:>5}{x['oc_markers']:>6}{x['vec']:>8}{x['img']:>5}  {x['cls']}")
# prove: any n_ocg==0 file with oc_markers>0 ? (would be a contradiction / missed tagging)
bad=[x for x in rows if x.get("n_ocg")==0 and x.get("oc_markers",0)>0]
print("\nPROOF CHECK — files with 0 catalog OCGs but >0 /OC markers (should be NONE):", [x["file"] for x in bad] or "NONE")
