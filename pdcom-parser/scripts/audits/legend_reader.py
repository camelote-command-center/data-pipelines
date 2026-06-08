"""Hardened legend reader: pairs form-recursed swatches (fill/stroke) to nearby
text labels. Works regardless of where the legend lives (page or Form XObject)."""
import sys, fitz, re, unicodedata
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from pdcom_parser import ocg_layers as O

def _text_lines(page):
    out=[]
    for b in page.get_text("dict").get("blocks",[]):
        for ln in b.get("lines",[]):
            bb=ln.get("bbox"); txt=" ".join(s.get("text","") for s in ln.get("spans",[])).strip()
            if txt and bb: out.append({"bbox":fitz.Rect(bb),"text":txt})
    return out

_NOISE=re.compile(r'(@|www\.|\.ch\b|t\s*0\d{2}|f\s*0\d{2}|\d{2}\.\d{2}\.\d{4}|sia\b|fsu\b|approuv|adopt|architect|avenue|\bgen[eè]ve\b|conseil)',re.I)
def _is_noise_label(lab):
    s=lab.strip()
    if len(s)<3: return True
    if re.fullmatch(r'[\d\s.,/\-]+',s): return True              # pure numeric / codes
    letters=[c for c in s if c.isalpha()]
    if letters and all(c.isupper() for c in letters): return True  # all-caps = title/place/commune
    if _NOISE.search(s): return True
    return False

def read_legend(page, doc):
    els=O.collect_painted_elements(page, doc)
    sw=[]
    for e in els:
        x0,y0,x1,y1=e["bbox"]; w=x1-x0; h=y1-y0
        if 2<=w<=34 and 2<=h<=34 and max(w,h)/max(min(w,h),0.1)<6 and e["color"]:
            sw.append(e)
    # legend-region isolation: keep swatches in the densest x-columns (legend is stacked)
    from collections import Counter
    if len(sw)>=6:
        xb=Counter(round(((s["bbox"][0]+s["bbox"][2])/2)/15)*15 for s in sw)
        keep={x for x,c in xb.items() if c>=3}
        if keep: sw=[s for s in sw if round(((s["bbox"][0]+s["bbox"][2])/2)/15)*15 in keep]
    lines=_text_lines(page)
    # color usage tally across ALL fills (feature_count per color)
    from collections import Counter
    usage=Counter(e["color"] for e in els if e["kind"]=="fill" and e["color"])
    entries=[]; used_text=set()
    for s in sw:
        sx0,sy0,sx1,sy1=s["bbox"]; scy=(sy0+sy1)/2
        best=None;bd=1e9
        for idx,ln in enumerate(lines):
            tb=ln["bbox"]
            # label to the right (typical) or left, same y-band
            if tb.x0>=sx1-2: dx=tb.x0-sx1; side='r'
            elif tb.x1<=sx0+2: dx=sx0-tb.x1; side='l'
            else: continue
            if dx>70: continue
            dy=abs((tb.y0+tb.y1)/2-scy)
            if dy>max(8,(sy1-sy0)/2+5): continue
            sc=dx+dy*2+(0 if side=='r' else 6)
            if sc<bd: bd=sc; best=(idx,ln)
        if best is None: continue
        idx,ln=best
        lab=re.sub(r'^[\s\-/–—>·•]+','',ln["text"]).strip()
        if _is_noise_label(lab): continue
        entries.append({"label":lab,"color":s["color"],"kind":s["kind"],"feature_count":usage.get(s["color"],0)})
    # dedup by (color,label)
    seen=set();uniq=[]
    for e in entries:
        k=(e["color"],e["label"].lower())
        if k in seen: continue
        seen.add(k); uniq.append(e)
    return uniq

if __name__=="__main__":
    D=Path("/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese")
    tests=["pdcom_perly-certoux_carte-de-synthese-mai-2018.pdf","pdcom_collonge-bellerive_synthese.pdf",
           "pdcom_puplinge_plan-synthese-final-v2.pdf","pdcom_Veyrier_2e_Z5.pdf","pdcom_thonex.pdf"]
    for pdf in tests:
        p=D/pdf
        if not p.exists(): print(pdf,"MISSING"); continue
        try:
            doc=fitz.open(p); ent=read_legend(doc[0],doc)
            print(f"\n=== {pdf}: {len(ent)} legend entries ===")
            for e in ent[:14]: print(f"   {e['color']} {e['kind']:<6} fc={e['feature_count']:<4} {e['label'][:55]!r}")
        except Exception as ex: print(pdf,"ERR",ex)
