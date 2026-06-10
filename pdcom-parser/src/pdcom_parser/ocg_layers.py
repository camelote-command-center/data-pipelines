"""OCG layer-aware vector extractor for PDCom Illustrator synthesis PDFs.

Root-cause fix (2026-06-05): prior runs classified by fill color via
page.get_drawings(), which (a) ignores OCG layer membership and (b) does not
recurse into Form XObjects. The PDCom category geometry is organized by LAYER
(Optional Content Group), and the actual polygons live inside Form XObjects
invoked with `Do` inside each layer's `/OC /MCx BDC ... EMC` marked-content block.

This module parses the page content stream sequentially, tracks the CTM stack
and the OCG marked-content stack, recurses into Form XObjects (applying their
Matrix), and attributes every filled path to its owning layer — in PDF user
space (EPSG-agnostic; georeferencing happens downstream).

Returns, per layer name: filled polygons + a fill-color histogram.
"""
from __future__ import annotations

import re
from collections import Counter

import fitz

_NUM = re.compile(rb'[+-]?(?:\d+\.?\d*|\.\d+)$')


def tokenize(data: bytes):
    i, n = 0, len(data)
    out = []
    while i < n:
        c = data[i:i+1]
        if c in b' \t\r\n\x00\x0c':
            i += 1; continue
        if c == b'%':
            j = data.find(b'\n', i); i = n if j < 0 else j + 1; continue
        if c == b'/':
            j = i + 1
            while j < n and data[j:j+1] not in b' \t\r\n\x00\x0c/<>[](){}%':
                j += 1
            out.append(('name', data[i+1:j].decode('latin-1'))); i = j; continue
        if c == b'(':
            depth, j, buf = 1, i + 1, bytearray()
            while j < n and depth > 0:
                ch = data[j]
                if ch == 0x5c:
                    if j + 1 < n: buf.append(data[j+1])
                    j += 2; continue
                if ch == 0x28: depth += 1
                elif ch == 0x29:
                    depth -= 1
                    if depth == 0: j += 1; break
                buf.append(ch); j += 1
            out.append(('str', bytes(buf))); i = j; continue
        if c == b'<':
            if data[i+1:i+2] == b'<':
                out.append(('op', '<<')); i += 2; continue
            j = data.find(b'>', i); out.append(('hex', data[i+1:j])); i = j + 1; continue
        if c == b'>':
            if data[i+1:i+2] == b'>':
                out.append(('op', '>>')); i += 2; continue
            i += 1; continue
        if c in b'[]':
            out.append(('op', c.decode())); i += 1; continue
        j = i
        while j < n and data[j:j+1] not in b' \t\r\n\x00\x0c/<>[](){}%':
            j += 1
        tok = data[i:j]
        if _NUM.match(tok):
            out.append(('num', float(tok)))
        else:
            out.append(('op', tok.decode('latin-1')))
        i = j
    return out


def matmul(m, n):
    a, b, c, d, e, f = m
    A, B, C, D, E, F = n
    return (a*A + b*C, a*B + b*D, c*A + d*C, c*B + d*D, e*A + f*C + E, e*B + f*D + F)


def apply(m, x, y):
    a, b, c, d, e, f = m
    return (a*x + c*y + e, b*x + d*y + f)


def _bez(p0, p1, p2, p3, seg=6):
    out = []
    for k in range(1, seg + 1):
        t = k / seg; u = 1 - t
        out.append((
            u*u*u*p0[0] + 3*u*u*t*p1[0] + 3*u*t*t*p2[0] + t*t*t*p3[0],
            u*u*u*p0[1] + 3*u*u*t*p1[1] + 3*u*t*t*p2[1] + t*t*t*p3[1],
        ))
    return out


def _to_hex(rgb):
    r, g, b = (max(0, min(255, int(round(c * 255)))) for c in rgb)
    return f"#{r:02x}{g:02x}{b:02x}"


def _parse_matrix(raw):
    if not raw or raw[0] != 'array':
        return (1, 0, 0, 1, 0, 0)
    nums = [float(x) for x in re.findall(r'[+-]?(?:\d+\.?\d*|\.\d+)', raw[1])]
    return tuple(nums[:6]) if len(nums) >= 6 else (1, 0, 0, 1, 0, 0)


def _deref(doc, raw):
    """Resolve a key value that may be INLINE (a dict/array string) or an
    INDIRECT reference ('N 0 R') to its object text. Fixes the 2026-06-06
    under-count: pages whose /Resources is an indirect object (e.g. dardagny
    '6 0 R') previously parsed to an empty Properties map, so every /OC /ocN
    BDC failed to resolve and all layer geometry fell through unattributed."""
    if not raw or raw[0] == 'null':
        return None
    if raw[0] == 'xref':
        m = re.match(r'(\d+)\s+0\s+R', raw[1].strip())
        if m:
            try:
                return doc.xref_object(int(m.group(1)), compressed=True)
            except Exception:
                return None
        return None
    return raw[1]


def _resources(doc, res_raw):
    """Parse a Resources dict → {'XObject': {name:xref}, 'Properties': {mc:xref}}.
    Handles indirect /Resources AND indirect /XObject and /Properties sub-dicts."""
    out = {"XObject": {}, "Properties": {}}
    s = _deref(doc, res_raw)
    if not s:
        return out
    for key in ("XObject", "Properties"):
        body = None
        m = re.search('/?' + key + r'\s*<<(.*?)>>', s, re.DOTALL)
        if m:
            body = m.group(1)
        else:
            m2 = re.search('/?' + key + r'\s+(\d+)\s+0\s+R', s)
            if m2:
                try:
                    body = doc.xref_object(int(m2.group(1)), compressed=True)
                except Exception:
                    body = None
        if body:
            for nm, xref in re.findall(r'/([^\s/<>]+)\s+(\d+)\s+0\s+R', body):
                out[key][nm] = int(xref)
    return out


# Layers we never need geometry from — still walked to keep the stack balanced,
# but their fills are not accumulated (perf: BÂTI HORS SOL alone is ~10k fills).
def extract_layer_polys(page, doc, skip_geometry: set[str] | None = None,
                        stroke_layers: set[str] | None = None,
                        capture_all: bool = False,
                        group_by_color: bool = False,
                        max_depth: int = 12):
    """Return {layer_name: {'polys': [[(x,y),...]], 'colors': Counter}} in PDF user space.

    `stroke_layers`: layer names whose STROKED paths (S/s ops) should also be
    captured as polylines — used for reference layers like LIMITES COMMUNALES /
    CADASTRE that are drawn as outlines, not fills (needed for georeferencing).
    """
    skip_geometry = skip_geometry or set()
    stroke_layers = stroke_layers or set()
    page_res = _resources(doc, doc.xref_get_key(page.xref, "Resources"))
    xref_to_name = {x: i.get('name') for x, i in doc.get_ocgs().items()}
    result: dict[str, dict] = {}
    form_cache: dict[int, tuple] = {}

    def active(mc_stack):
        for nm in reversed(mc_stack):
            if nm is not None:
                return nm
        return "__ALL__" if capture_all else None

    def run(data, ctm, mc_stack, resources, depth, visiting):
        toks = tokenize(data)
        ctm_stack = []
        operands = []
        cur = []
        subpaths = []
        start = None
        fill_rgb = None
        i, N = 0, len(toks)
        while i < N:
            t, v = toks[i]
            if t in ('num', 'name', 'str', 'hex'):
                operands.append((t, v)); i += 1; continue
            if t != 'op':
                i += 1; continue
            op = v
            if op == '[':
                j = i + 1
                while j < N and toks[j] != ('op', ']'):
                    j += 1
                i = j + 1; continue
            if op == '<<':
                j = i + 1; d = 1
                while j < N and d > 0:
                    if toks[j] == ('op', '<<'): d += 1
                    elif toks[j] == ('op', '>>'): d -= 1
                    j += 1
                i = j; operands.append(('dict', None)); continue
            nums = [x for (tt, x) in operands if tt == 'num']
            names = [x for (tt, x) in operands if tt == 'name']
            lay = active(mc_stack)
            want = lay is not None and lay not in skip_geometry

            if op == 'q':
                ctm_stack.append(ctm)
            elif op == 'Q':
                ctm = ctm_stack.pop() if ctm_stack else ctm
            elif op == 'cm' and len(nums) >= 6:
                ctm = matmul(tuple(nums[-6:]), ctm)
            elif op == 'm' and len(nums) >= 2:
                if cur: subpaths.append(cur)
                start = (nums[-2], nums[-1]); cur = [start]
            elif op == 'l' and len(nums) >= 2:
                cur.append((nums[-2], nums[-1]))
            elif op == 'c' and len(nums) >= 6:
                p0 = cur[-1] if cur else (nums[-6], nums[-5])
                cur.extend(_bez(p0, (nums[-6], nums[-5]), (nums[-4], nums[-3]), (nums[-2], nums[-1])))
            elif op == 'v' and len(nums) >= 4:
                p0 = cur[-1] if cur else (nums[-4], nums[-3])
                cur.extend(_bez(p0, p0, (nums[-4], nums[-3]), (nums[-2], nums[-1])))
            elif op == 'y' and len(nums) >= 4:
                p0 = cur[-1] if cur else (nums[-4], nums[-3])
                cur.extend(_bez(p0, (nums[-4], nums[-3]), (nums[-2], nums[-1]), (nums[-2], nums[-1])))
            elif op == 're' and len(nums) >= 4:
                x, y, w, h = nums[-4:]
                if cur: subpaths.append(cur); cur = []
                subpaths.append([(x, y), (x+w, y), (x+w, y+h), (x, y+h), (x, y)])
            elif op == 'h':
                if cur and start: cur.append(start)
            elif op == 'rg' and len(nums) >= 3:
                fill_rgb = tuple(nums[-3:])
            elif op == 'g' and len(nums) >= 1:
                fill_rgb = (nums[-1],) * 3
            elif op == 'k' and len(nums) >= 4:
                cc, mm, yy, kk = nums[-4:]
                fill_rgb = ((1-cc)*(1-kk), (1-mm)*(1-kk), (1-yy)*(1-kk))
            elif op in ('f', 'F', 'f*', 'b', 'b*', 'B', 'B*'):
                if cur: subpaths.append(cur); cur = []
                if group_by_color:
                    if fill_rgb is not None:
                        bucket = _to_hex(fill_rgb)
                        slot = result.setdefault(bucket, {"polys": [], "colors": Counter()})
                        for sp in subpaths:
                            if len(sp) >= 3:
                                slot["polys"].append([apply(ctm, px, py) for (px, py) in sp])
                        slot["colors"][bucket] += 1
                elif want:
                    slot = result.setdefault(lay, {"polys": [], "colors": Counter()})
                    for sp in subpaths:
                        if len(sp) >= 3:
                            slot["polys"].append([apply(ctm, px, py) for (px, py) in sp])
                    if fill_rgb is not None and subpaths:
                        slot["colors"][_to_hex(fill_rgb)] += 1
                subpaths = []; start = None
            elif op in ('S', 's'):
                if cur: subpaths.append(cur); cur = []
                if lay in stroke_layers:
                    slot = result.setdefault(lay, {"polys": [], "colors": Counter()})
                    for sp in subpaths:
                        if len(sp) >= 2:
                            slot["polys"].append([apply(ctm, px, py) for (px, py) in sp])
                subpaths = []; start = None
            elif op == 'n':
                cur = []; subpaths = []; start = None
            elif op == 'Do' and names:
                xname = names[-1]
                xref = resources["XObject"].get(xname) or page_res["XObject"].get(xname)
                if xref and depth < max_depth and xref not in visiting:
                    sub = doc.xref_get_key(xref, "Subtype")
                    if sub and sub[1] == '/Form':
                        if xref in form_cache:
                            sdata, smat, sres = form_cache[xref]
                        else:
                            sdata = doc.xref_stream(xref)
                            smat = _parse_matrix(doc.xref_get_key(xref, "Matrix"))
                            sres = _resources(doc, doc.xref_get_key(xref, "Resources"))
                            form_cache[xref] = (sdata, smat, sres)
                        if sdata:
                            run(sdata, matmul(smat, ctm), mc_stack,
                                sres, depth + 1, visiting | {xref})
            elif op == 'BDC':
                if names and names[0] == 'OC' and len(names) >= 2:
                    mc = names[1]
                    xref = resources["Properties"].get(mc) or page_res["Properties"].get(mc)
                    mc_stack.append(xref_to_name.get(xref))
                else:
                    mc_stack.append(None)
            elif op == 'BMC':
                mc_stack.append(None)
            elif op == 'EMC':
                if mc_stack: mc_stack.pop()
            operands = []; i += 1
        return ctm

    run(page.read_contents(), (1, 0, 0, 1, 0, 0), [], page_res, 0, frozenset())
    return result


def collect_painted_elements(page, doc, max_depth: int = 12):
    """Form-recursing collector of every painted element on the page, in page
    user-space. Returns list of dicts: {kind: 'fill'|'stroke', color: hex,
    bbox: (x0,y0,x1,y1), npts: int}. Ignores OCG layers (collects everything) —
    used for the legend reader, which must see swatches wherever they live
    (page stream OR nested Form XObjects)."""
    page_res = _resources(doc, doc.xref_get_key(page.xref, "Resources"))
    out = []
    form_cache = {}

    def run(data, ctm, resources, depth, visiting):
        toks = tokenize(data)
        ctm_stack = []
        operands = []
        cur = []
        subpaths = []
        start = None
        fill_rgb = None
        stroke_rgb = None
        i, N = 0, len(toks)
        while i < N:
            t, v = toks[i]
            if t in ('num', 'name', 'str', 'hex'):
                operands.append((t, v)); i += 1; continue
            if t != 'op':
                i += 1; continue
            op = v
            if op == '[':
                j = i + 1
                while j < N and toks[j] != ('op', ']'): j += 1
                i = j + 1; continue
            if op == '<<':
                j = i + 1; d = 1
                while j < N and d > 0:
                    if toks[j] == ('op', '<<'): d += 1
                    elif toks[j] == ('op', '>>'): d -= 1
                    j += 1
                i = j; operands.append(('dict', None)); continue
            nums = [x for (tt, x) in operands if tt == 'num']
            names = [x for (tt, x) in operands if tt == 'name']
            if op == 'q': ctm_stack.append(ctm)
            elif op == 'Q': ctm = ctm_stack.pop() if ctm_stack else ctm
            elif op == 'cm' and len(nums) >= 6: ctm = matmul(tuple(nums[-6:]), ctm)
            elif op == 'm' and len(nums) >= 2:
                if cur: subpaths.append(cur)
                start = (nums[-2], nums[-1]); cur = [start]
            elif op == 'l' and len(nums) >= 2: cur.append((nums[-2], nums[-1]))
            elif op == 'c' and len(nums) >= 6:
                p0 = cur[-1] if cur else (nums[-6], nums[-5])
                cur.extend(_bez(p0, (nums[-6], nums[-5]), (nums[-4], nums[-3]), (nums[-2], nums[-1])))
            elif op in ('v', 'y') and len(nums) >= 4:
                p0 = cur[-1] if cur else (nums[-4], nums[-3])
                cur.extend(_bez(p0, p0, (nums[-4], nums[-3]), (nums[-2], nums[-1])))
            elif op == 're' and len(nums) >= 4:
                x, y, w, h = nums[-4:]
                if cur: subpaths.append(cur); cur = []
                subpaths.append([(x, y), (x+w, y), (x+w, y+h), (x, y+h), (x, y)])
            elif op == 'h':
                if cur and start: cur.append(start)
            elif op == 'rg' and len(nums) >= 3: fill_rgb = tuple(nums[-3:])
            elif op == 'g' and len(nums) >= 1: fill_rgb = (nums[-1],)*3
            elif op == 'k' and len(nums) >= 4:
                cc, mm, yy, kk = nums[-4:]; fill_rgb = ((1-cc)*(1-kk), (1-mm)*(1-kk), (1-yy)*(1-kk))
            elif op == 'RG' and len(nums) >= 3: stroke_rgb = tuple(nums[-3:])
            elif op == 'G' and len(nums) >= 1: stroke_rgb = (nums[-1],)*3
            elif op == 'K' and len(nums) >= 4:
                cc, mm, yy, kk = nums[-4:]; stroke_rgb = ((1-cc)*(1-kk), (1-mm)*(1-kk), (1-yy)*(1-kk))
            elif op in ('f', 'F', 'f*', 'b', 'b*', 'B', 'B*', 'S', 's'):
                if cur: subpaths.append(cur); cur = []
                kind = 'stroke' if op in ('S', 's') else 'fill'
                rgb = stroke_rgb if kind == 'stroke' else fill_rgb
                for sp in subpaths:
                    if len(sp) < 2: continue
                    pts = [apply(ctm, px, py) for (px, py) in sp]
                    xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
                    out.append({"kind": kind, "color": _to_hex(rgb) if rgb else None,
                                "bbox": (min(xs), min(ys), max(xs), max(ys)), "npts": len(pts)})
                subpaths = []; start = None
            elif op == 'n': cur = []; subpaths = []; start = None
            elif op == 'Do' and names:
                xref = resources["XObject"].get(names[-1]) or page_res["XObject"].get(names[-1])
                if xref and depth < max_depth and xref not in visiting:
                    sub = doc.xref_get_key(xref, "Subtype")
                    if sub and sub[1] == '/Form':
                        if xref in form_cache: sdata, smat, sres = form_cache[xref]
                        else:
                            sdata = doc.xref_stream(xref); smat = _parse_matrix(doc.xref_get_key(xref, "Matrix"))
                            sres = _resources(doc, doc.xref_get_key(xref, "Resources")); form_cache[xref] = (sdata, smat, sres)
                        if sdata: run(sdata, matmul(smat, ctm), sres, depth+1, visiting | {xref})
            operands = []; i += 1
    run(page.read_contents(), (1, 0, 0, 1, 0, 0), page_res, 0, frozenset())
    return out


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else \
        "/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese/pdcom_cologny_2e_synthese.pdf"
    doc = fitz.open(path)
    page = doc[0]
    res = extract_layer_polys(page, doc, skip_geometry={"BÂTI HORS SOL", "Pochage", "FOND NOIR ET BLANC"})
    print(f"layers with fills: {len(res)}")
    for nm in sorted(res, key=lambda k: -len(res[k]["polys"])):
        top = res[nm]["colors"].most_common(2)
        print(f"  {len(res[nm]['polys']):>5} polys  colors={top}  {nm!r}")
