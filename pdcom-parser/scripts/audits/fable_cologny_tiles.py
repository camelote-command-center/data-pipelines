"""Fable vision benchmark — tile generator (DIAGNOSTIC ONLY, no DB writes).

Renders Cologny's OCG vector PDF as overlapping high-zoom tiles (crisp vector
render via fitz clip), overlays OUR mv_plots parcel outlines + per-tile numeric
labels using the building-xcorr affine (99.7% match), and writes:
  /tmp/fable_tiles/tile_<r>_<c>.png
  /tmp/fable_tiles/index.json   {tile: {label: egrid}}
  /tmp/fable_tiles/overview.png (low-dpi full page, to locate the legend)
Parcels are labeled in the tile whose CORE (non-overlap) region contains their
centroid; overlap margins give context without duplicate labeling.
"""
import sys, json
from pathlib import Path
import fitz, psycopg
from shapely import wkt as W
from PIL import Image, ImageDraw, ImageFont
sys.path.insert(0, str(Path("/Users/a/data-pipelines/pdcom-parser/src")))
sys.path.insert(0, str(Path("/Users/a/data-pipelines/pdcom-parser/scripts")))
import cologny_ocg_load as C

OUT = Path("/tmp/fable_tiles"); OUT.mkdir(exist_ok=True)
PDF = C.PDF
ZOOM_PXPM = 3.2          # px per metre in tiles
TILE_M = 450             # tile core size (m)
OVER_M = 60              # overlap margin (m)

def main():
    doc = fitz.open(PDF); page = doc[0]
    page_h = page.rect.height
    params, s, corr, match = C.georeference(page, doc)
    a, b, d, e, xoff, yoff = params
    print(f"georef scale={s:.3f} m/pt match={match:.1f}%")

    with psycopg.connect(C.LAMAP) as cn, cn.cursor() as cur:
        cur.execute("""SELECT egrid, ST_AsText(ST_Transform(geometry,2056))
                       FROM ref.plots WHERE commune_name='Cologny' AND geometry IS NOT NULL""")
        parcels = [(eg, W.loads(w)) for eg, w in cur.fetchall()]
    print(f"parcels: {len(parcels)}")

    # world bbox of all parcels
    xs0=[]; ys0=[]; xs1=[]; ys1=[]
    for _, g in parcels:
        x0,y0,x1,y1 = g.bounds; xs0.append(x0); ys0.append(y0); xs1.append(x1); ys1.append(y1)
    WX0, WY0, WX1, WY1 = min(xs0), min(ys0), max(xs1), max(ys1)
    print(f"world bbox: {WX0:.0f},{WY0:.0f} -> {WX1:.0f},{WY1:.0f}  ({WX1-WX0:.0f}x{WY1-WY0:.0f} m)")

    def w2pt(X, Y):           # world -> PDF pt (y-up)
        return (X-xoff)/a, (Y-yoff)/e

    # page-space zoom: px per pt = ZOOM_PXPM * s  (s = m/pt)
    zpt = ZOOM_PXPM * s
    ncols = int((WX1-WX0)//TILE_M)+1; nrows = int((WY1-WY0)//TILE_M)+1
    print(f"grid {nrows} rows x {ncols} cols = {nrows*ncols} tiles @ {int(TILE_M+2*OVER_M)}m -> {int((TILE_M+2*OVER_M)*ZOOM_PXPM)}px")

    try:
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 26)
        font_s = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 20)
    except Exception:
        font = font_s = ImageFont.load_default()

    index = {}
    for r in range(nrows):
        for c in range(ncols):
            cx0 = WX0 + c*TILE_M; cy1 = WY1 - r*TILE_M   # core top-left (world)
            cx1 = min(cx0+TILE_M, WX1); cy0 = max(cy1-TILE_M, WY0)
            # which parcels' centroids fall in CORE?
            mine = [(eg,g) for eg,g in parcels
                    if cx0 <= g.centroid.x < cx1 and cy0 <= g.centroid.y < cy1]
            if not mine: continue
            ex0, ey0, ex1, ey1 = cx0-OVER_M, cy0-OVER_M, cx1+OVER_M, cy1+OVER_M
            # world->pt corners; pt y-up -> fitz clip y-down
            px0, py0 = w2pt(ex0, ey0); px1, py1 = w2pt(ex1, ey1)
            clip = fitz.Rect(min(px0,px1), page_h-max(py0,py1), max(px0,px1), page_h-min(py0,py1))
            pix = page.get_pixmap(matrix=fitz.Matrix(zpt, zpt), clip=clip, colorspace=fitz.csRGB, alpha=False)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            dr = ImageDraw.Draw(img)

            def w2px(X, Y):   # world -> tile pixel
                xpt, ypt = w2pt(X, Y)
                return (xpt-clip.x0)*zpt, ((page_h-ypt)-clip.y0)*zpt

            tile_id = f"t{r}_{c}"
            lmap = {}
            # draw ALL parcels intersecting the extended tile as outlines (context),
            # but number only the core ones
            for eg, g in parcels:
                gx0,gy0,gx1,gy1 = g.bounds
                if gx1<ex0 or gx0>ex1 or gy1<ey0 or gy0>ey1: continue
                geoms = g.geoms if g.geom_type.startswith("Multi") else [g]
                for gg in geoms:
                    pts = [w2px(X,Y) for X,Y in zip(*gg.exterior.xy)]
                    if len(pts) >= 3:
                        dr.line(pts+[pts[0]], fill=(255,0,0), width=2)
            for i,(eg,g) in enumerate(sorted(mine, key=lambda t: (-t[1].centroid.y, t[1].centroid.x)), 1):
                lmap[str(i)] = eg
                cxp, cyp = w2px(g.centroid.x, g.centroid.y)
                lbl = str(i)
                f = font if g.area > 1500 else font_s
                bb = dr.textbbox((0,0), lbl, font=f)
                wpx, hpx = bb[2]-bb[0], bb[3]-bb[1]
                dr.rectangle([cxp-wpx/2-3, cyp-hpx/2-3, cxp+wpx/2+3, cyp+hpx/2+3],
                             fill=(255,255,255), outline=(255,0,0))
                dr.text((cxp-wpx/2, cyp-hpx/2-bb[1]), lbl, fill=(200,0,0), font=f)
            img.save(OUT/f"{tile_id}.png")
            index[tile_id] = lmap
    n_lab = sum(len(v) for v in index.values())
    print(f"tiles written: {len(index)}, parcels labeled: {n_lab}")
    (OUT/"index.json").write_text(json.dumps(index))

    # overview for legend localisation
    ov = page.get_pixmap(matrix=fitz.Matrix(0.55,0.55), colorspace=fitz.csRGB, alpha=False)
    Image.frombytes("RGB",(ov.width,ov.height),ov.samples).save(OUT/"overview.png")
    print("overview.png written", ov.width, ov.height)

if __name__ == "__main__":
    main()
