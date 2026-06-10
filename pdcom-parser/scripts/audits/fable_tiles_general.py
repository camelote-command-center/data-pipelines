"""Generalized Fable tile generator (Part 2). DIAGNOSTIC ONLY, no DB writes.
Usage: .venv/bin/python fable_tiles_general.py <commune> <pdf_path> <georef_json> <outdir>
Renders overlapping tiles from the map PDF, overlays ref.plots parcels (red
outlines + per-tile numeric labels), writes index.json mapping labels->egrid.
"""
import sys, json
from pathlib import Path
import fitz, psycopg
from shapely import wkt as W
from PIL import Image, ImageDraw, ImageFont

LAMAP="postgresql://postgres.fckdwddgtdbvhzloejni:SUpolkmn098%24@aws-1-eu-central-2.pooler.supabase.com:5432/postgres"
ZOOM_PXPM=3.2; TILE_M=450; OVER_M=60

def main(commune, pdf, georef_json, outdir):
    OUT=Path(outdir); OUT.mkdir(exist_ok=True)
    g=json.load(open(georef_json))
    a,_,_,e,xoff,yoff=g["params"]
    doc=fitz.open(pdf); page=doc[0]; page_h=page.rect.height; page_w=page.rect.width
    with psycopg.connect(LAMAP) as cn:
        cur=cn.cursor()
        cur.execute("""SELECT egrid, ST_AsText(ST_Transform(geometry,2056)) FROM ref.plots
                       WHERE commune_name=%s AND geometry IS NOT NULL""",(commune,))
        parcels=[(eg,W.loads(w)) for eg,w in cur.fetchall()]
    print(f"{commune}: {len(parcels)} parcels; scale={a:.3f} m/pt")
    xs0=[];ys0=[];xs1=[];ys1=[]
    for _,gm in parcels:
        x0,y0,x1,y1=gm.bounds; xs0.append(x0);ys0.append(y0);xs1.append(x1);ys1.append(y1)
    WX0,WY0,WX1,WY1=min(xs0),min(ys0),max(xs1),max(ys1)
    def w2pt(X,Y): return (X-xoff)/a,(Y-yoff)/e
    zpt=ZOOM_PXPM*a
    ncols=int((WX1-WX0)//TILE_M)+1; nrows=int((WY1-WY0)//TILE_M)+1
    try:
        font=ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc",26)
        font_s=ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc",20)
    except Exception:
        font=font_s=ImageFont.load_default()
    index={}
    for r in range(nrows):
        for c in range(ncols):
            cx0=WX0+c*TILE_M; cy1=WY1-r*TILE_M
            cx1=min(cx0+TILE_M,WX1); cy0=max(cy1-TILE_M,WY0)
            mine=[(eg,gm) for eg,gm in parcels if cx0<=gm.centroid.x<cx1 and cy0<=gm.centroid.y<cy1]
            if not mine: continue
            ex0,ey0,ex1,ey1=cx0-OVER_M,cy0-OVER_M,cx1+OVER_M,cy1+OVER_M
            px0,py0=w2pt(ex0,ey0); px1,py1=w2pt(ex1,ey1)
            clip=fitz.Rect(max(0,min(px0,px1)),max(0,page_h-max(py0,py1)),
                           min(page_w,max(px0,px1)),min(page_h,page_h-min(py0,py1)))
            if clip.width<10 or clip.height<10: continue
            pix=page.get_pixmap(matrix=fitz.Matrix(zpt,zpt),clip=clip,colorspace=fitz.csRGB,alpha=False)
            img=Image.frombytes("RGB",(pix.width,pix.height),pix.samples)
            dr=ImageDraw.Draw(img)
            def w2px(X,Y):
                xpt,ypt=w2pt(X,Y)
                return (xpt-clip.x0)*zpt,((page_h-ypt)-clip.y0)*zpt
            tile_id=f"t{r}_{c}"; lmap={}
            for eg,gm in parcels:
                gx0,gy0,gx1,gy1=gm.bounds
                if gx1<ex0 or gx0>ex1 or gy1<ey0 or gy0>ey1: continue
                geoms=gm.geoms if gm.geom_type.startswith("Multi") else [gm]
                for gg in geoms:
                    pts=[w2px(X,Y) for X,Y in zip(*gg.exterior.xy)]
                    if len(pts)>=3: dr.line(pts+[pts[0]],fill=(255,0,0),width=2)
            for i,(eg,gm) in enumerate(sorted(mine,key=lambda t:(-t[1].centroid.y,t[1].centroid.x)),1):
                lmap[str(i)]=eg
                cxp,cyp=w2px(gm.centroid.x,gm.centroid.y)
                lbl=str(i); f=font if gm.area>1500 else font_s
                bb=dr.textbbox((0,0),lbl,font=f); wpx,hpx=bb[2]-bb[0],bb[3]-bb[1]
                dr.rectangle([cxp-wpx/2-3,cyp-hpx/2-3,cxp+wpx/2+3,cyp+hpx/2+3],fill=(255,255,255),outline=(255,0,0))
                dr.text((cxp-wpx/2,cyp-hpx/2-bb[1]),lbl,fill=(200,0,0),font=f)
            img.save(OUT/f"{tile_id}.png"); index[tile_id]=lmap
    (OUT/"index.json").write_text(json.dumps(index))
    print(f"tiles={len(index)} labeled={sum(len(v) for v in index.values())}")

if __name__=="__main__":
    main(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])
