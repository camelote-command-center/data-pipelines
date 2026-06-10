"""Generalized building-xcorr georef for non-OCG PDCom maps (DIAGNOSTIC ONLY).

Corsier (vector): building candidates = small dark-grey FILLS from the
form-recursing painted-element collector.
Bardonnex (scan): building candidates = dark pixels of the page raster
(point cloud taken directly from thresholded pixels; xcorr is robust to the
extra line/text pixels as long as building mass dominates locally).

Outputs per commune: scale (m/pt or m/px), affine params, building match %.
Gate: match>=90 to proceed (same as Cologny). Saves /tmp/georef_<c>.json.
"""
import sys, json
from pathlib import Path
import fitz, numpy as np, psycopg
sys.path.insert(0, str(Path("/Users/a/data-pipelines/pdcom-parser/src")))
from pdcom_parser import ocg_layers as O

LAMAP="postgresql://postgres.fckdwddgtdbvhzloejni:SUpolkmn098%24@aws-1-eu-central-2.pooler.supabase.com:5432/postgres"
DIR=Path("/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese")

def _rast(p,x0,y0,nx,ny,cell):
    g=np.zeros((ny,nx),np.float32)
    ix=((p[:,0]-x0)/cell).astype(int); iy=((p[:,1]-y0)/cell).astype(int)
    m=(ix>=0)&(ix<nx)&(iy>=0)&(iy<ny)
    np.add.at(g,(iy[m],ix[m]),1.0); return g

def xcorr(P, Q, smin, smax, step=0.01, cell=20.0):
    qx0,qy0=Q[:,0].min()-500,Q[:,1].min()-500
    qx1,qy1=Q[:,0].max()+500,Q[:,1].max()+500
    NX=int((qx1-qx0)/cell)+1; NY=int((qy1-qy0)/cell)+1
    Qf=np.fft.rfft2(_rast(Q,qx0,qy0,NX,NY,cell))
    best=None
    for s in np.arange(smin,smax,step):
        Pf=P.astype(float).copy(); Pf[:,0]-=Pf[:,0].min(); Pf[:,1]-=Pf[:,1].min()
        Ps=Pf*s
        if Ps[:,0].max()>(qx1-qx0) or Ps[:,1].max()>(qy1-qy0): continue
        corr=np.fft.irfft2(Qf*np.conj(np.fft.rfft2(_rast(Ps,0,0,NX,NY,cell))),s=(NY,NX))
        pk=np.unravel_index(np.argmax(corr),corr.shape); v=corr[pk]
        if best is None or v>best[0]: best=(v,s,pk)
    v,s,(dy,dx)=best
    minx,miny=P[:,0].min(),P[:,1].min()
    params=[s,0,0,s,qx0+dx*cell-s*minx,qy0+dy*cell-s*miny]
    T=np.column_stack([P[:,0]*s+params[4],P[:,1]*s+params[5]])
    rs=set((int(x//25),int(y//25)) for x,y in Q)
    hit=sum(any((int(x//25)+a,int(y//25)+b) in rs for a in(-1,0,1) for b in(-1,0,1)) for x,y in T)
    return params,s,v,100*hit/len(T)

def buildings_db(commune):
    with psycopg.connect(LAMAP) as cn:
        cur=cn.cursor()
        cur.execute("""SELECT ST_X(ST_Centroid(b.geometry)), ST_Y(ST_Centroid(b.geometry))
                       FROM ref.buildings_geo b
                       JOIN ref.communes c ON c.commune_name=%s
                       WHERE ST_Intersects(b.geometry, ST_Expand(ST_Transform(c.geometry,2056), 600))""",(commune,))
        return np.array(cur.fetchall(),float)

def corsier():
    doc=fitz.open(DIR/"pdcom_corsier_2e_synthese.pdf"); page=doc[0]
    els=O.collect_painted_elements(page,doc)
    pts=[]
    for e in els:
        if e["kind"]!="fill" or not e["color"]: continue
        x0,y0,x1,y1=e["bbox"]; w,h=x1-x0,y1-y0
        if not (1.2<=w<=28 and 1.2<=h<=28): continue
        r,g,b=int(e["color"][1:3],16),int(e["color"][3:5],16),int(e["color"][5:7],16)
        if abs(r-g)<28 and abs(g-b)<28 and 40<=r<=150:   # dark grey
            pts.append(((x0+x1)/2,(y0+y1)/2))
    P=np.array(pts)
    print(f"[corsier] building-like fills: {len(P)}")
    Q=buildings_db("Corsier")
    print(f"[corsier] DB buildings: {len(Q)}")
    params,s,v,match=xcorr(P,Q,0.8,3.2)
    print(f"[corsier] scale={s:.3f} m/pt corr={v:.0f} match={match:.1f}%")
    json.dump({"space":"pdf_pt","params":params,"scale":s,"match":match},open('/tmp/georef_corsier.json','w'))

def bardonnex():
    doc=fitz.open(DIR/"pdcom_bardonnex_synthese.pdf"); page=doc[0]
    Z=1.0   # work at native pt resolution => "px" = pt
    pix=page.get_pixmap(matrix=fitz.Matrix(Z,Z),colorspace=fitz.csRGB,alpha=False)
    img=np.frombuffer(pix.samples,dtype=np.uint8).reshape(pix.height,pix.width,3).astype(int)
    r,g,b=img[...,0],img[...,1],img[...,2]
    grey=(abs(r-g)<26)&(abs(g-b)<26)
    dark=grey&(r>=40)&(r<=130)
    ys,xs=np.nonzero(dark)
    # exclude the legend margin (right 14% of sheet) to reduce noise
    keep=xs < pix.width*0.86
    xs,ys=xs[keep],ys[keep]
    # subsample to ~12k points for speed
    if len(xs)>12000:
        sel=np.random.RandomState(42).choice(len(xs),12000,replace=False)
        xs,ys=xs[sel],ys[sel]
    P=np.column_stack([xs.astype(float), (pix.height-ys).astype(float)])  # y-up
    print(f"[bardonnex] dark building-ish pixels used: {len(P)}")
    Q=buildings_db("Bardonnex")
    print(f"[bardonnex] DB buildings: {len(Q)}")
    params,s,v,match=xcorr(P,Q,0.8,3.0)
    print(f"[bardonnex] scale={s:.3f} m/px corr={v:.0f} match={match:.1f}%")
    json.dump({"space":"pdf_pt","params":params,"scale":s,"match":match},open('/tmp/georef_bardonnex.json','w'))

if __name__=="__main__":
    corsier(); bardonnex()
