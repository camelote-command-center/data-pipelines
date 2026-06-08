"""Extractor-2 DRY-RUN on 3 communes via crosswalk color->key + building-xcorr georef. No writes."""
import sys, json, fitz, numpy as np, psycopg, re, unicodedata
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from pdcom_parser import ocg_layers as O
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union
from shapely import wkt as W
from shapely.affinity import affine_transform
LAMAP="postgresql://postgres.fckdwddgtdbvhzloejni:SUpolkmn098%24@aws-1-eu-central-2.pooler.supabase.com:5432/postgres"
D=Path("/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese")
CW=json.load(open(Path(__file__).resolve().parent/"crosswalk.json"))["crosswalk"]
# commune -> {color_lower: (key,label)}
cmap=defaultdict(dict)
for r in CW: cmap[r["commune"]][r["color"].lower()]=(r["category_key"],r["label"])
TARGETS=[("Le Grand-Saconnex","pdcom_grand-saconnex_2e_synthese.pdf"),
         ("Satigny","pdcom_satigny_rapport.pdf"),
         ("Vandoeuvres","pdcom_vandoeuvres.pdf")]
def georef(page,doc,name):
    res=O.extract_layer_polys(page,doc,capture_all=True)
    polys=res.get("__ALL__",{}).get("polys",[]); cent=[]
    for poly in polys:
        xs=[p[0] for p in poly];ys=[p[1] for p in poly];w=max(xs)-min(xs);h=max(ys)-min(ys)
        if 1<w<40 and 1<h<40: cent.append((sum(xs)/len(xs),sum(ys)/len(ys)))
    if len(cent)<200: return None,None,0,None
    P=np.array(cent)
    with psycopg.connect(LAMAP) as c, c.cursor() as cur:
        cur.execute("SELECT ST_XMin(b),ST_YMin(b),ST_XMax(b),ST_YMax(b) FROM (SELECT ST_Extent(ST_Transform(geometry,2056)) b FROM ref.communes WHERE commune_name=%s) t",(name,))
        x0,y0,x1,y1=cur.fetchone();cx,cy=(x0+x1)/2,(y0+y1)/2
        cur.execute("SELECT ST_X(ST_Centroid(geometry)),ST_Y(ST_Centroid(geometry)) FROM ref.buildings_geo WHERE ST_X(ST_Centroid(geometry)) BETWEEN %s AND %s AND ST_Y(ST_Centroid(geometry)) BETWEEN %s AND %s",(cx-6000,cx+6000,cy-6000,cy+6000))
        Q=np.array(cur.fetchall(),float)
        cur.execute("SELECT ST_AsText(ST_Transform(geometry,2056)) FROM ref.communes WHERE commune_name=%s",(name,)); commune=W.loads(cur.fetchone()[0])
    CELL=20.0;qx0,qy0=Q[:,0].min()-500,Q[:,1].min()-500;qx1,qy1=Q[:,0].max()+500,Q[:,1].max()+500
    NX=int((qx1-qx0)/CELL)+1;NY=int((qy1-qy0)/CELL)+1
    def rast(p,x0,y0,nx,ny):
        g=np.zeros((ny,nx),np.float32);ix=((p[:,0]-x0)/CELL).astype(int);iy=((p[:,1]-y0)/CELL).astype(int)
        m=(ix>=0)&(ix<nx)&(iy>=0)&(iy<ny);np.add.at(g,(iy[m],ix[m]),1.0);return g
    Qf=np.fft.rfft2(rast(Q,qx0,qy0,NX,NY));best=None
    for s in np.arange(3.30,3.75,0.02):
        Pf=P.copy();Pf[:,0]-=Pf[:,0].min();Pf[:,1]-=Pf[:,1].min();Ps=Pf*s
        if Ps[:,0].max()>(qx1-qx0) or Ps[:,1].max()>(qy1-qy0):continue
        corr=np.fft.irfft2(Qf*np.conj(np.fft.rfft2(rast(Ps,0,0,NX,NY))),s=(NY,NX))
        pk=np.unravel_index(np.argmax(corr),corr.shape);v=corr[pk]
        if best is None or v>best[0]:best=(v,s,pk)
    v,s,(dy,dx)=best;minx,miny=P[:,0].min(),P[:,1].min()
    params=[s,0,0,s,qx0+dx*CELL-s*minx,qy0+dy*CELL-s*miny]
    T=np.column_stack([P[:,0]*s+params[4],P[:,1]*s+params[5]]);rs=set((int(x//25),int(y//25)) for x,y in Q)
    hit=sum(any((int(x//25)+a,int(y//25)+b) in rs for a in(-1,0,1) for b in(-1,0,1)) for x,y in T)
    return params,commune,100*hit/len(T),s
for name,pdf in TARGETS:
    p=D/pdf
    if not p.exists(): print(f"{name}: MISSING {pdf}"); continue
    doc=fitz.open(p);page=doc[0]
    params,commune,match,s=georef(page,doc,name)
    print(f"\n=== {name} ({pdf}) georef scale={s} match={match:.1f}% ===" if params else f"\n=== {name}: georef FAILED ===")
    if not params: continue
    bycol=O.extract_layer_polys(page,doc,group_by_color=True)
    perkey=defaultdict(list);keylabel={}
    for hexc,(key,label) in cmap[name].items():
        polys=bycol.get(hexc,{}).get("polys",[])
        for poly in polys:
            if len(poly)>=3:
                g=affine_transform(Polygon(poly),params).buffer(0)
                if not g.is_empty: perkey[key].append(g); keylabel[key]=label
    if not perkey: print("   (no crosswalk colors produced geometry)"); continue
    for key in sorted(perkey,key=lambda k:-sum(g.area for g in perkey[k])):
        pre=unary_union(perkey[key]);clip=pre.intersection(commune)
        a=clip.area; inpct=100*a/max(pre.area,1)
        print(f"   {key:<26} feats={len(perkey[key]):<4} area={a:>10.0f} m2  in-commune={inpct:>3.0f}%  label={keylabel[key][:40]!r}")
