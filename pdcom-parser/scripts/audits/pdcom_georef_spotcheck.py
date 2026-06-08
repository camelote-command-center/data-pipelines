import sys, fitz, numpy as np, psycopg
sys.path.insert(0,'src')
from pdcom_parser import ocg_layers as O
D="/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese/"
LAMAP="postgresql://postgres.fckdwddgtdbvhzloejni:SUpolkmn098%24@aws-1-eu-central-2.pooler.supabase.com:5432/postgres"
# commune, pdf, archetype
SPOT=[("Le Grand-Saconnex","pdcom_grand-saconnex_2e_synthese.pdf","vector-forms"),
      ("Perly-Certoux","pdcom_perly-certoux_carte-de-synthese-mai-2018.pdf","vector-flat-dense"),
      ("Collex-Bossy","pdcom_Collex-Bossy_060.pdf","vector-flat-sparse"),
      ("Satigny","pdcom_satigny_rapport.pdf","vector-flat-dense"),
      ("Meyrin","pdcom_meyrin.pdf","raster-scan")]
def is_grey(rgb):
    if rgb is None: return False
    r,g,b=rgb[:3]; return abs(r-g)<0.06 and abs(g-b)<0.06 and 0.4<((r+g+b)/3)<0.92
for nm,pdf,arch in SPOT:
    try:
        doc=fitz.open(D+pdf); page=doc[0]
        res=O.extract_layer_polys(page, doc, capture_all=True)
        polys=res.get("__ALL__",{}).get("polys",[])
        # building proxies: small footprint-ish polygons
        cent=[]
        for poly in polys:
            xs=[p[0] for p in poly]; ys=[p[1] for p in poly]
            w=max(xs)-min(xs); h=max(ys)-min(ys)
            if 1<w<40 and 1<h<40:  # building-sized at ~3.5 m/pt → 3.5-140m
                cent.append((sum(xs)/len(xs),sum(ys)/len(ys)))
        if len(cent)<200:
            print(f"{nm:<20}[{arch}] only {len(cent)} bldg-proxy fills -> georef NOT viable by buildings"); continue
        P=np.array(cent)
        with psycopg.connect(LAMAP) as c, c.cursor() as cur:
            cur.execute("SELECT ST_XMin(b),ST_YMin(b),ST_XMax(b),ST_YMax(b) FROM (SELECT ST_Extent(ST_Transform(geometry,2056)) b FROM ref.communes WHERE commune_name=%s) t",(nm,))
            x0,y0,x1,y1=cur.fetchone()
            cx,cy=(x0+x1)/2,(y0+y1)/2
            cur.execute("""SELECT ST_X(ST_Centroid(geometry)),ST_Y(ST_Centroid(geometry)) FROM ref.buildings_geo
                           WHERE ST_X(ST_Centroid(geometry)) BETWEEN %s AND %s AND ST_Y(ST_Centroid(geometry)) BETWEEN %s AND %s""",
                        (cx-6000,cx+6000,cy-6000,cy+6000))
            Q=np.array(cur.fetchall(),float)
        if len(Q)<200: print(f"{nm:<20}[{arch}] only {len(Q)} real buildings nearby"); continue
        CELL=20.0; qx0,qy0=Q[:,0].min()-500,Q[:,1].min()-500; qx1,qy1=Q[:,0].max()+500,Q[:,1].max()+500
        NX=int((qx1-qx0)/CELL)+1;NY=int((qy1-qy0)/CELL)+1
        def rast(p,x0,y0,nx,ny):
            g=np.zeros((ny,nx),np.float32);ix=((p[:,0]-x0)/CELL).astype(int);iy=((p[:,1]-y0)/CELL).astype(int)
            m=(ix>=0)&(ix<nx)&(iy>=0)&(iy<ny);np.add.at(g,(iy[m],ix[m]),1.0);return g
        Qf=np.fft.rfft2(rast(Q,qx0,qy0,NX,NY)); best=None
        for s in np.arange(3.40,3.66,0.02):
            Pf=P.copy();Pf[:,0]-=Pf[:,0].min();Pf[:,1]-=Pf[:,1].min();Ps=Pf*s
            if Ps[:,0].max()>(qx1-qx0) or Ps[:,1].max()>(qy1-qy0): continue
            corr=np.fft.irfft2(Qf*np.conj(np.fft.rfft2(rast(Ps,0,0,NX,NY))),s=(NY,NX))
            pk=np.unravel_index(np.argmax(corr),corr.shape);v=corr[pk]
            if best is None or v>best[0]: best=(v,s,pk)
        v,s,(dy,dx)=best; minx,miny=P[:,0].min(),P[:,1].min()
        xoff=qx0+dx*CELL-s*minx; yoff=qy0+dy*CELL-s*miny
        T=np.column_stack([P[:,0]*s+xoff,P[:,1]*s+yoff])
        rs=set((int(x//25),int(y//25)) for x,y in Q)
        hit=sum(any((int(x//25)+a,int(y//25)+b) in rs for a in(-1,0,1) for b in(-1,0,1)) for x,y in T)
        print(f"{nm:<20}[{arch}] proxies={len(P)} scale={s:.2f} match={100*hit/len(T):.1f}%")
    except Exception as e:
        print(f"{nm:<20}[{arch}] ERROR {e}")
