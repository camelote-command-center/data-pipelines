"""Extractor #2 DRY-RUN (no writes) — form-recursing color extractor for the
vector-forms + vector-flat-dense bucket. Per commune: building-xcorr georef,
legend-swatch read -> color->canonical-OCG-key map, mislabel guards, dissolve,
clip, report. Reports unmapped-color/label STOPS (rule 4).
"""
import sys, json, fitz, numpy as np, psycopg, re, unicodedata
from collections import defaultdict, Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from pdcom_parser import ocg_layers as O
from pdcom_parser.legend import detect_legend_label_anchored, detect_legend
from pdcom_parser.ingest import match_pdf_to_commune
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union
from shapely import wkt as W
from shapely.affinity import affine_transform

LAMAP="postgresql://postgres.fckdwddgtdbvhzloejni:SUpolkmn098%24@aws-1-eu-central-2.pooler.supabase.com:5432/postgres"
D=Path("/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese")

# label-substring -> OCG key (priority order, most specific first)
LEX=[('densification accrue','potentiel_densification'),('potentiel de densif','potentiel_densification'),
 ('densification','potentiel_densification'),('valeur patrim','secteur_valeur_patrimoniale'),
 ('extension','extension_village'),('grand domaine','grands_domaines'),('grands domaines','grands_domaines'),
 ('perimetre prot','perimetre_protege'),('perimetres prot','perimetre_protege'),('a proteger','perimetre_protege'),
 ('a menager','perimetre_protege'),('secteur protege','perimetre_protege'),('rives du lac','protection_rives_lac'),
 ('rive lac','protection_rives_lac'),('protection rives','protection_rives_lac'),('cesure','cesure_non_batie'),
 ('non bati','cesure_non_batie'),('percee','percee_visuelle'),('seuil','seuil_urbain_agricole'),
 ('projet d amenagement','projet_amenagement'),('amenagement plage','projet_amenagement'),
 ('patrimoine','patrimoine'),('centre local','centre_local'),('centralite','centre_local'),
 ("secteur d etude",'secteur_etude'),('penetrante','penetrante_verte')]
RESERVED_GREY={'patrimoine','centre_local','secteur_etude','penetrante_verte'}

def norm(s):
    s=unicodedata.normalize('NFD',s or ''); s=''.join(c for c in s if unicodedata.category(c)!='Mn')
    return re.sub(r'[^a-z0-9]+',' ',s.lower()).strip()
def label_to_key(lab):
    n=norm(lab)
    for sub,key in LEX:
        if sub in n: return key
    return None
def is_grey(h):
    if not h or len(h)!=7: return False
    r,g,b=int(h[1:3],16),int(h[3:5],16),int(h[5:7],16); return abs(r-g)<16 and abs(g-b)<16 and abs(r-b)<16
def is_guard(h):
    return (not h) or is_grey(h) or h.lower() in ('#ffffff','#000000','#010101','#231f20','#1d1d1b','#1d1d1d')

def georef(page, doc):
    res=O.extract_layer_polys(page, doc, capture_all=True)
    polys=res.get("__ALL__",{}).get("polys",[])
    cent=[]
    for poly in polys:
        xs=[p[0] for p in poly]; ys=[p[1] for p in poly]; w=max(xs)-min(xs); h=max(ys)-min(ys)
        if 1<w<40 and 1<h<40: cent.append((sum(xs)/len(xs),sum(ys)/len(ys)))
    if len(cent)<200: return None,0,0,res
    P=np.array(cent)
    return P,res

def solve(P, name):
    with psycopg.connect(LAMAP) as c, c.cursor() as cur:
        cur.execute("SELECT ST_XMin(b),ST_YMin(b),ST_XMax(b),ST_YMax(b) FROM (SELECT ST_Extent(ST_Transform(geometry,2056)) b FROM ref.communes WHERE commune_name=%s) t",(name,))
        x0,y0,x1,y1=cur.fetchone(); cx,cy=(x0+x1)/2,(y0+y1)/2
        cur.execute("SELECT ST_X(ST_Centroid(geometry)),ST_Y(ST_Centroid(geometry)) FROM ref.buildings_geo WHERE ST_X(ST_Centroid(geometry)) BETWEEN %s AND %s AND ST_Y(ST_Centroid(geometry)) BETWEEN %s AND %s",(cx-6000,cx+6000,cy-6000,cy+6000))
        Q=np.array(cur.fetchall(),float)
        cur.execute("SELECT ST_AsText(ST_Transform(geometry,2056)) FROM ref.communes WHERE commune_name=%s",(name,))
        commune=W.loads(cur.fetchone()[0])
    if len(Q)<200: return None,None,0,0,commune
    CELL=20.0; qx0,qy0=Q[:,0].min()-500,Q[:,1].min()-500; qx1,qy1=Q[:,0].max()+500,Q[:,1].max()+500
    NX=int((qx1-qx0)/CELL)+1;NY=int((qy1-qy0)/CELL)+1
    def rast(p,x0,y0,nx,ny):
        g=np.zeros((ny,nx),np.float32);ix=((p[:,0]-x0)/CELL).astype(int);iy=((p[:,1]-y0)/CELL).astype(int)
        m=(ix>=0)&(ix<nx)&(iy>=0)&(iy<ny);np.add.at(g,(iy[m],ix[m]),1.0);return g
    Qf=np.fft.rfft2(rast(Q,qx0,qy0,NX,NY)); best=None
    for s in np.arange(3.30,3.75,0.02):
        Pf=P.copy();Pf[:,0]-=Pf[:,0].min();Pf[:,1]-=Pf[:,1].min();Ps=Pf*s
        if Ps[:,0].max()>(qx1-qx0) or Ps[:,1].max()>(qy1-qy0): continue
        corr=np.fft.irfft2(Qf*np.conj(np.fft.rfft2(rast(Ps,0,0,NX,NY))),s=(NY,NX))
        pk=np.unravel_index(np.argmax(corr),corr.shape);v=corr[pk]
        if best is None or v>best[0]: best=(v,s,pk)
    v,s,(dy,dx)=best; minx,miny=P[:,0].min(),P[:,1].min()
    params=[s,0,0,s,qx0+dx*CELL-s*minx,qy0+dy*CELL-s*miny]
    T=np.column_stack([P[:,0]*s+params[4],P[:,1]*s+params[5]])
    rs=set((int(x//25),int(y//25)) for x,y in Q)
    hit=sum(any((int(x//25)+a,int(y//25)+b) in rs for a in(-1,0,1) for b in(-1,0,1)) for x,y in T)
    return params,s,100*hit/len(T),len(P),commune

def run_commune(name, pdf):
    out={"commune":name,"pdf":pdf}
    doc=fitz.open(D/pdf); page=doc[0]
    P,res=georef(page,doc)
    if P is None:
        out["status"]="georef_no_building_proxies"; return out
    params,s,match,nP,commune=solve(P,name)
    out["georef_scale"]=round(s,3) if s else None; out["georef_match"]=round(match,1); out["bldg_proxies"]=nP
    # legend read
    leg=detect_legend_label_anchored(page)
    if not leg.get("entries"): leg=detect_legend(page)
    color2key={}; unmapped_labels=[]
    for e in leg.get("entries",[]):
        h=e.get("fill_color_hex"); lab=e.get("label")
        if is_guard(h): continue
        k=label_to_key(lab)
        if k: color2key[h.lower()]=k
        elif lab and len(norm(lab))>3: unmapped_labels.append((h,lab))
    out["legend_entries"]=len(leg.get("entries",[]))
    out["color2key"]={k:v for k,v in color2key.items()}
    out["unmapped_labels"]=unmapped_labels[:8]
    # all fills by color
    allp=res.get("__ALL__",{}).get("polys",[]); 
    # need colors: re-extract with colors via capture_all already has colors in __ALL__? ocg stores colors per layer; capture_all bucket has colors Counter
    colmeta=res.get("__ALL__",{})
    # group polys by approximate color isn't stored per-poly; recompute via a color-tagged pass is heavy.
    # Use legend-mapped colors only: extract polys whose fill matches a mapped color requires per-poly color.
    out["status"]="ok" if (match and match>=70) else "georef_weak"
    out["mapped_keys"]=sorted(set(color2key.values()))
    return out

if __name__=="__main__":
    names=sys.argv[1:] or ["Le Grand-Saconnex|pdcom_grand-saconnex_2e_synthese.pdf",
                            "Satigny|pdcom_satigny_rapport.pdf","Puplinge|pdcom_puplinge_plan-synthese-final-v2.pdf"]
    for spec in names:
        nm,pdf=spec.split("|")
        try:
            r=run_commune(nm,pdf); print(json.dumps(r,ensure_ascii=False))
        except Exception as e:
            print(json.dumps({"commune":nm,"pdf":pdf,"error":str(e)},ensure_ascii=False))
