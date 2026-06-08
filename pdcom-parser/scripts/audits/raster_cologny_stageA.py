"""CC raster-overlay accuracy benchmark — Cologny, STAGE A (perfect georef).
DIAGNOSTIC ONLY. No DB writes.

Renders the OCG vector PDF to RGB @300dpi (pixel->world exact via the same
building-xcorr affine that built ground truth -> georef error 0), masks colored
fills, calibrates each category's RENDERED color from its known zones (legend-
equivalent per-commune binding; the vector swatch is unreliable under alpha
blending -- potentiel_densification proves this), then per Cologny parcel
(mv_plots geometry) does a zonal majority vote and scores vs ground truth across
a coverage-threshold sweep. Confusion matrix includes the 'none' class.
"""
import sys, json
from pathlib import Path
import numpy as np, psycopg, fitz
from PIL import Image, ImageDraw
from shapely import wkt as W
sys.path.insert(0, str(Path("/Users/a/data-pipelines/pdcom-parser/src")))
sys.path.insert(0, str(Path("/Users/a/data-pipelines/pdcom-parser/scripts")))
from pdcom_parser import ocg_layers as O
import cologny_ocg_load as C   # reuse georeference()

LAMAP = C.LAMAP
PDF = C.PDF
DPI = 300
SCALE = DPI/72.0
KEYS = ['potentiel_densification','grands_domaines','secteur_valeur_patrimoniale',
        'extension_village','percee_visuelle','cesure_non_batie']

def rgb2lab(arr):
    a = arr.astype(np.float64)/255.0
    m = a > 0.04045
    a[m] = ((a[m]+0.055)/1.055)**2.4; a[~m] = a[~m]/12.92
    a *= 100.0
    X = a[...,0]*0.4124+a[...,1]*0.3576+a[...,2]*0.1805
    Y = a[...,0]*0.2126+a[...,1]*0.7152+a[...,2]*0.0722
    Z = a[...,0]*0.0193+a[...,1]*0.1192+a[...,2]*0.9505
    X/=95.047; Y/=100.0; Z/=108.883
    def f(t):
        t=t.copy(); mm=t>0.008856; t[mm]=t[mm]**(1/3); t[~mm]=7.787*t[~mm]+16/116; return t
    fx,fy,fz=f(X),f(Y),f(Z)
    L=116*fy-16; A=500*(fx-fy); B=200*(fy-fz)
    return np.stack([L,A,B],-1)

def hsv_colored_mask(rgb, s_thr=0.18, v_lo=0.20, v_hi=0.95):
    r,g,b = rgb[...,0]/255.,rgb[...,1]/255.,rgb[...,2]/255.
    mx=np.maximum(np.maximum(r,g),b); mn=np.minimum(np.minimum(r,g),b)
    v=mx; s=np.where(mx>0,(mx-mn)/np.maximum(mx,1e-9),0)
    return (s>s_thr)&(v>v_lo)&(v<v_hi)

def main():
    # ---- DB pulls
    with psycopg.connect(LAMAP) as cn, cn.cursor() as cur:
        cur.execute("""SELECT egrid, ST_AsText(ST_Transform(geometry,2056))
                       FROM ref.plots WHERE commune_name='Cologny' AND geometry IS NOT NULL""")
        parcels = [(e, W.loads(w)) for e,w in cur.fetchall()]
        cur.execute("""SELECT DISTINCT ON (egrid) egrid, category_key
                       FROM ref.plot_pdcom_urbanisation WHERE commune_name='Cologny'
                       ORDER BY egrid, overlap_m2 DESC NULLS LAST""")
        truth = dict(cur.fetchall())
        cur.execute("""SELECT category_key, ST_AsText(geometry)
                       FROM ref.pdcom_urbanisation_zones WHERE commune_name='Cologny'""")
        zones = [(k, W.loads(w)) for k,w in cur.fetchall()]
    print(f"parcels={len(parcels)}  truth-labeled={len(truth)}  zone-rows={len(zones)}")

    # ---- render
    doc=fitz.open(PDF); page=doc[0]; page_h=page.rect.height
    pix=page.get_pixmap(matrix=fitz.Matrix(SCALE,SCALE), colorspace=fitz.csRGB, alpha=False)
    img=np.frombuffer(pix.samples,dtype=np.uint8).reshape(pix.height,pix.width,3)
    H,Wd=img.shape[:2]; print("render",img.shape)

    # ---- perfect georef = the building-xcorr affine that built truth
    params,s,corr,match=C.georeference(page,doc)
    a,b,d,e,xoff,yoff = params  # shapely 6-tuple: X=a*x+b*y+xoff ; Y=d*x+e*y+yoff
    print(f"georef: scale={s:.3f} m/pt  building_match={match:.1f}%  px/m≈{SCALE/s:.3f}")

    def world2pix(X,Y):
        # invert affine (b=d=0): x=(X-xoff)/a ; y=(Y-yoff)/e ; PDF y-up -> fitz y-down
        x=(X-xoff)/a; y=(Y-yoff)/e
        return x*SCALE, (page_h-y)*SCALE

    def poly_pixels(geom):
        """Return RGB pixel array inside geom (world 2056) via cropped PIL mask."""
        gg = geom.geoms if geom.geom_type.startswith('Multi') else [geom]
        allpx=[]
        for g in gg:
            xs,ys=g.exterior.xy
            pts=[world2pix(X,Y) for X,Y in zip(xs,ys)]
            isx=[p[0] for p in pts]; jsy=[p[1] for p in pts]
            i0,i1=int(max(0,min(isx))),int(min(Wd,max(isx))+1)
            j0,j1=int(max(0,min(jsy))),int(min(H,max(jsy))+1)
            if i1-i0<1 or j1-j0<1: continue
            m=Image.new('1',(i1-i0,j1-j0),0); dr=ImageDraw.Draw(m)
            dr.polygon([(p[0]-i0,p[1]-j0) for p in pts], fill=1)
            mk=np.array(m,bool)
            crop=img[j0:j1,i0:i1]
            if mk.shape==crop.shape[:2] and mk.any():
                allpx.append(crop[mk])
        return np.concatenate(allpx) if allpx else np.empty((0,3),np.uint8)

    # ---- calibrate each category's RENDERED color from its zone pixels
    cent={}
    for k in KEYS:
        zpx=[poly_pixels(g) for kk,g in zones if kk==k]
        zpx=np.concatenate(zpx) if zpx else np.empty((0,3),np.uint8)
        if len(zpx)==0: print(f"  [calib] {k}: no zone pixels"); continue
        col=hsv_colored_mask(zpx)
        use=zpx[col] if col.sum()>=30 else zpx
        med=np.median(use,axis=0)
        cent[k]=med
        print(f"  [calib] {k:<30} rendered≈#{int(med[0]):02x}{int(med[1]):02x}{int(med[2]):02x}  "
              f"(colored {100*col.mean():.0f}% of zone px, n={len(zpx)})")
    cent_keys=list(cent.keys())
    cent_lab=rgb2lab(np.array([cent[k] for k in cent_keys]).reshape(-1,1,3)).reshape(-1,3)

    # global colored fraction of whole map
    gc=hsv_colored_mask(img); print(f"colored fraction of full map: {100*gc.mean():.1f}%")

    # ---- per-parcel classification
    recs=[]
    for idx,(egrid,geom) in enumerate(parcels):
        px=poly_pixels(geom); n=len(px)
        if n==0: recs.append((egrid,'none',0.0,0)); continue
        col=hsv_colored_mask(px); ncol=int(col.sum()); cov=ncol/n
        if ncol==0: recs.append((egrid,'none',0.0,n)); continue
        lab=rgb2lab(px[col].reshape(-1,1,3)).reshape(-1,3)
        dist=np.linalg.norm(lab[:,None,:]-cent_lab[None,:,:],axis=2)
        assign=dist.argmin(1)
        votes=np.bincount(assign,minlength=len(cent_keys))
        recs.append((egrid, cent_keys[int(votes.argmax())], cov, n))
        if idx%200==0: print(f"  ..{idx}/{len(parcels)}")

    # ---- score across threshold sweep
    cls = KEYS+['none']
    def score(T):
        cm={t:{p:0 for p in cls} for t in cls}
        ok=0; tot=0
        for egrid,pred,cov,n in recs:
            t = truth.get(egrid,'none')
            p = pred if (pred!='none' and cov>=T) else 'none'
            cm[t][p]+=1; tot+=1; ok+= (t==p)
        return ok/tot, cm
    out={"georef":{"scale_m_per_pt":round(s,4),"building_match_pct":round(match,1),
                   "px_per_m":round(SCALE/s,3)},
         "colored_fraction_pct":round(100*gc.mean(),1),
         "calibrated_colors":{k:f"#{int(cent[k][0]):02x}{int(cent[k][1]):02x}{int(cent[k][2]):02x}" for k in cent},
         "n_parcels":len(parcels),"n_truth":len(truth),"sweep":{}}
    print("\n=== THRESHOLD SWEEP (overall per-plot accuracy) ===")
    for T in [0.01,0.02,0.05,0.10,0.15,0.20]:
        acc,cm=score(T)
        out["sweep"][T]={"acc":round(acc,4),"cm":cm}
        print(f"  T={T:>4}: acc={100*acc:.1f}%")
    # confusion at T=0.05 (representative)
    _,cm=score(0.05)
    print("\n=== CONFUSION MATRIX @T=0.05 (rows=truth, cols=pred) ===")
    hdr=[c[:10] for c in cls]; print("truth\\pred  "+" ".join(f"{h:>10}" for h in hdr))
    for t in cls:
        print(f"{t[:10]:<11}"+" ".join(f"{cm[t][p]:>10}" for p in cls))
    Path("/tmp/raster_stageA.json").write_text(json.dumps(out,ensure_ascii=False,indent=0))
    print("\nwrote /tmp/raster_stageA.json")

if __name__=="__main__": main()
