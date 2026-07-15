"""Build human-checkable sample tables for the two generalization communes.
Stratified-random sample weighted across predicted classes so Ilan/claude.ai
can eyeball-verify; per-parcel CROP saved with the parcel outlined."""
import json, sys, random
from pathlib import Path
import fitz, psycopg
from shapely import wkt as W
from PIL import Image, ImageDraw, ImageFont

LAMAP="postgresql://postgres.fckdwddgtdbvhzloejni:SUpolkmn098%24@aws-1-eu-central-2.pooler.supabase.com:5432/postgres"
random.seed(13)

def build(commune, pdf, georef_json, pred_json, outdir, target_n=70):
    OUT=Path(outdir); OUT.mkdir(exist_ok=True)
    g=json.load(open(georef_json)); a,_,_,e,xoff,yoff=g["params"]
    pred=json.load(open(pred_json))
    idx_path = Path(pred_json).parent.parent / "index.json"
    # find index next to tiles
    key="corsier" if commune.lower().startswith("corsier") else "vand"
    idx_json=Path(f"/tmp/fable_tiles_{key}/index.json")
    idx=json.load(open(idx_json))
    # flatten: per-parcel pred
    p2c={}
    for t,labs in pred.items():
        for lab,d in labs.items():
            p2c[idx[t][lab]] = d.get("cat",d.get("class","none"))
    # stratify by class
    by_cls={}
    for eg,c in p2c.items(): by_cls.setdefault(c,[]).append(eg)
    # target: cap each class at target_n//n_cls + per-class top-up; ensure each class with >=3 picks gets at least 5
    picks=[]
    cls_list=sorted(by_cls.keys())
    per=max(5, target_n//max(len(cls_list),1))
    for c,egs in by_cls.items():
        random.shuffle(egs)
        picks += [(eg,c) for eg in egs[:per]]
    if len(picks)<target_n:
        # top up with extra randoms
        extra=[(eg,c) for c,egs in by_cls.items() for eg in egs[per:]]
        random.shuffle(extra); picks += extra[:target_n-len(picks)]
    picks=picks[:target_n]
    print(f"{commune}: {len(picks)} sample parcels across {len({c for _,c in picks})} classes")
    # geometry lookup
    with psycopg.connect(LAMAP) as cn:
        cur=cn.cursor()
        cur.execute("""SELECT egrid, parcel_number, ST_AsText(ST_Transform(geometry,2056))
                       FROM ref.plots WHERE commune_name=%s AND egrid = ANY(%s)""",
                    (commune,[eg for eg,_ in picks]))
        geom={eg:(pn,W.loads(w)) for eg,pn,w in cur.fetchall()}
    doc=fitz.open(pdf); page=doc[0]; page_h=page.rect.height; page_w=page.rect.width
    Z=4.0
    try: font=ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc",36)
    except Exception: font=ImageFont.load_default()
    rows=[]
    for i,(eg,c) in enumerate(picks,1):
        if eg not in geom: continue
        pn,gm=geom[eg]
        cx,cy=gm.centroid.x,gm.centroid.y
        xpt=(cx-xoff)/a; ypt=(cy-yoff)/e
        # window: 160m square
        win=160
        x0pt=(cx-win/2-xoff)/a; x1pt=(cx+win/2-xoff)/a
        y0pt=(cy-win/2-yoff)/e; y1pt=(cy+win/2-yoff)/e
        cl=fitz.Rect(max(0,min(x0pt,x1pt)),max(0,page_h-max(y0pt,y1pt)),
                     min(page_w,max(x0pt,x1pt)),min(page_h,page_h-min(y0pt,y1pt)))
        if cl.width<10 or cl.height<10: continue
        pix=page.get_pixmap(matrix=fitz.Matrix(Z,Z),clip=cl,colorspace=fitz.csRGB,alpha=False)
        img=Image.frombytes("RGB",(pix.width,pix.height),pix.samples)
        dr=ImageDraw.Draw(img)
        # draw parcel outline
        geoms=gm.geoms if gm.geom_type.startswith("Multi") else [gm]
        for gg in geoms:
            pts=[((((X-xoff)/a)-cl.x0)*Z, ((page_h-((Y-yoff)/e))-cl.y0)*Z) for X,Y in zip(*gg.exterior.xy)]
            if len(pts)>=3:
                dr.line(pts+[pts[0]],fill=(255,0,0),width=4)
        # label
        cxp=((xpt-cl.x0))*Z; cyp=((page_h-ypt)-cl.y0)*Z
        bb=dr.textbbox((0,0),str(i),font=font); wpx,hpx=bb[2]-bb[0],bb[3]-bb[1]
        dr.rectangle([cxp-wpx/2-6,cyp-hpx/2-6,cxp+wpx/2+6,cyp+hpx/2+6],fill=(255,255,255),outline=(255,0,0),width=2)
        dr.text((cxp-wpx/2,cyp-hpx/2-bb[1]),str(i),fill=(200,0,0),font=font)
        path=OUT/f"s{i:03d}_{eg}.png"
        img.save(path)
        rows.append({"n":i,"egrid":eg,"parcel":pn,"predicted":c,"crop":str(path)})
    (OUT/"sample.json").write_text(json.dumps(rows,indent=1))
    # ASCII table
    with open(OUT/"sample.tsv","w") as f:
        f.write("n\tegrid\tparcel\tpredicted\tcrop\n")
        for r in rows:
            f.write(f"{r['n']}\t{r['egrid']}\t{r['parcel']}\t{r['predicted']}\t{r['crop']}\n")
    print(f"  wrote {len(rows)} crops -> {OUT}/sample.tsv")
    return rows

if __name__=="__main__":
    build("Corsier","/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese/pdcom_corsier_2e_synthese.pdf",
          "/tmp/georef_corsier.json","/tmp/fable_out_corsier/corsier_pred.json",
          "/tmp/fable_sample_corsier",70)
    build("Vandoeuvres","/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese/pdcom_vandoeuvres_synthese.pdf",
          "/tmp/georef_vandoeuvres.json","/tmp/fable_out_vand/vand_pred.json",
          "/tmp/fable_sample_vand",70)
