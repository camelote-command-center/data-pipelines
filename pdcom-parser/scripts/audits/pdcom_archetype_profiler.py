import fitz, glob, os, re, json, collections, unicodedata
def norm(s):
    s=unicodedata.normalize('NFD',s or ''); s=''.join(c for c in s if unicodedata.category(c)!='Mn')
    return s.lower()
def hexc(rgb):
    if rgb is None: return None
    r,g,b=(max(0,min(255,int(round(c*255)))) for c in rgb[:3]); return f"#{r:02x}{g:02x}{b:02x}"
def is_grey(h):
    if not h: return False
    r=int(h[1:3],16);g=int(h[3:5],16);b=int(h[5:7],16)
    return abs(r-g)<12 and abs(g-b)<12 and abs(r-b)<12
KW={'potentiel_densification':['densification','potentiel de densif'],'zone_5':['zone 5','zone villa','5e zone'],
    'extension_village':['extension','village'],'perimetre_protege':['protege','protég','a menager','ménager'],
    'secteur_valeur_patrimoniale':['valeur patrim','patrimoine'],'grands_domaines':['grand domaine','grands domaines'],
    'protection_rives_lac':['rives du lac','rive du lac'],'cesure_non_batie':['cesure','césure','non bati','non bâti'],
    'plq_a_etablir':['plq','modification de zone',' mz '],'plq_realise':['plq realise','plq réalisé','en force'],
    'centre_local':['centre local','centralite','centralité'],'secteur_etude':["secteur d'etude","secteur d'étude"]}
d="/Users/a/Desktop/Lamap Reshape/PDCom/Plans de Synthese"
out=[]
for f in sorted(glob.glob(d+"/*.pdf")):
    base=os.path.basename(f)
    try:
        doc=fitz.open(f); page=doc[0]; pr=page.rect
        n_img=len(page.get_images()); n_pages=doc.page_count
        ocg=len(doc.get_ocgs())
        # page-stream fills via get_drawings (v0.5.3 view)
        col_area=collections.defaultdict(float); col_cnt=collections.defaultdict(int)
        try:
            for dr in page.get_drawings():
                fill=dr.get('fill')
                if fill is None: continue
                r=dr.get('rect')
                if r is None: continue
                a=abs((r[2]-r[0])*(r[3]-r[1]))
                h=hexc(fill); col_area[h]+=a; col_cnt[h]+=1
        except Exception: pass
        # form usage (impoverishment signal): count Do + sample form fill colors
        ndo=0; form_cols=collections.Counter()
        try:
            cont=page.read_contents()
            ndo=cont.count(b' Do')+cont.count(b'\nDo')
        except Exception: pass
        txt=norm(page.get_text())
        khits=[k for k,kws in KW.items() if any(w in txt for w in kws)]
        total_a=sum(col_area.values()) or 1
        top=sorted(col_area.items(),key=lambda x:-x[1])[:6]
        top_nongrey=[(h,round(100*a/total_a)) for h,a in top if not is_grey(h)][:5]
        # archetype
        if ocg>=20: arch='OCG-illustrator' if any(n.get('name')=='POTENTIEL DENSIFICATION' for n in doc.get_ocgs().values()) else 'OCG-stylo-plandesite'
        elif n_img>=1 and len(page.get_drawings())<50: arch='raster-scan'
        elif ndo>20: arch='vector-forms(hidden-fills)'
        elif len(page.get_drawings())>200: arch='vector-flat-dense'
        else: arch='vector-flat-sparse'
        out.append({'pdf':base,'pages':n_pages,'imgs':n_img,'ocg':ocg,'drawings':len(page.get_drawings()),
                    'n_do':ndo,'archetype':arch,'kw':khits,'top_nongrey_colors':top_nongrey,
                    'grey_share_pct':round(100*sum(a for h,a in col_area.items() if is_grey(h))/total_a)})
    except Exception as e:
        out.append({'pdf':base,'error':str(e)})
json.dump(out, open("/tmp/profile.json","w"), ensure_ascii=False)
# archetype tally
arch=collections.Counter(o.get('archetype','ERR') for o in out)
print("ARCHETYPE BUCKETS:")
for a,c in arch.most_common(): print(f"  {c:>3}  {a}")
print(f"\n{len(out)} PDFs profiled -> /tmp/profile.json")
