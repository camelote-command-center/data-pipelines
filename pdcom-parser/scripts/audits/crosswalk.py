"""Per-commune crosswalk: harvested legend label -> best of 26 canonical keys.
Label kept VERBATIM as category_label. Reference/base colors skipped. Unmapped
designations listed (never force a near-miss). Diagnostic only."""
import json, re, unicodedata
from pathlib import Path
INV=Path(__file__).resolve().parent/"legend_inventory.json"
def norm(s):
    s=unicodedata.normalize('NFD',s or ''); s=''.join(c for c in s if unicodedata.category(c)!='Mn')
    return re.sub(r'[^a-z0-9]+',' ',s.lower()).strip()
# (substring, key) — ORDER = priority, most specific first
LEX=[
 # planification & procedure (specific instruments first)
 ('plan de site','plan_de_site'),('image directrice','image_directrice'),
 ("perimetre d etude",'perimetre_etude'),('etude d amenagement','perimetre_etude'),("secteur d etude",'perimetre_etude'),
 ('modification de zone','modification_zone'),('developper par mz','modification_zone'),('declassement','modification_zone'),
 ('plq','plq'),('plan localise de quartier','plq'),
 ('projet d amenagement','projet_amenagement'),('amenagement promenade','projet_amenagement'),
 # activites / equipements / OI
 ('oi et ong','quartier_oi_ong'),('oi/ong','quartier_oi_ong'),('organisations internationales','quartier_oi_ong'),
 ('quartier d activites','quartier_activites'),("secteur d activites",'quartier_activites'),('zone artisanale','quartier_activites'),
 ('artisanal','quartier_activites'),('industriel','quartier_activites'),('activites','quartier_activites'),
 ('equipement','equipement_public'),('equipements','equipement_public'),('amenagement d interet public','equipement_public'),
 ('terrains reserves a des equipements','equipement_public'),
 # patrimoine & protection
 ('valeur patrimoniale','secteur_valeur_patrimoniale'),('patrimoine bati','secteur_valeur_patrimoniale'),
 ('grand domaine','grands_domaines'),('grands domaines','grands_domaines'),('domaine de','grands_domaines'),
 ('a proteger','perimetre_protege'),('a menager','perimetre_protege'),('perimetre protege','perimetre_protege'),
 ('a preserver','perimetre_protege'),('preserver et valoriser','perimetre_protege'),('secteur protege','perimetre_protege'),
 # paysage / nature / agri
 ('inconstructible','inconstructible'),('rives du lac','protection_rives_lac'),('rive lac','protection_rives_lac'),
 ('protection rives','protection_rives_lac'),('penetrante','penetrante_verte'),('continuite vegetale','penetrante_verte'),
 ('cesure','cesure_non_batie'),('non bati','cesure_non_batie'),('percee','percee_visuelle'),('vue sur','percee_visuelle'),
 ('jardins familiaux','jardins_familiaux'),('jardins et plantages','jardins_familiaux'),('potager','jardins_familiaux'),
 ('bois','bois_foret'),('foret','bois_foret'),('bosquet','bois_foret'),('cordon boise','bois_foret'),('massif','bois_foret'),
 ('seuil','seuil_urbain_agricole'),
 ('zone agricole','espace_agricole'),('espace agricole','espace_agricole'),('entite agricole','espace_agricole'),
 ('ferme','espace_agricole'),('agricol','espace_agricole'),('vigne','espace_agricole'),
 # habitat & densification
 ('densification','potentiel_densification'),('potentiel de densif','potentiel_densification'),
 ('developpement residentiel','potentiel_densification'),
 ('front actif','front_actif'),('front bati','front_actif'),
 ('quartier residentiel','quartier_residentiel'),('zone villa','quartier_residentiel'),('zone 5','quartier_residentiel'),('densite limitee','quartier_residentiel'),('residentiel','quartier_residentiel'),
 ('extension','extension_village'),
 ('centre villageois','centre_village'),('tissu villageois','centre_village'),('coeur de village','centre_village'),
 ('centralite','centre_village'),('village','centre_village'),
]
def to_key(lab):
    n=norm(lab)
    for sub,k in LEX:
        if sub in n: return k
    return None
def is_base_color(h):
    if not h or len(h)!=7: return True
    r,g,b=int(h[1:3],16),int(h[3:5],16),int(h[5:7],16)
    if abs(r-g)<16 and abs(g-b)<16 and abs(r-b)<16: return True   # grey/white/black
    return False

d=json.load(open(INV))
crosswalk=[]; unmapped=[]
for it in d["inventory"]:
    if it.get("mobility"): continue
    for e in it["entries"]:
        if e["kind"]!="fill": continue          # strokes/text = reference, skip
        if is_base_color(e["color"]): continue   # cadastre/bati/mask/black = reference, skip
        k=to_key(e["label"])
        rec={"commune":it["commune"],"label":e["label"],"color":e["color"],"fc":e["feature_count"]}
        if k: crosswalk.append({**rec,"category_key":k})
        elif len(norm(e["label"]))>=4: unmapped.append(rec)
json.dump({"crosswalk":crosswalk,"unmapped":unmapped}, open(Path(__file__).resolve().parent/"crosswalk.json","w"), ensure_ascii=False, indent=1)
from collections import Counter
print(f"crosswalk rows: {len(crosswalk)} | unmapped: {len(unmapped)}")
print("\nkey distribution:")
for k,n in Counter(r['category_key'] for r in crosswalk).most_common(): print(f"  {n:>3}  {k}")
VOC=re.compile(r'(densit|quartier|zone|agricol|patrimoin|centralit|centre|extension|village|proteg|menager|domaine|cesure|percee|paysage|seuil|hameau|verdure|vegetal|penetrante|espace|equipement|activit|industriel|artisan|residentiel|pole|front|reserve|batir|construct|developp|amenagement|jardin|vigne|foret|bois|parc|rives|lac|plan de site|image directrice|mutation|surelev|tissu|arbre|haie|trame|site bati|sites batis|entite)',re.I)
STREET=re.compile(r'^(r\.|rte|ch\.|av\.|chemin|route|rue|impasse|sentier|promenade|pl\.|place|quai)\b|^\(|^[0-9]',re.I)
real=[u for u in unmapped if VOC.search(norm(u['label'])) and not STREET.match(u['label'].strip())]
json.dump(real, open(Path(__file__).resolve().parent/'unmapped_real.json','w'), ensure_ascii=False, indent=1)
print(f"\nUNMAPPED REAL-DESIGNATION labels (filtered from {len(unmapped)} raw): {len(real)}")
seen=set()
for u in real:
    key=norm(u['label'])
    if key in seen: continue
    seen.add(key)
    print(f"  {u['commune']:<16} {u['color']} {u['label'][:60]!r}")
