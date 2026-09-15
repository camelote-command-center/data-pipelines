"""Extract original SDAN map rasters with source/page provenance; never infer CRS."""
import argparse, hashlib, json
from pathlib import Path
import pymupdf as fitz

ROOT=Path(__file__).resolve().parent
SHA='35b62cc028b93a649c7b752c14c2e549bee6ac5d207f1c3033ecf55445b5a729'
MAPS={12:[(52,'urban-concept')],20:[(114,'eysins-landscape'),(115,'eysins-urban'),(116,'prangins-landscape-urban'),(117,'prangins-uses')],22:[(124,'nyon-long-term-landscape'),(125,'nyon-long-term-urban'),(126,'vuarpilliere-mondre-asse-landscape-urban'),(127,'vuarpilliere-mondre-asse-urban')],30:[(156,'implementation-workstreams')]}
LEFT=['Centre principal de Nyon','Double centre de Prangins','Centre de grand village à renforcer','Quartier de village avec densification possible','Zone à bâtir périphérique sans densification','Nouvelle route avec fonctions centrales','Secteur construit avec renouvellement urbain','Secteur avec potentiel élevé de développement dans les zones à bâtir actuelles','Secteur de densification dans zones à bâtir actuelles','Secteur avec potentiel élevé de développement hors zones à bâtir actuelles','Secteur avec potentiel élevé de développement à long terme hors zones à bâtir actuelles','Compléments d’urbanisation importants dans les grands villages (Crans-près-Céligny, Trélex)']
RIGHT=['Secteur de la Vuarpillière – L’Asse pour un développement sélectif','Secteur de L’Asse pour infrastructures et équipements publics','Parc urbain','Porte principale de Nyon (niveau agglomération)','Porte de localité (niveau local)','Aménagement paysager des limites d’urbanisation','Littoral avec protection paysagère prioritaire','Vallon avec cours d’eau et cordons boisés à respecter','Espace vert tampon d’importance régionale','Zone agricole et forêt','Zone intermédiaire à supprimer']
def digest(b):return hashlib.sha256(b).hexdigest()
def main():
    ap=argparse.ArgumentParser();ap.add_argument('pdf',type=Path);args=ap.parse_args()
    assert digest(args.pdf.read_bytes())==SHA,'Source version changed'
    doc=fitz.open(args.pdf);assert len(doc)==43
    maps=[]
    def asset(page_no,xref,name):
        page=doc[page_no-1];data=doc.extract_image(xref);rects=page.get_image_rects(xref)
        assert len(rects)==1
        path='images/'+name+'.'+data['ext'];(ROOT/path).parent.mkdir(exist_ok=True);(ROOT/path).write_bytes(data['image'])
        return {'pdf_page':page_no,'printed_page':page_no-4,'xref':xref,'file':path,'width':data['width'],'height':data['height'],'sha256':digest(data['image']),'page_rect_points':list(rects[0]),'crs':None}
    for n,items in MAPS.items():
        page=doc[n-1]
        assert len(page.get_drawings())==2,'Review new vector content before proceeding'
        for xref,name in items:
            item=asset(n,xref,name);item['name']=name;maps.append(item)
    page=doc[12];symbols=[]
    for x in page.get_images():
        rect=page.get_image_rects(x[0])[0];symbols.append((rect.x0,rect.y0,x[0]))
    assert len(symbols)==23
    for side,labels,select in [('left',LEFT,lambda x:x<300),('right',RIGHT,lambda x:x>=300)]:
        items=sorted([x for x in symbols if select(x[0])],key=lambda x:x[1]);assert len(items)==len(labels)
        for i,((_,_,xref),label) in enumerate(zip(items,labels),1):
            item=asset(13,xref,f'legend-{side}-{i:02}');item['label']=label;item['side']=side;item['row']=i
            maps.append(item)
    text={str(n):doc[n-1].get_text() for n in [5,13,19,20,21,22,23,25,27,30,31,33,34,35]}
    out={'document_id':'bd31519f-f1a2-5579-9ee0-e852f082d1dc','source_url':'https://www.nyon.ch/media/document/0/rapport-synthese-sdan-document-urb-nyon-071220.pdf','source_sha256':SHA,'maps':maps[:10],'urban_legend':maps[10:],'page_text':text,'geographic_validation':'pending','receiver_release':'not_ready','vector_map_paths':0,'vector_path_note':'Each selected map page contains two header/footer rules; thematic map content is embedded raster.'}
    (ROOT/'inventory.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
    print('Extracted 10 original map rasters and 23 legend symbols; source SHA verified.')
if __name__=='__main__':main()
