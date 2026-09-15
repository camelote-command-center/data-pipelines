"""Offline extraction of historical tables and raw navigation polygons; no CRS."""
import hashlib, json, re, zipfile
from pathlib import Path
from decimal import Decimal
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw
from shapely.geometry import Polygon
from shapely.validation import explain_validity

ROOT = Path(__file__).resolve().parent
ZIP = ROOT.parent / 'source-review/legacy-html-snapshot.zip'
BASE = 'https://www.nyon.ch/plan-directeur/'
def sha(data): return hashlib.sha256(data).hexdigest()
def clean(tag): return ' '.join(tag.get_text(' ', strip=True).split())
def main():
    z = zipfile.ZipFile(ZIP)
    def soup(name): return BeautifulSoup(z.read(name).decode('latin1'), 'html.parser')
    index = z.read('frames/fonctions_js.html').decode('latin1')
    section = index.split('path="secteurs";', 1)[1].split('path="lieux";', 1)[0]
    chapters = re.findall(r'addChapter\("", "([^"]+)"', section)
    assert chapters[:18] == ['doc-sec'+chr(97+i) for i in range(18)]
    im = Image.open(ROOT/'secteurs.gif').convert('RGB'); draw = ImageDraw.Draw(im)
    areas = soup('documents/secteurs/doc-secall.html').find('map', attrs={'name':'secteurs'}).find_all('area')
    rows = []; polygons = {}; mismatches = []
    for area in areas:
        target = int(re.fullmatch(r'showDocument\((\d+)\);return true;',area['onclick']).group(1))
        assert 1 <= target <= 18 and target not in polygons
        raw = [int(x) for x in area['coords'].split(',')]
        assert len(raw)%2 == 0
        points = list(zip(raw[::2], raw[1::2])); poly = Polygon(points); polygons[target] = poly
        assert all(0<=x<im.width and 0<=y<im.height for x,y in points)
        path = 'documents/secteurs/'+chapters[target-1]+'.html'
        page = soup(path); title = clean(page.h1)
        sector_ids = [int(n) for n in re.findall(r'\d+',title.split(' - ',1)[0])]
        tables = []
        for table in page.find_all('table'):
            cells = [[clean(c) for c in tr.find_all(['td','th'],recursive=False)] for tr in table.find_all('tr')]
            assert len(cells)==4 and all(len(r)==6 for r in cells)
            tables.append({'headers':cells[0], 'raw_sector_row':cells[1], 'raw_percent_row':cells[2], 'raw_city_total_row':cells[3],
                'values':dict(zip(['area_ha','historical_current','historical_additional_potential','historical_total_potential','potential_density_per_ha'],[None if v=='-' else float(Decimal(v)) for v in cells[1][1:]]))})
        assert len(tables)==2 and 'POPULATION' in tables[0]['headers'][2] and 'EMPLOIS' in tables[1]['headers'][2]
        letter=chr(64+target)
        if area['href'].split()[-1] != letter: mismatches.append({'target':target,'href':area['href'],'resolved_letter':letter})
        rows.append({'target':target,'letter':letter,'title':title,'historical_sector_ids':sector_ids,'source_url':BASE+path,'source_sha256':sha(z.read(path)),
            'population':tables[0],'employment':tables[1], 'navigation_polygon':{'raw_coords':area['coords'],'points_pixels':points,'valid':poly.is_valid,'validity':explain_validity(poly),'area_pixels_squared':poly.area,'crs':None,'semantics':'HTML image-map interaction region; not verified planning boundary'}})
        draw.line(points+[points[0]],fill=(0,100,255),width=1)
        p=poly.representative_point(); draw.text((p.x,p.y),letter,fill=(0,0,0),stroke_width=1,stroke_fill=(255,255,255))
    rows.sort(key=lambda r:r['target'])
    assert sorted(n for r in rows for n in r['historical_sector_ids'])==list(range(1,24))
    totals={}
    for kind in ['population','employment']:
        assert len({tuple(r[kind]['raw_city_total_row']) for r in rows})==1
        totals[kind]={'group_sums':{key:float(sum(Decimal(str(r[kind]['values'][key])) for r in rows)) for key in ['area_ha','historical_current','historical_additional_potential','historical_total_potential']},'source_city_total_row':rows[0][kind]['raw_city_total_row']}
    overlaps=[]
    for a,p in polygons.items():
        for b,q in polygons.items():
            if a<b and p.is_valid and q.is_valid:
                area=p.intersection(q).area
                if area>0: overlaps.append({'targets':[a,b],'area_pixels_squared':area})
    evidence={}
    for name in ['doc-secanalyse','doc-sechabitat','doc-secemploi','doc-popsect','doc-empsect']:
        path='documents/secteurs/'+name+'.html'; page=soup(path)
        evidence[name]={'url':BASE+path,'sha256':sha(z.read(path)),'body_text':clean(page.body)}
    result={'commune_bfs':5724,'classification':'historical quantitative analysis; research only','baseline_year':None,'baseline_year_status':'not established; page title 2000 is not a verified statistical reference year','approved_version_equivalence':'unresolved','georeferenced':False,'receiver_eligible':False,'zip_sha256':sha(ZIP.read_bytes()),'image':{'url':BASE+'documents/lieux/plans/secteurs.gif','sha256':sha((ROOT/'secteurs.gif').read_bytes()),'size_pixels':list(im.size)},'groups':rows,'totals':totals,'qa':{'href_mismatches':mismatches,'invalid_targets':[r['target'] for r in rows if not r['navigation_polygon']['valid']],'overlaps_between_valid_polygons':overlaps,'invalid_geometry_repaired':False},'methodology_evidence':evidence}
    (ROOT/'extraction.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    im.resize((im.width*3,im.height*3)).save(ROOT/'navigation-overlay.png')
    print(json.dumps({'groups':len(rows),'sector_ids':23,'totals':totals,'qa':result['qa']},indent=2))
if __name__=='__main__':main()
