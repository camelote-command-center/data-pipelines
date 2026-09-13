"""Official source adapters. Preserve supplied identifiers; never infer commune IDs."""
import hashlib
import json
import re
import zipfile
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from defusedxml import ElementTree as ET

STAC = 'https://www.geodienste.ch/stac/collections/npl_nutzungsplanung/items'
ZH = 'https://oerebdocs.zh.ch/'
CANTONS = 'AG AI AR BE BL BS FR GE GL GR JU LU NE NW OW SG SH SO SZ TG TI UR VD VS ZG ZH'.split()
RESTRICTED = {'NW', 'OW', 'VD'}

def local(tag):
    return tag.split('}')[-1]

def field(e, *names):
    return next((c for c in e if local(c.tag) in names), None)

def scalar(e, *names):
    c = field(e, *names)
    return c.text.strip() if c is not None and c.text else None

def multilingual(e, *names):
    c = field(e, *names)
    if c is None:
        return []
    result = []
    for n in c.iter():
        text = scalar(n, 'Text')
        if text:
            item = {'language': scalar(n, 'Language'), 'text': text}
            if item not in result:
                result.append(item)
    if not result and c.text and c.text.strip():
        result.append({'language': None, 'text': c.text.strip()})
    return result

def parse_xtf_zip(path, canton, asset_url):
    """Stream XTFs; only retain document records, never load canton geometry in memory."""
    rows = []
    with zipfile.ZipFile(path) as archive:
        names = [n for n in archive.namelist() if n.lower().endswith('.xtf')]
        if not names:
            raise ValueError('no_xtf_members')
        for name in names:
            info = archive.getinfo(name)
            if info.file_size > 2_000_000_000:
                raise ValueError('xtf_size_limit')
            with archive.open(name) as stream:
                for _, e in ET.iterparse(stream, events=('end',)):
                    tag = local(e.tag)
                    if tag.endswith(('.Dokument', '.Document')) and 'TID' in e.attrib:
                        titles = multilingual(e, 'Titel', 'Titre')
                        links = multilingual(e, 'TextImWeb', 'TexteSurInternet')
                        bfs = scalar(e, 'NurInGemeinde', 'SeulementCommune')
                        metadata = {'asset_url': asset_url, 'member': name, 'tid': e.attrib['TID'],
                                    'titles': titles, 'links': links,
                                    'published_from': scalar(e, 'publiziertAb', 'publieDepuis'),
                                    'xml': ET.tostring(e, encoding='unicode')}
                        # One source record per language/URL, including a no-URL record.
                        for link in links or [{'language': None, 'text': None}]:
                            lang = link['language'] or next((t['language'] for t in titles if t['language']), None)
                            title = next((t['text'] for t in titles if t['language'] == lang), titles[0]['text'] if titles else e.attrib['TID'])
                            rows.append(dict(source='geodienste_npl', canton_code=canton,
                                source_key=name+'::'+e.attrib['TID']+'::'+str(link['language'])+'::'+str(link['text']),
                                title=title, document_url=link['text'], commune_bfs=int(bfs) if bfs and bfs.isdigit() else None,
                                language=lang, legal_status=scalar(e,'Rechtsstatus','StatutJuridique'),
                                document_type=scalar(e,'Typ','Type'), source_metadata=metadata))
                    # Clearing only whole feature objects preserves multilingual children.
                    if 'TID' in e.attrib:
                        e.clear()
    return rows

def parse_zh_listing(html):
    soup = BeautifulSoup(html, 'html.parser')
    # BFS from option LABEL, not internal option value / data-table-sort-value.
    communes = {}
    for opt in soup.select('select[name="gemeinden[id][]"] option'):
        m = re.fullmatch(r'(.+) \((\d+)\)', opt.get_text(strip=True))
        if m:
            communes[m[1]] = int(m[2])
    result = []
    for tr in soup.select('tbody tr'):
        cells = tr.select('td')
        if len(cells) != 10:
            raise ValueError('zh_column_contract_changed')
        texts = [c.get_text(' ', strip=True) for c in cells]
        if not texts[0].isdigit():
            raise ValueError('zh_missing_docid')
        a = cells[1].find('a', href=True)
        result.append(dict(source='zh_oerebdocs', canton_code='ZH',source_key=texts[0],
            title=texts[5],document_url=urljoin(ZH,a['href']) if a else None,
            commune_bfs=communes.get(texts[4]) if communes.get(texts[4],9000)<8000 else None,
            language='de',legal_status=texts[6],document_type=texts[3],
            source_metadata={'docid':texts[0], 'municipality':texts[4], 'theme':texts[2],
                             'date':texts[7], 'year':texts[8], 'number':texts[9],
                             'register_url':ZH+'?themen%5Bid%5D%5B%5D=1'}))
    if not result:
        raise ValueError('zh_empty_register')
    if soup.select('a[rel="next"]'):
        raise ValueError('zh_pagination_requires_adapter_update')
    return result

def stable_hash(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
