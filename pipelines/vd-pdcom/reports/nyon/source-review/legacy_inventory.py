"""Read Nyon's published literal chapter index without executing legacy JavaScript."""
import pathlib,re,json,hashlib,concurrent.futures,zipfile,datetime
from urllib.parse import urljoin,urlsplit
import requests
from bs4 import BeautifulSoup
ROOT='https://www.nyon.ch/plan-directeur/'
OUT=pathlib.Path(__file__).resolve().parent

def chapter_paths(text):
    current=None;out=[]
    for line in text.splitlines():
        m=re.match(r'\s*path="([a-z_]+)";',line)
        if m:current=m[1]
        m=re.search(r'addChapter\("[^"]*", "([a-z0-9_.-]+)"',line)
        if m:
            if current is None:raise ValueError('chapter_without_path')
            out.append('documents/'+current+'/'+m[1]+'.html')
    if not 1<=len(out)<=300:raise ValueError('unexpected_chapter_count')
    return list(dict.fromkeys(out))

def fetch(path):
    url=urljoin(ROOT,path)
    if not url.startswith(ROOT):raise ValueError('unreviewed_legacy_path')
    with requests.get(url,stream=True,timeout=(10,30),allow_redirects=False) as r:
        if r.status_code!=200:return dict(path=path,url=url,status=r.status_code),None
        data=bytearray()
        for chunk in r.iter_content(65536):
            data.extend(chunk)
            if len(data)>2_000_000:raise ValueError('legacy_page_size_limit')
    data=bytes(data);soup=BeautifulSoup(data,'html.parser')
    title=soup.title.get_text(' ',strip=True) if soup.title else ''
    if 'Plan directeur' not in title:raise ValueError('unexpected_legacy_document')
    images=sorted({urljoin(url,a['src']) for a in soup.select('img[src]')})
    return dict(path=path,url=url,status=200,sha256=hashlib.sha256(data).hexdigest(),bytes=len(data),title=title,text_chars=len(soup.get_text(' ',strip=True)),image_urls=images),data

def main():
    manifest,data=fetch('frames/fonctions_js.html');assert data is not None
    paths=chapter_paths(data.decode('latin1'));results=[];bodies={'frames/fonctions_js.html':data}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        for row,raw in pool.map(fetch,paths):
            results.append(row)
            if raw is not None:bodies[row['path']]=raw
    with zipfile.ZipFile(OUT/'legacy-html-snapshot.zip','w',zipfile.ZIP_DEFLATED) as z:
        for name,raw in sorted(bodies.items()):
            info=zipfile.ZipInfo(name,date_time=(2026,9,15,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED;z.writestr(info,raw)
    report=dict(checked_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_root=ROOT,index=manifest,chapter_count=len(paths),html_success=sum(x['status']==200 for x in results),failures=[x for x in results if x['status']!=200],chapters=results,snapshot_sha256=hashlib.sha256((OUT/'legacy-html-snapshot.zip').read_bytes()).hexdigest(),status='legacy_source_inventory_not_pdf_or_geographic_coverage',limit='Literal published HTML index only; no JavaScript execution, obsolete viewer installation or claimed2004byte-version equivalence.')
    (OUT/'legacy-inventory.json').write_text(json.dumps(report,indent=2)+'\n');print({k:report[k] for k in ['chapter_count','html_success','failures']})
if __name__=='__main__':main()
