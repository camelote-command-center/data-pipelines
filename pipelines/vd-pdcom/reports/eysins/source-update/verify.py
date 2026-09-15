import hashlib,json,sys
from pathlib import Path
from bs4 import BeautifulSoup
import pymupdf
P=Path(__file__).resolve().parent
sys.path.insert(0,str(P.parents[2]));import discovery
r=json.loads((P/'source-review.json').read_text())
for page in r['page_evidence']:
    soup=BeautifulSoup((P/page['table_fixture']).read_text(),'html.parser');errors=[]
    links=[{'href':a.get('href'),'label':a.get_text(' ',strip=True),'row_context':context} for a,context in discovery.page_links(soup,errors)]
    assert not errors and links==page['embedded_links']
    assert len(links)==page['recovered_link_count'] and not soup.select('a[href]')
for d in r['documents']:
    data=(P/d['file']).read_bytes();assert hashlib.sha256(data).hexdigest()==d['sha256'];assert len(pymupdf.open(stream=data,filetype='pdf'))==d['pages']
assert not r['new_pdcom_found'] and not r['no_plan_established']
print('27 real embedded links and three adjacent reference PDFs verified; no false PDCom count.')
