"""Offline verification of the legacy index, snapshot bytes and recovered DWF."""
import pathlib,json,hashlib,zipfile
from legacy_inventory import chapter_paths
p=pathlib.Path(__file__).resolve().parent;i=json.loads((p/'legacy-inventory.json').read_text());e=json.loads((p/'source-review.json').read_text())
assert hashlib.sha256((p/'legacy-html-snapshot.zip').read_bytes()).hexdigest()==i['snapshot_sha256']
with zipfile.ZipFile(p/'legacy-html-snapshot.zip') as z:
 index=z.read('frames/fonctions_js.html');assert hashlib.sha256(index).hexdigest()==i['index']['sha256'];assert len(chapter_paths(index.decode('latin1')))==231
 assert set(chapter_paths(index.decode('latin1')))=={r['path'] for r in i['chapters']}
 for r in i['chapters']:
  if r['status']==200:assert hashlib.sha256(z.read(r['path'])).hexdigest()==r['sha256']
 assert len(z.namelist())==231 #230chapter pages plus index
assert i['html_success']==230 and len(i['failures'])==1
raw=(p/'route-st-cergue-955.dwf').read_bytes();assert raw.startswith(b'(DWF V00.36)');assert hashlib.sha256(raw).hexdigest()==e['nyon_legacy']['dwf']['sha256']
print('231chapter references,230HTMLsnapshots,1explicit404,andDWFbytes verified')
