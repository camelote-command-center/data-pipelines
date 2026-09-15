import argparse,hashlib,json,subprocess,sys
from pathlib import Path
from PIL import Image
P=Path(__file__).resolve().parent
ap=argparse.ArgumentParser();ap.add_argument('--pdf',type=Path);args=ap.parse_args()
d=json.loads((P/'inventory.json').read_text());review=json.loads((P/'semantic-review.json').read_text())
assert len(d['maps'])==10 and len(d['urban_legend'])==23
assert len(review['key_sector_groups'])==4
for r in d['maps']+d['urban_legend']:
    assert hashlib.sha256((P/r['file']).read_bytes()).hexdigest()==r['sha256']
    assert Image.open(P/r['file']).size==(r['width'],r['height'])
    assert r['crs'] is None
assert {r['pdf_page'] for r in d['maps']}=={12,20,22,30}
assert len({r['label'] for r in d['urban_legend']})==23
assert review['key_sector_groups'][0]['map_local_legend']['hatching']!=review['key_sector_groups'][1]['map_local_legend']['hatching']
assert not review['geography']['pixel_shapes_extracted'] and review['geography']['new_sectors_delivered']==0
manifest=json.loads((P.parents[2]/'document_reviews.json').read_text())
m=next(r for r in manifest if r['document_id']==d['document_id']);assert m['sha256']==d['source_sha256']
assert {r['page_number'] for r in m['map_pages']}=={12,20,22,30}
assert m['geographic_validation']=='pending' and m['receiver_release']=='not_ready'
if args.pdf:
    before=(P/'inventory.json').read_bytes()
    subprocess.run([sys.executable,str(P/'extract.py'),str(args.pdf)],check=True)
    assert (P/'inventory.json').read_bytes()==before
print('10 maps, 23 symbols, source provenance, local legends and unreleased research state verified.')
