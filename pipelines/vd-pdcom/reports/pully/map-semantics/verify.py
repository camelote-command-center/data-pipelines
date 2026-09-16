from pathlib import Path
import json,hashlib
from PIL import Image,ImageChops
p=Path(__file__).resolve().parent
for row in json.loads((p/'artifact-manifest.json').read_text())['files']:
 data=(p/row['file']).read_bytes();assert len(data)==row['bytes'];assert hashlib.sha256(data).hexdigest()==row['sha256']
s=json.loads((p/'semantics.json').read_text());assert hashlib.sha256((p/'../source-review/source.pdf').read_bytes()).hexdigest()==s['source_sha256']
ids=[]
for legend in s['legends']:
 ids.extend(x['id'] for x in legend['entries'])
 with Image.open(p/legend['map_source']) as im, Image.open(p/legend['legend_crop']) as crop:
  assert ImageChops.difference(im.crop(legend['crop_box_in_3x_display_pixels']),crop).getbbox() is None
assert len(ids)==len(set(ids))==44
assert all(x in ids for c in s['crosswalks'] for x in c['legend_ids'])
ctx=json.loads((p/'boverattes-context.json').read_text());assert ctx['receiver_release']=='not_ready';assert len(set(ctx['historical_parcel_scope']['numbers']))==8
for row in ctx['source_files']:
 file=row['name']+('.pdf' if row['name'].startswith('boverattes') else '.html')
 assert hashlib.sha256((p/file).read_bytes()).hexdigest()==row['sha256']
print('Verified 44 legend entries, 8 thematic crosswalks, source/crop/artifact hashes and adjacent-plan provenance. No geography released.')
