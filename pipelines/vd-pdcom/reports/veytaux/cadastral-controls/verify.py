from pathlib import Path
import json,hashlib
from shapely.geometry import Polygon
p=Path(__file__).resolve().parent
for row in json.loads((p/'reference-manifest.json').read_text()):
 raw=(p/row['file']).read_bytes();assert hashlib.sha256(raw).hexdigest()==row['sha256'];j=json.loads(raw)
 assert len(j['features'])==row['count'] and not j.get('exceededTransferLimit')
 assert len({v['attributes']['OBJECTID'] for v in j['features']})==row['count']
 for f in j['features']:
  polygons=[Polygon(r) for r in f['geometry']['rings']];assert all(g.is_valid for g in polygons)
  g=polygons[0]
  for other in polygons[1:]:g=g.symmetric_difference(other)
  assert g.is_valid and g.area>0
findings=json.loads((p/'findings.json').read_text());assert findings['historical_numeric_consistency']['difference_m2']==2898
assert findings['geographic_validation']=='pending' and findings['receiver_release'] is False
print('Frozen source counts/identities/hashes/ring geometry and research gates verified')
