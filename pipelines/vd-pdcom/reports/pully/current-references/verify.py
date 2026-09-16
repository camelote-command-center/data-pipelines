from pathlib import Path
import hashlib,json
from shapely.geometry import shape
p=Path(__file__).resolve().parent
for row in json.loads((p/'manifest.json').read_text())['files']:assert hashlib.sha256((p/row['file']).read_bytes()).hexdigest()==row['sha256']
a=json.loads((p/'current-named-parcels.geojson').read_text());b=json.loads((p/'current-footprints.geojson').read_text());r=json.loads((p/'reference-receipt.json').read_text());f=json.loads((p/'footprints-receipt.json').read_text())
assert len(a['features'])==r['count_check']['count']==8;assert len(b['features'])==f['count']==57
assert hashlib.sha256((p/'current-named-parcels.geojson').read_bytes()).hexdigest()==r['response_sha256'];assert hashlib.sha256((p/'current-footprints.geojson').read_bytes()).hexdigest()==f['response_sha256']
assert {x['properties']['NUMERO'] for x in a['features']}=={'1738','1743','1744','1755','1756','2034','3420','3421'}
assert len({x['properties']['EGRID'] for x in a['features']})==8;assert all(x['properties']['NO_COM_FED']==5590 for x in a['features'])
assert len({x['properties']['OBJECTID'] for x in b['features']})==57
assert all(shape(x['geometry']).is_valid and not shape(x['geometry']).is_empty for x in a['features']+b['features'])
occ=json.loads((p/'footprint-occupancy.json').read_text())
for row in occ:
 parcel=next(x for x in a['features'] if x['properties']['EGRID']==row['egrid']);g=shape(parcel['geometry']);expected={}
 for x in b['features']:
  area=g.intersection(shape(x['geometry'])).area
  if area>1:expected[x['properties']['OBJECTID']]=area
 assert set(expected)=={x['objectid'] for x in row['footprints_over_1m2']}
 for x in row['footprints_over_1m2']:assert abs(expected[x['objectid']]-x['overlap_m2'])<1e-8
assert sum(len(x['footprints_over_1m2']) for x in occ)==21
print('Verified 8 current parcel-number matches, 57 reference footprints and 21 intersections >1m². No historical lineage or capacity claim.')
