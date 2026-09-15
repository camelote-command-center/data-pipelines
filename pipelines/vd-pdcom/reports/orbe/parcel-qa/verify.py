"""Offline reproduction of all frozen official intersection sets."""
import json,pathlib
from shapely.geometry import shape
p=pathlib.Path(__file__).resolve().parent;r=json.loads((p/'parcel-review.json').read_text());parcels={f['properties']['EGRID']:shape(f['geometry']) for f in json.loads((p/'official-parcels.geojson').read_text())['features']};gs={f['properties']['boundary_variant']:shape(f['geometry']) for f in json.loads((p.parent/'alignment/priority-band-lv95.geojson').read_text())['features']};gs.update(inner_minus5m=gs['inner'].buffer(-5),outer_plus5m=gs['outer'].buffer(5),inner_minus10m=gs['inner'].buffer(-10),outer_plus10m=gs['outer'].buffer(10))
for name,g in gs.items():
 actual={e:g.intersection(pg).area for e,pg in parcels.items() if g.intersection(pg).area>0};expected={x['egrid']:x['overlap_m2'] for x in r['variants'][name]['pairs']};assert actual.keys()==expected.keys();assert max(abs(actual[e]-expected[e]) for e in actual)<1e-7
print('All seven intersection sets and areas reproduced')
