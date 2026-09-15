"""Offline reproduction of all30 Morges sector/buffer intersection sets."""
import json,pathlib
from shapely.geometry import shape
p=pathlib.Path(__file__).resolve().parent;r=json.loads((p/'parcel-review.json').read_text());features=json.loads((p/'official-parcels.geojson').read_text())['features'];parcels={f['properties']['EGRID']:shape(f['geometry']) for f in features};assert len(parcels)==len(features)==r['reference_count']==399
assert all(g.is_valid for g in parcels.values());gs={str(f['properties']['drawing_index']):shape(f['geometry']) for f in json.loads((p.parent/'alignment/full-hatch-lv95.geojson').read_text())['features']}
for name,base in gs.items():
 for offset in [-10,-5,0,5,10]:
  g=base if offset==0 else base.buffer(offset)
  actual={e:g.intersection(pg).area for e,pg in parcels.items() if g.intersection(pg).area>0};expected={x['egrid']:x['overlap_m2'] for x in r['sectors'][name][str(offset)]['pairs']};assert actual.keys()==expected.keys();assert max(abs(actual[e]-expected[e]) for e in actual)<1e-7
rows=[x for v in r['sectors'].values() for x in v['0']['pairs']];assert len(rows)==62 and len({x['egrid'] for x in rows})==61
print('All30 intersection sets/areas and62pairs/61EGRIDs reproduced')
