"""Recheck frozen official-reference intersections without database mutation."""
import pathlib,json
from shapely.geometry import shape
root=pathlib.Path(__file__).resolve().parent;r=json.loads((root/'variants.json').read_text());fc=json.loads((root/'official-parcels.geojson').read_text());qa=json.loads((root/'official-parcel-qa.json').read_text());assert len(fc['features'])==qa['count'];results={}
for v in r['features']:
 radius=v['properties']['closing_radius_pixels']
 for distance in ([0,-10,-5,5,10] if radius==2 else [0]):
  g=shape(v['geometry']).buffer(distance);hits=[]
  for p in fc['features']:
   pg=shape(p['geometry']);x=g.intersection(pg)
   if x.area>0:hits.append(p['properties']['EGRID'])
  key=f'radius{radius}_buffer{distance}';assert hits==qa['variants'][key]['egrid_ids'];results[key]=len(hits)
print(json.dumps(results))
