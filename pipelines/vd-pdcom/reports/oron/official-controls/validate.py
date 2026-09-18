import pathlib,json,collections
from shapely.geometry import shape,Polygon
p=pathlib.Path(__file__).resolve().parent;boundary=shape(json.loads((p/'commune-boundary.json').read_text())['geometry']);assert boundary.is_valid
r=json.loads((p/'footprints.json').read_text());rows=[]
for f in r['features']:
 rings=[Polygon(x) for x in f['geometry']['rings']];assert all(x.is_valid for x in rings)
 g=rings[0]
 for h in rings[1:]:g=g.symmetric_difference(h)
 assert g.is_valid
 rows.append({'objectid':f['attributes']['OBJECTID'],'type':f['attributes']['GENRE_TXT'],'area_m2':g.area,'centroid_lv95':list(g.centroid.coords)[0],'commune_overlap_fraction':g.intersection(boundary).area/g.area})
summary={'valid_reference_features':len(rows),'types':dict(collections.Counter(x['type'] for x in rows)),'boundary_valid':True,'boundary_bounds_lv95':list(boundary.bounds),'zero_interior_overlap_ids':[x['objectid'] for x in rows if x['commune_overlap_fraction']==0],'partial_boundary_ids':[x['objectid'] for x in rows if 0<x['commune_overlap_fraction']<.999999],'limits':'Reference snapshot consistency only, not PDF alignment or source-vintage certification.'}
(p/'reference-validation.json').write_text(json.dumps({'summary':summary,'features':rows},indent=2));print(summary)
