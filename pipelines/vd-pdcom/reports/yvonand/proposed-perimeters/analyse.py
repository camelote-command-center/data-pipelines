import json,pathlib,hashlib
from shapely.geometry import shape,mapping,MultiPoint
from shapely.affinity import affine_transform
from shapely.ops import unary_union
root=pathlib.Path(__file__).resolve().parent.parent;out=root/'proposed-perimeters';j=json.load(open(out/'source-contours.json'));alignment=json.load(open(root/'boundary-research/exploratory-alignment.json'));sc=alignment['scale_metres_per_pdf_point'];tx,ty=alignment['translation'];sg={r['drawing_index']:shape(r['geometry_pdf_display_points']) for r in json.load(open(root/'boundary-research/black-path-candidates.json'))['candidates']};checks=[r for r in alignment['matches'] if r['role']=='reserved'];points=[affine_transform(sg[r['source_drawing_index']],[sc,0,0,-sc,tx,ty]).centroid for r in checks];hull=MultiPoint(points).convex_hull;features=[]
for r in j['contours']:
 g=affine_transform(shape(r['geometry']),[sc,0,0,-sc,tx,ty]);assert g.is_valid
 local=[check for check,pt in zip(checks,points) if g.distance(pt)<=300];r['local_reserved_checks_300m']=local;r['outside_reserved_hull_m2']=g.difference(hull).area;r['area_m2']=g.area;features.append({'type':'Feature','properties':{k:v for k,v in r.items() if k!='geometry'},'geometry':mapping(g)})
geo={'type':'FeatureCollection','crs':{'type':'name','properties':{'name':'EPSG:2056'}},'features':features};(out/'research-contours-lv95.geojson').write_text(json.dumps(geo,indent=2));bbox=unary_union([shape(x['geometry']) for x in features]).bounds
refs=json.load(open(out/'current-parcels.geojson'))['features'];receipt=json.load(open(out/'reference-receipt.json'))
assert hashlib.sha256((out/'current-parcels.geojson').read_bytes()).hexdigest()==receipt['file_sha256']
assert len(refs)==receipt['count']==len({x['properties']['EGRID'] for x in refs})
for r in refs:assert shape(r['geometry']).is_valid and r['properties']['NO_COM_FED']==5939
pairs=[]
for f in features:
 g=shape(f['geometry']);prop=f['properties'];key=f"{prop['drawing_index']}/{prop['subpath']}"
 for r in refs:
  h=shape(r['geometry']);area=g.intersection(h).area
  if area>1:pairs.append({'source_contour':key,'egrid':r['properties']['EGRID'],'numero':r['properties']['NUMERO'],'kind':r['properties']['GENRE_TXT'],'overlap_m2':area,'current_parcel_area_m2':h.area,'fraction_current_parcel':area/h.area})
 print(key,'area',round(g.area),'controls',len(prop['local_reserved_checks_300m']),'outsidehull',round(prop['outside_reserved_hull_m2'],2))
(out/'research-intersections.json').write_text(json.dumps({'limits':'Research overlap of historical cadastral contours carrying proposed hatching; NOT net developable land or complete PDCom sectors. >1m2 threshold; all slivers retained above threshold.','pairs':pairs},indent=2));print('pairs',len(pairs),'egrids',len({p['egrid'] for p in pairs}))
