import pathlib,json,numpy as np
from shapely.geometry import Polygon,shape,mapping,MultiPoint
from shapely.ops import unary_union
p=pathlib.Path(__file__).resolve().parent.parent;q=p/'research-geometries';q.mkdir(exist_ok=True)
a=np.vstack([json.loads((p/'intermap-registration/algorithm-result.json').read_text())['affine_pdf77_to_pdf177'],[0,0,1]]);b=np.vstack([json.loads((p/'map-registration-audit/review.json').read_text())['affine_x_y_1_to_e_n'],[0,0,1]]);composed=b@a
boundary=shape(json.loads((p/'official-controls/commune-boundary.json').read_text())['geometry']);matches=json.loads((p/'intermap-registration/matches.json').read_text());good=[m for m in matches if m['role']=='check' and m['error_target_pixels']<=3];hull=MultiPoint([np.array(m['source_pixel'])/2 for m in good]).convex_hull
refs=[]
for f in json.loads((p/'official-controls/footprints.json').read_text())['features']:
 if f['attributes']['GENRE_TXT']!='bâtiment':continue
 g=Polygon(f['geometry']['rings'][0],f['geometry']['rings'][1:]);assert g.is_valid;refs.append((f['attributes'],g))
rows=[]
for f in json.loads((p/'densification-candidates/traces.json').read_text())['features']:
 source=Polygon(f['ring']);xy=np.c_[np.array(f['ring']),np.ones(len(f['ring']))]@composed.T;g=Polygon(xy[:,:2]);assert g.is_valid
 overlap=[{'objectid':attrs['OBJECTID'],'egid':attrs['EGID'],'overlap_m2':g.intersection(h).area} for attrs,h in refs if g.intersection(h).area>1]
 buffers=[{'illustrative_m':n,'aboveground_overlap_gt1m2_count':sum(g.buffer(n).intersection(h).area>1 for _,h in refs)} for n in [-10,0,10]]
 rows.append({'id':f['id'],'crs':'EPSG:2056','geometry':mapping(g),'provisional_area_m2':g.area,'commune_interior_fraction':g.intersection(boundary).area/g.area,'outside_accepted_image_check_hull_fraction':source.difference(hull).area/source.area,'aboveground_matches':overlap,'sensitivity':buffers,'status':'withheld_research_only'})
r={'source_sha256':'f6c73677a1940ff8529d795c60f375b2270a5f124ad946bf6bb14ca987eb7680','composed_pdf77_to_lv95':composed.tolist(),'features':rows,'method':'Compose frozen PR139 image-to-image affine with frozen PR137 environment-map boundary transform; no refit or datum conversion.','limits':['Geometries inherit unresolved ground error, generalisation and manual trace uncertainty.','Image checks are not ground controls; hull coverage does not prove positional accuracy.','Areas are illustrative gross trace areas, not net buildable land or floor-area rights.','Building overlap counts are research candidates, not parcel allocations or capacity.','Plus/minus10m buffers are illustrative, not confidence intervals.','Consultation version with long-term PACom conditions; no current building rights.'],'delivery':'withheld_research_only'}
(q/'composition-result.json').write_text(json.dumps(r,indent=2));print([(x['id'],round(x['provisional_area_m2']),x['commune_interior_fraction'],x['outside_accepted_image_check_hull_fraction'],[b['aboveground_overlap_gt1m2_count'] for b in x['sensitivity']]) for x in rows])
