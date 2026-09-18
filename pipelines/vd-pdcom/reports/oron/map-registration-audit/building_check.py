import pathlib,json,pymupdf,numpy as np
from shapely.geometry import Polygon,shape
from shapely.strtree import STRtree
p=pathlib.Path(__file__).resolve().parent.parent;q=p/'map-registration-audit';r=json.loads((q/'review.json').read_text());a=np.array(r['affine_x_y_1_to_e_n']);commune=shape(json.loads((p/'official-controls/commune-boundary.json').read_text())['geometry']);refs=[];attrs=[]
for f in json.loads((p/'official-controls/footprints.json').read_text())['features']:
 if f['attributes']['GENRE_TXT']!='bâtiment':continue
 g=Polygon(f['geometry']['rings'][0],f['geometry']['rings'][1:]);refs.append(g);attrs.append(f['attributes'])
tree=STRtree([g.centroid for g in refs]);d=pymupdf.open(str(p/'recovered-source/report.pdf'));checks=[];skipped={}
for i,pa in enumerate(d[176].get_drawings()):
 if pa['fill'] is None or max(abs(c-.51) for c in pa['fill'])>.0001:continue
 if not all(x[0]=='l' for x in pa['items']):skipped['non_line']=skipped.get('non_line',0)+1;continue
 pts=[];disjoint=False
 for item in pa['items']:
  if pts and tuple(item[1])!=pts[-1]:disjoint=True;break
  if not pts:pts.append(tuple(item[1]))
  pts.append(tuple(item[2]))
 if disjoint or len(pts)<3:skipped['disjoint_or_short']=skipped.get('disjoint_or_short',0)+1;continue
 xy=np.c_[np.array(pts),np.ones(len(pts))]@a.T;g=Polygon(xy)
 if not g.is_valid or g.area<5:continue
 if not commune.covers(g.centroid):continue
 ix=int(tree.nearest(g.centroid));h=refs[ix];err=g.centroid.distance(h.centroid);iou=g.intersection(h).area/g.union(h).area
 checks.append({'path_index':i,'nearest_objectid':attrs[ix]['OBJECTID'],'centroid_distance_m':err,'iou':iou,'source_area_m2':g.area,'reference_area_m2':h.area})
summary={'candidate_count':len(checks),'skipped':skipped,'centroid_distance_median_m':float(np.median([x['centroid_distance_m'] for x in checks])),'median_iou':float(np.median([x['iou'] for x in checks])),'strong_count_iou_ge_085_distance_le_5':sum(x['iou']>=.85 and x['centroid_distance_m']<=5 for x in checks),'limits':'Automated nearest-centroid diagnostic on selected gray paths, not verified individual identities or training/refinement. All mismatches retained. No transfer to page77.'}
(q/'building-checks.json').write_text(json.dumps({'summary':summary,'checks':checks},indent=2));r['building_diagnostics']=summary;(q/'review.json').write_text(json.dumps(r,indent=2));print(summary)
