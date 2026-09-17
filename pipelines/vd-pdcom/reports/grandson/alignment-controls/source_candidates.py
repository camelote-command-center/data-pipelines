"""Prepare historical gray-fill candidates; does not fit geographic coordinates."""
import pathlib,json,hashlib,pymupdf
from shapely.geometry import Polygon,mapping
p=pathlib.Path(__file__).resolve().parent;f=p.parent/'source-review/07.pdf';sha=hashlib.sha256(f.read_bytes()).hexdigest();assert sha=='1f349cf0dcf2d9f5fb7e13ccddaee37fa041c223c4c95834fef990c9765a360a'
d=pymupdf.open(f);q=d[0];rows=[];excluded=[];palette=(0.7960784435272217,0.800000011920929,0.800000011920929);box=pymupdf.Rect(40,40,895,815)
for idx,path in enumerate(q.get_drawings()):
 if path['fill']!=palette:continue
 rect=path['rect']*q.rotation_matrix
 if not box.contains(rect):excluded.append({'drawing_index':idx,'reason':'Outside conservative display map rectangle or touches its edge','display_bbox':list(rect)});continue
 points=[];error=None
 for item in path['items']:
  kind=item[0]
  if kind not in ['l','c']:error='unsupported native command '+kind;break
  ps=[list(x) for x in item[1:]]
  if not points:points.append(ps[0])
  if abs(points[-1][0]-ps[0][0])+abs(points[-1][1]-ps[0][1])>.01:error='multiple/discontinuous subpaths';break
  if kind=='l':points.append(ps[1])
  else:
   for n in range(1,33):
    t=n/32;points.append([(1-t)**3*ps[0][j]+3*(1-t)**2*t*ps[1][j]+3*(1-t)*t*t*ps[2][j]+t**3*ps[3][j] for j in [0,1]])
 if error:excluded.append({'drawing_index':idx,'reason':error});continue
 g=Polygon(points)
 if not g.is_valid or g.area==0:excluded.append({'drawing_index':idx,'reason':'invalid or empty polygon'});continue
 rows.append({'drawing_index':idx,'geometry_pdf_coordinates':mapping(g),'centroid_pdf':list(g.centroid.coords[0]),'area_pdf_points_squared':g.area,'display_bbox':list(rect)})
r={'source_sha256':sha,'page':1,'native_rotation':q.rotation,'palette':palette,'display_map_rectangle':list(box),'candidate_count':len(rows),'excluded_count':len(excluded),'candidates':rows,'excluded':excluded,'limits':['Gray fills are historical candidate footprints, not matched building identities.','Conservative map rectangle excludes border objects; coverage is not exhaustive.','Cubic curves sampled at32segments; invalid/discontinuous/unsupported paths explicitly excluded.','No geographic transform, fitting, validation or runtime sector.']};(p/'source-candidates.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n');print('candidates',len(rows),'excluded',len(excluded));print(excluded[:8])
