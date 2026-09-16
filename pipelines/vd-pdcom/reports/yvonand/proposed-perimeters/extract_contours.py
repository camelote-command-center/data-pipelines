"""Reproduce reviewed source cadastral contours; these are not whole PDCom sectors."""
import pathlib,json,math,hashlib
import pymupdf as f
from shapely.geometry import Polygon,mapping
out=pathlib.Path(__file__).resolve().parent;pdf=out.parent/'source-review/source-7.pdf';sha='d4fecb86752e2f2cb2c3359e835e9555694979b88288bb50795080ec349a1a2b';assert hashlib.sha256(pdf.read_bytes()).hexdigest()==sha
page=f.open(pdf)[0];drawings=page.get_drawings(extended=True);selected=[(4502,4),(4536,4),(4552,2),(4569,2),(4576,1),(4576,2),(4586,0),(4588,5)];rows=[]
for idx,sub in selected:
 d=drawings[idx];assert d['type']=='s' and d['color']==(0,0,0);rings=[];pts=[]
 for it in d['items']:
  if it[0]!='l':
   if pts:rings.append(pts);pts=[]
   continue
  a=list(it[1]*page.rotation_matrix);b=list(it[2]*page.rotation_matrix)
  if pts and math.dist(pts[-1],a)>.001:rings.append(pts);pts=[]
  if not pts:pts=[a]
  pts.append(b)
 if pts:rings.append(pts)
 pts=rings[sub];assert math.dist(pts[0],pts[-1])<.001;g=Polygon(pts);assert g.is_valid and g.area>2000
 rows.append({'drawing_index':idx,'subpath':sub,'area':g.area,'geometry':mapping(g)})
obj={'source_sha256':sha,'coordinate_system':'PDF rotated display points,not geography','selection':'Eight visually reviewed native cadastral subpaths bearing proposed hatching; non-exhaustive and not exact proposed-area perimeters.','contours':rows};(out/'source-contours.json').write_text(json.dumps(obj,indent=2));print('verified source contours',len(rows))
