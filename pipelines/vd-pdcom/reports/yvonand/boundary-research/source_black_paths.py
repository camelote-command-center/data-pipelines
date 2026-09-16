import pymupdf as f,json,pathlib,math,hashlib
from shapely.geometry import Polygon,mapping,box
from shapely.ops import transform,unary_union
import numpy as np
root=pathlib.Path(__file__).resolve().parent.parent;assert hashlib.sha256((root/'source-review/source-7.pdf').read_bytes()).hexdigest()=='d4fecb86752e2f2cb2c3359e835e9555694979b88288bb50795080ec349a1a2b';p=f.open(root/'source-review/source-7.pdf')[0];m=p.rotation_matrix;ds=p.get_drawings(extended=True);mask=box(*p.rect).difference(unary_union([box(*r) for r in [(0,0,390,705),(1550,0,2200,400),(1940,1460,2384,1684)]]));records=[];stack=[]
for idx,d in enumerate(ds):
 stack=[s for s in stack if s['level']<d['level']]
 if d['type'] in ('clip','group'):stack.append(d);continue
 if d.get('fill')!=(0,0,0) or not all(it[0]=='l' for it in d['items']):continue
 pts=[]
 for it in d['items']:
  start=tuple(it[1]);end=tuple(it[2])
  if pts and math.dist(pts[-1],start)>.001:break
  if not pts:pts=[start]
  pts.append(end)
 else:
  if len(pts)<4:continue
  g=Polygon(pts)
  if not g.is_valid or not 3<g.area<1500:continue
  for c in stack:
   if c['type']=='clip':g=g.intersection(box(*c['scissor']))
  g=transform(lambda x,y:(m.a*np.asarray(x)+m.c*np.asarray(y)+m.e,m.b*np.asarray(x)+m.d*np.asarray(y)+m.f),g)
  if not mask.covers(g) or g.is_empty or g.geom_type!='Polygon':continue
  records.append({'drawing_index':idx,'geometry_pdf_display_points':mapping(g),'area_points2':g.area})
(root/'boundary-research/black-path-candidates.json').write_text(json.dumps({'limits':'Black paint candidates, includes symbols; NOT identified buildings. PDF display points only.','source_sha256':'d4fecb86752e2f2cb2c3359e835e9555694979b88288bb50795080ec349a1a2b','candidates':records}));print(len(records))
