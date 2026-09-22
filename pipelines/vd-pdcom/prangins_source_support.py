import numpy as np
from shapely.geometry import Polygon
from shapely.ops import unary_union
def flatten(p0,p1,p2,p3,depth=0):
 p0,p1,p2,p3=map(np.array,(p0,p1,p2,p3));v=p3-p0;length=np.linalg.norm(v)
 deviation=max(abs((v[0]*(p1-p0)[1]-v[1]*(p1-p0)[0])),abs((v[0]*(p2-p0)[1]-v[1]*(p2-p0)[0])))/length if length>1e-9 else max(np.linalg.norm(p1-p0),np.linalg.norm(p2-p0))
 if deviation<=.03:return [p0.tolist(),p3.tolist()]
 if depth>=20:raise ValueError('curve_flatten_limit')
 q0=(p0+p1)/2;q1=(p1+p2)/2;q2=(p2+p3)/2;r0=(q0+q1)/2;r1=(q1+q2)/2;mid=(r0+r1)/2
 return flatten(p0,q0,r0,mid,depth+1)[:-1]+flatten(mid,r1,q2,p3,depth+1)

def polygon(items):
 pts=[]
 for it in items:
  if it[0] not in ('l','c'):raise ValueError('unreviewed_path_operator')
  start=list(it[1]);end=list(it[-1])
  if pts and pts[-1]!=start:raise ValueError('multiple_contours_require_review')
  if not pts:pts.append(start)
  pts.extend([end] if it[0]=='l' else flatten(*[list(p) for p in it[1:]])[1:])
 g=Polygon(pts)
 if not g.is_valid:raise ValueError('invalid_source_polygon')
 return g

def compound(items,even_odd):
 parts=[];part=[];end=None
 for it in items:
  if it[0] not in ('l','c'):raise ValueError('unsupported_compound_operator')
  if part and list(it[1])!=end:parts.append(part);part=[]
  part.append(it);end=list(it[-1])
 if part:parts.append(part)
 rings=[polygon(it) for it in parts]
 if any(r.is_empty or not r.is_valid or r.area<=0 for r in rings):raise ValueError('invalid_source_ring')
 from shapely.ops import polygonize
 cells=list(polygonize(unary_union([r.boundary for r in rings])))
 signs=[1 if r.exterior.is_ccw else -1 for r in rings];selected=[]
 for cell in cells:
  at=cell.representative_point();inside=[i for i,r in enumerate(rings) if r.contains(at)]
  if (len(inside)%2 if even_odd else sum(signs[i] for i in inside)!=0):selected.append(cell)
 return unary_union(selected)
