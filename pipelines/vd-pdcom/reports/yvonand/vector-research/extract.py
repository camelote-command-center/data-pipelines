"""Extract auditable PDF paint paths, not sectors or geographic coordinates."""
import argparse,hashlib,json,math,collections
from pathlib import Path
import pymupdf as fitz
import numpy as np
from shapely.geometry import Polygon,box,mapping
from shapely.ops import unary_union,transform
from PIL import Image,ImageDraw
SHA='d4fecb86752e2f2cb2c3359e835e9555694979b88288bb50795080ec349a1a2b'
ap=argparse.ArgumentParser();ap.add_argument('--pdf',required=True);ap.add_argument('--out',required=True);a=ap.parse_args();out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
assert hashlib.sha256(Path(a.pdf).read_bytes()).hexdigest()==SHA
page=fitz.open(a.pdf)[0];drawings=page.get_drawings(extended=True)
def curve(p0,p1,p2,p3,depth=0):
 p0,p1,p2,p3=map(np.array,(p0,p1,p2,p3));v=p3-p0;n=np.linalg.norm(v)
 e=max(abs(np.cross(v,p1-p0)),abs(np.cross(v,p2-p0)))/n if n>1e-9 else max(np.linalg.norm(p1-p0),np.linalg.norm(p2-p0))
 if e<=.03:return [p0.tolist(),p3.tolist()]
 if depth>=20:raise ValueError('curve_depth')
 q0=(p0+p1)/2;q1=(p1+p2)/2;q2=(p2+p3)/2;r0=(q0+q1)/2;r1=(q1+q2)/2;m=(r0+r1)/2
 return curve(p0,q0,r0,m,depth+1)[:-1]+curve(m,r1,q2,p3,depth+1)
def shape(d):
 rings=[];pts=[]
 for it in d['items']:
  if it[0]=='re':
   if pts:rings.append(pts);pts=[]
   r=it[1];rings.append([[r.x0,r.y0],[r.x1,r.y0],[r.x1,r.y1],[r.x0,r.y1]]);continue
  if it[0] not in ('l','c'):raise ValueError('operator_'+it[0])
  start=list(it[1]);end=list(it[-1])
  if pts and math.dist(pts[-1],start)>1e-4:rings.append(pts);pts=[]
  if not pts:pts=[start]
  pts.extend([end] if it[0]=='l' else curve(*[list(p) for p in it[1:]])[1:])
 if pts:rings.append(pts)
 polys=[Polygon(p) for p in rings if len(p)>=3]
 if any(not p.is_valid for p in polys):raise ValueError('invalid_contour_no_repair')
 if len(polys)>1 and not d['even_odd']:raise ValueError('nonzero_multicontour_needs_winding_review')
 g=Polygon()
 for p in polys:g=g.symmetric_difference(p) if d['even_odd'] else g.union(p)
 return g
# Exclusions are in rotated display points, conservative page-furniture rectangles.
exclusions=[(0,0,390,705),(1550,0,2200,400),(1940,1460,2384,1684)]
mask=box(*page.rect).difference(unary_union([box(*r) for r in exclusions]));matrix=page.rotation_matrix
selected=[];rejected=[];clipped=[];stack=[]
colors={'orange':(1,.501991,0),'yellow':(1,1,0),'purple':(.501991,0,.501991),'cyan':(0,1,1)}
for idx,d in enumerate(drawings):
 stack=[s for s in stack if s['level']<d['level']]
 if d['type'] in ('clip','group'):stack.append(d);continue
 rgb=d.get('fill');label=next((k for k,c in colors.items() if rgb and max(abs(x-y) for x,y in zip(c,rgb))<.001),None)
 if label is None:continue
 rec={'drawing_index':idx,'seqno':d['seqno'],'paint_color':label,'rgb':rgb,'even_odd':d['even_odd'],'opacity':d['fill_opacity'],'clip_rectangles':[],'subsequent_overpainting':'not_subtracted; white hatches/buildings/roads may overlay this paint'}
 rec['source_path_items']=[[it[0]]+[list(v) if isinstance(v,(fitz.Point,fitz.Rect,fitz.Quad)) else v for v in it[1:]] for it in d['items']]
 try:
  g=shape(d);rec['raw_area_points2']=g.area
  for c in stack:
   if c['type']=='group':
    if c['blendmode']!='Normal' or c['opacity']!=1:raise ValueError('unreviewed_group')
   else:
    if len(c['items'])!=1 or c['items'][0][0]!='re':raise ValueError('nonrectangular_clip')
    r=list(c['items'][0][1]);rec['clip_rectangles'].append(r);g=g.intersection(box(*r))
  # PyMuPDF get_drawings is unrotated even when page.rect is rotated.
  g=transform(lambda x,y:(matrix.a*np.asarray(x)+matrix.c*np.asarray(y)+matrix.e,matrix.b*np.asarray(x)+matrix.d*np.asarray(y)+matrix.f),g).intersection(mask)
  if not g.is_valid:raise ValueError('invalid_clipped_no_repair')
  rec['display_area_points2']=g.area
  if g.is_empty or g.area==0:clipped.append(rec);continue
  rec['geometry_pdf_display_points']=mapping(g);selected.append(rec)
 except ValueError as e:rec['reason']=str(e);rejected.append(rec)
result={'source_sha256':SHA,'source_document_id':'b8fc92d6-c4a6-5a3c-8fee-dfa2b11de848','page_number':1,'coordinate_system':'ROTATED PDF display points, top-left origin; NOT geographic coordinates','page_rotation':page.rotation,'display_size':list(page.rect),'curve_tolerance_points':.03,'excluded_page_furniture':exclusions,'semantic_status':'paint paths only; existing/projected/hatching and final perimeter unresolved','geographic_status':'not_georeferenced','selected':selected,'clipped_out':clipped,'rejected':rejected}
(out/'paint-paths.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
scale=.7;px=page.get_pixmap(matrix=fitz.Matrix(scale,scale));im=Image.frombytes('RGB',[px.width,px.height],px.samples).convert('RGBA');overlay=Image.new('RGBA',im.size);dr=ImageDraw.Draw(overlay)
for rec in selected:
 g=rec['geometry_pdf_display_points'];polys=[g['coordinates']] if g['type']=='Polygon' else g['coordinates'] if g['type']=='MultiPolygon' else []
 for poly in polys:
  dr.line([(x*scale,y*scale) for x,y in poly[0]],fill=(0,40,255,190),width=2)
Image.alpha_composite(im,overlay).convert('RGB').save(out/'paint-path-overlay.jpg',quality=92)
print(json.dumps({'selected':len(selected),'clipped_out':len(clipped),'rejected':len(rejected),'colors':dict(collections.Counter(x['paint_color'] for x in selected)),'reasons':dict(collections.Counter(x['reason'] for x in rejected))}))
