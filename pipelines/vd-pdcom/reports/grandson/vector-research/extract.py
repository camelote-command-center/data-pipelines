"""Preserve native Bellevue/Borne-Nau paint paths; PDF coordinates only."""
import pathlib,json,hashlib,pymupdf
from shapely.geometry import Polygon
p=pathlib.Path(__file__).resolve().parent
f=p.parent/'source-review/07.pdf';b=f.read_bytes();sha='1f349cf0dcf2d9f5fb7e13ccddaee37fa041c223c4c95834fef990c9765a360a';assert hashlib.sha256(b).hexdigest()==sha
d=pymupdf.open(stream=b,filetype='pdf');q=d[0];drawings=q.get_drawings();rotation=q.rotation;q.set_rotation(0);rows=[]
for index in [0,1,64,97]:
 x=drawings[index];commands=[];points=[];shape=q.new_shape()
 for item in x['items']:
  kind=item[0];assert kind in ['l','c'];ps=[list(v) for v in item[1:]];commands.append([kind,*ps])
  if not points:points.append(ps[0])
  assert abs(points[-1][0]-ps[0][0])+abs(points[-1][1]-ps[0][1])<.01
  if kind=='l':points.append(ps[1]);shape.draw_line(*item[1:])
  else:
   shape.draw_bezier(*item[1:])
   for n in range(1,33):
    t=n/32;points.append([(1-t)**3*ps[0][axis]+3*(1-t)**2*t*ps[1][axis]+3*(1-t)*t*t*ps[2][axis]+t**3*ps[3][axis] for axis in [0,1]])
 poly=Polygon(points);assert poly.is_valid
 shape.finish(color=(1,0,1),width=1.5,closePath=True);shape.commit()
 r=x['rect'];q.insert_text((r.x0,r.y0),str(index),fontsize=12,color=(1,0,1))
 rows.append({'drawing_index':index,'seqno':x['seqno'],'fill':list(x['fill']),'bbox':list(x['rect']),'fill_rule':'evenodd' if x['even_odd'] else 'nonzero','native_commands':commands,'sampled_area_pdf_points_squared':poly.area,'sampled_valid':poly.is_valid,'approximation':'32 subdivisions per cubic for diagnostic area only; native cubic commands retained exactly','semantic':'underlying colored paint contour, not net developable perimeter'})
q.set_rotation(rotation);q.get_pixmap(dpi=100).save(p/'overlay.png');d.save(p/'overlay.pdf')
result={'source_sha256':sha,'source_page':1,'page_rotation':rotation,'coordinate_space':'unrotated PDF points, x right/y down; no geographic transform','paths':rows,'limits':['Manual source selection tied to visible white hatch overpainting; not a generic classifier.','Blue path1 overpaints part of orange path0; raw areas must not be summed.','No clipping, later symbol overpainting or land-use deductions applied.','Indicative historical zoning intentions; currentness, exact approval and alignment unresolved.']}
(p/'contours.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print([(x['drawing_index'],x['sampled_area_pdf_points_squared']) for x in rows])
