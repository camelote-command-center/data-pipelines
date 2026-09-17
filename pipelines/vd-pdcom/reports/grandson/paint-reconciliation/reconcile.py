"""Resolve ordering among four selected hatch-support paints, PDF space only."""
import pathlib,json,hashlib,itertools,pymupdf
from shapely.geometry import Polygon,mapping
from shapely.ops import unary_union
p=pathlib.Path(__file__).resolve().parent;src=p.parent/'vector-research/contours.json';data=json.loads(src.read_text());pdf=p.parent/'source-review/07.pdf';assert hashlib.sha256(pdf.read_bytes()).hexdigest()==data['source_sha256']
def polygon(row,n):
 pts=[]
 for kind,*ps in row['native_commands']:
  if not pts:pts.append(ps[0])
  if kind=='l':pts.append(ps[1])
  else:
   assert kind=='c'
   for k in range(1,n+1):
    t=k/n;pts.append([(1-t)**3*ps[0][j]+3*(1-t)**2*t*ps[1][j]+3*(1-t)*t*t*ps[2][j]+t**3*ps[3][j] for j in [0,1]])
 g=Polygon(pts);assert g.is_valid;return g
runs=[]
for n in [16,32,64,128]:
 raw=[polygon(x,n) for x in data['paths']];pieces=[]
 for i,g in enumerate(raw):pieces.append(g.difference(unary_union(raw[i+1:])))
 union=unary_union(raw);out=unary_union(pieces);assert union.symmetric_difference(out).area<1e-8
 overlaps=[pieces[i].intersection(pieces[j]).area for i,j in itertools.combinations(range(4),2)];assert max(overlaps)<1e-8
 assert all(g.is_valid for g in pieces)
 runs.append({'cubic_subdivisions':n,'raw_sum':sum(g.area for g in raw),'union_area':union.area,'double_count_removed':sum(g.area for g in raw)-union.area,'partition_areas':[g.area for g in pieces],'max_pair_overlap':max(overlaps),'union_difference':union.symmetric_difference(out).area})
result={'source_sha256':data['source_sha256'],'contours_sha256':hashlib.sha256(src.read_bytes()).hexdigest(),'units':'square unrotated PDF points, not square metres','method':'Subtract only later selected paint paths in source order 0,1,64,97; cubic sampling sensitivity retained; no snapping/repair/buffering','runs':runs,'partition':[{'source_index':row['drawing_index'],'fill':row['fill'],'geometry_pdf_coordinates':mapping(g)} for row,g in zip(data['paths'],pieces)],'limits':['Only the four selected hatch-support paints are reconciled.','Later roads, symbols, green/solid overpainting and other map content are not deducted.','Not net buildable area, not four confirmed opportunities.','No geographic alignment or parcel association; historical/currentness/approval limits remain.']}
(p/'results.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
d=pymupdf.open();page=d.new_page(width=842,height=1191)
for row,g in zip(data['paths'],pieces):
 for poly in ([g] if g.geom_type=='Polygon' else g.geoms):
  sh=page.new_shape();sh.draw_polyline(list(poly.exterior.coords))
  for ring in poly.interiors:sh.draw_polyline(list(ring.coords))
  sh.finish(fill=row['fill'],color=(0,0,0),width=.7,even_odd=True,closePath=True);sh.commit()
page.set_rotation(90);page.get_pixmap(dpi=90).save(p/'partition.png');d.save(p/'partition.pdf');print(json.dumps(runs,indent=2))
