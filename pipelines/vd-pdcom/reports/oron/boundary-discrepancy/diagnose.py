import pathlib,json,numpy as np,pymupdf
from shapely.geometry import shape,mapping,Polygon
p=pathlib.Path(__file__).resolve().parent.parent;q=p/'boundary-discrepancy';q.mkdir(exist_ok=True)
r=json.loads((p/'research-geometries/review.json').read_text());g=shape(r['features'][2]['geometry']);b=shape(json.loads((p/'official-controls/commune-boundary.json').read_text())['geometry']);out=g.difference(b);inv=np.linalg.inv(np.array(r['composed_pdf77_to_lv95']))
def rings(geom):
 for part in getattr(geom,'geoms',[geom]):
  if part.geom_type=='Polygon':yield list(part.exterior.coords)
def pdfxy(ring):return (np.c_[np.array(ring),np.ones(len(ring))]@inv.T)[:,:2]
parts=[]
for i,part in enumerate(getattr(out,'geoms',[out])):
 coords=list(part.exterior.coords);parts.append({'id':i,'area_m2':part.area,'bounds_lv95':part.bounds,'geometry':mapping(part),'source_pdf77_ring':pdfxy(coords).tolist(),'max_vertex_distance_to_boundary_m':max(__import__('shapely').geometry.Point(x).distance(b) for x in coords)})
result={'source_sha256':r['source_sha256'],'status':'unresolved_research_only','outside_total_m2':out.area,'outside_fraction':out.area/g.area,'parts':parts,'method':'Frozen prior geometry minus current registered commune boundary; inverse frozen matrix solely for visual inspection. No clipping, fit update or runtime mutation.','limits':['Maximum vertex distance is a diagnostic of polygon vertices, not a continuous Hausdorff distance.','Source proposal, manual tracing and registration uncertainty remain; discrepancy alone establishes no rights or municipal attribution.']}
(q/'diagnostic.json').write_text(json.dumps(result,indent=2))
for mode in ['source','overlay']:
 d=pymupdf.open(str(p/'recovered-source/report.pdf'));pg=d[76]
 if mode=='overlay':
  for geom,col in [(b,(0,.5,1)),(g,(1,0,1)),(out,(1,.45,0))]:
   for ring in rings(geom):pg.draw_polyline([pymupdf.Point(*x) for x in pdfxy(ring)],color=col,width=.35)
 xy=pdfxy(list(g.exterior.coords));pg.get_pixmap(matrix=pymupdf.Matrix(7,7),clip=pymupdf.Rect(*(xy.min(axis=0)-5),*(xy.max(axis=0)+5))).save(q/(mode+'.png'))
print({'outside_m2':out.area,'parts':[(x['area_m2'],x['max_vertex_distance_to_boundary_m']) for x in parts]})
