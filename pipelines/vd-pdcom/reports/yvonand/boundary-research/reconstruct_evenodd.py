import pathlib,json,numpy as np,cv2
from shapely.geometry import LineString,mapping
from shapely.ops import unary_union,polygonize,transform
p=pathlib.Path(__file__).resolve().parent.parent;j=json.load(open(p/'vector-research/paint-paths.json'));results=[]
for r in j['rejected']:
 assert r['even_odd'] and all(it[0]=='l' for it in r['source_path_items'])
 items=r['source_path_items'];pts=[items[0][1]]
 for it in items:assert np.linalg.norm(np.array(pts[-1])-it[1])<.001;pts.append(it[2])
 if pts[-1]!=pts[0]:pts.append(pts[0])
 def odd(pt):
  x,y=pt.x,pt.y;crossings=0
  for (x1,y1),(x2,y2) in zip(pts,pts[1:]):
   if (y1>y)!=(y2>y) and x < x1+(y-y1)*(x2-x1)/(y2-y1):crossings+=1
  return crossings%2==1
 faces=list(polygonize(unary_union([LineString([a,b]) for a,b in zip(pts,pts[1:]) if a!=b])));chosen=[f for f in faces if odd(f.representative_point())];g=unary_union(chosen);assert g.is_valid and not g.is_empty
 # Independent OpenCV even-odd scan conversion of raw input vs reconstructed rings.
 lo=np.min(pts,axis=0)-1;hi=np.max(pts,axis=0)+1;scale=12;size=np.ceil((hi-lo)*scale).astype(int)+1
 def raster(loops):
  a=np.zeros((size[1],size[0]),np.uint8);cv2.fillPoly(a,[np.round((np.array(loop)-lo)*scale).astype(np.int32) for loop in loops],255);return a
 polys=[g] if g.geom_type=='Polygon' else list(g.geoms);loops=[]
 for poly in polys:loops += [list(poly.exterior.coords)]+[list(h.coords) for h in poly.interiors]
 raw=raster([pts]);rebuilt=raster(loops);xor=raw!=rebuilt;boundary=cv2.morphologyEx(raw,cv2.MORPH_GRADIENT,np.ones((3,3),np.uint8))>0;interior_diff=int(np.sum(xor&~boundary));assert interior_diff==0
 display=transform(lambda x,y:(2384-np.asarray(y),np.asarray(x)),g)
 results.append({'drawing_index':r['drawing_index'],'paint_color':r['paint_color'],'source_even_odd':True,'method':'node original segments; polygonize; retain faces with odd ray crossings. No snapping, buffering or vertex displacement.','faces':len(faces),'retained_faces':len(chosen),'area_pdf_points2':g.area,'zero_interior_raster_differences':interior_diff==0,'raster_scale':12,'boundary_pixel_differences':int(np.sum(xor)),'geometry_pdf_display_points':mapping(display),'semantic_status':'paint representation only; not a sector boundary'})
(p/'boundary-research/evenodd-reconstruction.json').write_text(json.dumps({'source_sha256':j['source_sha256'],'source_document_id':j['source_document_id'],'page_number':1,'reconstructions':results},indent=2));print([(r['drawing_index'],r['faces'],r['retained_faces'],r['area_pdf_points2'],r['boundary_pixel_differences']) for r in results])
