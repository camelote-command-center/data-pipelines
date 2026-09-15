import argparse,hashlib,json,pathlib,sys,collections
import pymupdf as f
from shapely.geometry import Polygon
root=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,str(root.parent/'vector-outline'));from extract import runs
p=argparse.ArgumentParser();p.add_argument('--pdf',type=pathlib.Path,required=True);args=p.parse_args()
sha='8b78d42eb83210b6b63404dd8b4270fedd15d1b7e3d65d2a403d6edcee654a3c'
if hashlib.sha256(args.pdf.read_bytes()).hexdigest()!=sha:raise ValueError('source_sha_mismatch')
points=[];metadata=[];skipped=collections.Counter()
with f.open(args.pdf) as doc:
 for i,d in enumerate(doc[0].get_drawings()):
  c=d['fill'];r=d['rect']
  if not c or max(abs(x-.498039) for x in c)>.00001 or not (750<r.x0 and r.x1<1500 and 400<r.y0 and r.y1<1150):continue
  try:chains=runs(d['items'])
  except ValueError:skipped['non_line_path']+=1;continue
  if len(chains)!=1:skipped['multiple_rings']+=1;continue
  g=Polygon(chains[0])
  if not g.is_valid or not 2<g.area<2000:skipped['invalid_or_area_excluded']+=1;continue
  points.append([g.centroid.x,-g.centroid.y]);metadata.append({'drawing_index':i,'area_pdf_points_squared':g.area,'source_geometry':list(g.exterior.coords)})
(root/'source-points.json').write_text(json.dumps(points));(root/'source-components.json').write_text(json.dumps(metadata));(root/'source-selection.json').write_text(json.dumps({'sha256':sha,'pdf_page':1,'main_map_roi_pdf':[750,400,1500,1150],'rgb':[.498039]*3,'area_range_pdf_points_squared':[2,2000],'selected':len(points),'skipped':dict(skipped),'limit':'Selected building-like filled source polygons, not identified surveyed controls; main-map ROI excludes inset.'},indent=2));print(len(points),dict(skipped))
