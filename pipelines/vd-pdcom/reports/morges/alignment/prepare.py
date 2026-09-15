import argparse,hashlib,json,pathlib,sys,collections
import pymupdf as f
from shapely.geometry import Polygon
root=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,str(root.parent/'medium-density'));from extract import rings,SHA
p=argparse.ArgumentParser();p.add_argument('--pdf',type=pathlib.Path,required=True);args=p.parse_args()
assert hashlib.sha256(args.pdf.read_bytes()).hexdigest()==SHA
points=[];metadata=[];skipped=collections.Counter();rgb=(.8273746967315674,.739284336566925,.5452964305877686)
with f.open(args.pdf) as doc:
 for i,d in enumerate(doc[0].get_drawings()):
  c=d['fill'];r=d['rect']
  if not c or max(abs(a-b) for a,b in zip(c,rgb))>1e-6 or not (700<r.x0 and r.x1<2400 and 250<r.y0 and r.y1<1500):continue
  try:chains=rings(d['items'],.1)
  except ValueError:skipped['unsupported_path']+=1;continue
  if len(chains)!=1:skipped['multiple_rings']+=1;continue
  g=Polygon(chains[0])
  if not g.is_valid or not 4<g.area<2000:skipped['invalid_or_area_excluded']+=1;continue
  points.append([g.centroid.x,-g.centroid.y]);metadata.append({'drawing_index':i,'area_pdf_points_squared':g.area,'source_geometry':list(g.exterior.coords)})
(root/'source-points.json').write_text(json.dumps(points));(root/'source-components.json').write_text(json.dumps(metadata));(root/'source-selection.json').write_text(json.dumps({'sha256':SHA,'pdf_page':1,'main_map_roi_pdf':[700,250,2400,1500],'rgb':rgb,'area_range_pdf_points_squared':[4,2000],'selected':len(points),'skipped':dict(skipped),'limit':'Beige building-like filled source components, not identified surveyed controls; overprinted/clipped components and changed buildings may occur.'},indent=2));print(len(points),dict(skipped))
