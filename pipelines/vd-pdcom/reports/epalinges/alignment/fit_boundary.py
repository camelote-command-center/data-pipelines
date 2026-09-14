import json,pathlib,sys,hashlib,numpy as np,pymupdf as fitz,psycopg2
from shapely.geometry import Polygon,shape,mapping
from scipy.spatial import cKDTree
from scipy.optimize import least_squares
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[5]/'pdcom-parser'/'src'));from pdcom_parser.extract import _items_to_subpaths
import argparse
ap=argparse.ArgumentParser();ap.add_argument('--pdf',required=True);args=ap.parse_args()
pdf=pathlib.Path(args.pdf)
assert hashlib.sha256(pdf.read_bytes()).hexdigest()=='bd095d80d2284073a93ef7e108f89b083d32fb361c97b689809522b7b8f64c86';doc=fitz.open(pdf);page=doc[98];drawing=page.get_drawings()[2]
assert len(drawing['items'])==1171 and all(it[0] in ('l','c') for it in drawing['items'])
assert all(tuple(drawing['items'][i][-1])==tuple(drawing['items'][i+1][1]) for i in range(1170))
paths=_items_to_subpaths(drawing['items'],page.rect.height);assert len(paths)==1
poly=Polygon(paths[0]);assert poly.is_valid
root=pathlib.Path(__file__).resolve().parent
ref=shape(json.load(open(root/'reference-boundary-lv95.json'))['geometry'])
buildings=json.load(open(root/'reference-building-points.json'))
P=np.array([[p.x,p.y] for p in [poly.exterior.interpolate(d) for d in np.linspace(0,poly.exterior.length,1200,endpoint=False)]])
boundary=ref.boundary
Q=np.array([[p.x,p.y] for p in [boundary.interpolate(d) for d in np.arange(0,boundary.length,1)]])
P0=np.array([poly.centroid.x,poly.centroid.y]);Q0=np.array([ref.centroid.x,ref.centroid.y]);P-=P0;Q-=Q0
train=P[::2];hold=P[1::2];tree=cKDTree(Q)
def transform(p,x):
 a,s,tx,ty=x;R=np.array([[np.cos(a),-np.sin(a)],[np.sin(a),np.cos(a)]]);return p@R.T*s+[tx,ty]
x=np.array([0,np.sqrt(ref.area/poly.area),0,0])
for _ in range(20):
 d,idx=tree.query(transform(train,x));x=least_squares(lambda v:(transform(train,v)-Q[idx]).ravel(),x,loss='soft_l1',f_scale=3).x
D,_=tree.query(transform(hold,x));out={'document_sha256':hashlib.sha256(pdf.read_bytes()).hexdigest(),'commune_bfs':5584,'page_number':99,'drawing_index':2,'parameters':x.tolist(),'pdf_origin_y_flipped':P0.tolist(),'lv95_origin':Q0.tolist(),'calibration_reference':'bronze_ch.swiss_communes_geo BFS5584, EPSG2056','fit_points':600,'withheld_boundary_points':600,'withheld_boundary_median_m':float(np.median(D)),'withheld_boundary_p95_m':float(np.quantile(D,.95)),'withheld_boundary_rmse_m':float(np.sqrt(np.mean(D**2))),'withheld_boundary_max_m':float(max(D)),'status':'diagnostic_only','limitation':'Boundary fit/withheld samples share one reference boundary. Metrics are not independent surveyed point accuracy. Building-point overlays need separate visual review; no spatial release.'}
(root/'alignment.json').write_text(json.dumps(out,indent=2));(root/'reference-building-points.json').write_text(json.dumps(buildings));print(json.dumps(out,indent=2))
# Overlay source boundary and current RCB points in page coordinates.
a,s,tx,ty=x;R=np.array([[np.cos(a),-np.sin(a)],[np.sin(a),np.cos(a)]])
for egid,bx,by in buildings:
 p=(np.array([bx,by])-Q0-[tx,ty])@R/s+P0;pt=fitz.Point(p[0],page.rect.height-p[1]);page.draw_circle(pt,1.1,color=(1,0,1),width=.35)
for name,rect in [('north',(260,60,660,350)),('centre',(240,330,620,610)),('south',(70,520,650,800))]:
 page.get_pixmap(matrix=fitz.Matrix(2,2),clip=fitz.Rect(*rect)).save(str(root/(name+'.png')))
