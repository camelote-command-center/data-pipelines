import pathlib,json,numpy as np,pymupdf as fitz,sys,argparse,hashlib
args=argparse.ArgumentParser();args.add_argument("pdf",type=pathlib.Path);args=args.parse_args()
assert hashlib.sha256(args.pdf.read_bytes()).hexdigest()=="a4079a88201b716ede0793150102306f8b1faddb55c391a7c2c1696244a96d02", "unreviewed source PDF"
here=pathlib.Path(__file__).resolve().parent
repo=here.parents[4]
from shapely.geometry import shape,mapping
from shapely.affinity import affine_transform
sys.path.insert(0,str(repo/'pdcom-parser/src'));from pdcom_parser.extract import drawing_to_polygon
root=here;a=json.load(open(here.parent/'alignment.json'));angle,s,tx,ty=a['parameters'];R=np.array([[np.cos(angle),-np.sin(angle)],[np.sin(angle),np.cos(angle)]]);P0=np.array(a['pdf_origin']);Q0=np.array(a['reference_origin']);matrix=[s*R[0,0],s*R[0,1],s*R[1,0],s*R[1,1],Q0[0]+tx-s*(R@P0)[0],Q0[1]+ty-s*(R@P0)[1]]
original=fitz.open(args.pdf);page=original[0];polys=[]
for d in page.get_drawings():
 if d.get('layer')=='BATI' and d.get('fill'):
  g=drawing_to_polygon(d,page.rect.height,min_area=8)
  if g is not None and g.area<15000:polys.append(g)
summary=[]
for p in json.load(open(here.parent/'holdout-outliers.json')):
 ref=json.load(open(root/f"holdout-{p['test_index']}.json"));pdfpoint=np.array(p['pdf_point']);g=next(g for g in polys if np.array_equal(np.round([g.centroid.x,g.centroid.y],2),pdfpoint));world=affine_transform(g,matrix)
 scored=[]
 for f in ref['features']:
  h=shape(f['geometry']);area=world.intersection(h).area;union=world.union(h).area
  scored.append({'egid':f['properties'].get('EGID'),'objectid':f['properties']['OBJECTID'],'iou':area/union,'intersection_area_m2':area,'distance_to_footprint_m':f['distance_to_footprint_m']})
 scored.sort(key=lambda x:-x['iou']);out={'holdout_index':p['test_index'],'reference_count_verified':ref['count_verified'],'old_point_residual_m':p['nearest_m'],'source_building_geometry_lv95':mapping(world),'best_footprint_matches':scored[:3]};summary.append(out)
 doc=fitz.open();doc.insert_pdf(original,from_page=0,to_page=0);target=doc[0]
 for f in ref['features']:
  h=shape(f['geometry']);parts=[h] if h.geom_type=='Polygon' else list(h.geoms)
  for poly in parts:
   points=(np.array(poly.exterior.coords)-Q0-[tx,ty])@R/s+P0;points[:,1]=page.rect.height-points[:,1]
   target.draw_polyline([fitz.Point(*v) for v in points],color=(1,0,1),width=.6)
 x,y=pdfpoint;y=page.rect.height-y;target.draw_circle(fitz.Point(x,y),3,color=(0,0,1),width=.7)
 target.get_pixmap(matrix=fitz.Matrix(2.5,2.5),clip=fitz.Rect(x-55,y-55,x+55,y+55)).save(str(root/f"footprints-{p['test_index']}.png"))
(root/'review-summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps([{k:v for k,v in r.items() if k!='source_building_geometry_lv95'} for r in summary],indent=2))
