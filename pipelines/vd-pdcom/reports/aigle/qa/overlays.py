import pathlib,json,numpy as np,pymupdf as fitz,argparse,hashlib
ap=argparse.ArgumentParser();ap.add_argument("--pdf",required=True);args=ap.parse_args()
assert hashlib.sha256(pathlib.Path(args.pdf).read_bytes()).hexdigest()=="bbd1d3d0474d5f491ac4cc17b05d7bf1c7912861d9f93f9347c719bf7522fedc"
from shapely.geometry import shape
root=pathlib.Path(__file__).resolve().parent;a=json.load(open(root.parent/'alignment/alignment.json'));ang,s,tx,ty=a['parameters'];R=np.array([[np.cos(ang),-np.sin(ang)],[np.sin(ang),np.cos(ang)]]);P0=np.array(a['pdf_origin_y_flipped']);Q0=np.array(a['reference_origin_lv95']);source=fitz.open(args.pdf);h=source[18].rect.height
for r in json.load(open(root/'summary.json')):
 doc=fitz.open();doc.insert_pdf(source,from_page=18,to_page=18);p=doc[0];overlay=p.new_shape()
 for f in json.load(open(root/f"holdout-{r['holdout_index']}.json"))['features']:
  g=shape(f['geometry']);parts=[g] if g.geom_type=='Polygon' else list(g.geoms)
  for part in parts:
   pts=(np.array(part.exterior.coords)-Q0-[tx,ty])@R/s+P0;pts[:,1]=h-pts[:,1];overlay.draw_polyline([fitz.Point(*v) for v in pts])
 overlay.finish(color=(1,0,1),width=.4);overlay.commit();x,y=r['pdf_point_y_flipped'];y=h-y;p.draw_circle(fitz.Point(x,y),1.5,color=(0,0,1),width=.5);p.get_pixmap(matrix=fitz.Matrix(4,4),clip=fitz.Rect(x-27,y-27,x+27,y+27)).save(root/f"overlay-{r['holdout_index']}.png")
