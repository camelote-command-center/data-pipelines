"""Qualified raster outline candidates; no claim of exact legal perimeter."""
import argparse,hashlib,json,pathlib,numpy as np,pymupdf as f
from scipy import ndimage
from shapely.geometry import box,mapping
from shapely.ops import unary_union,transform
ROOT=pathlib.Path(__file__).resolve().parent
SHA='7d2fa9b86cb8bb49757eb935bd0735ff3a1476aebc8de39aed4cd39d905f42d5'
def extract(pdf,output):
 if hashlib.sha256(pdf.read_bytes()).hexdigest()!=SHA:raise ValueError('source_sha_mismatch')
 with f.open(pdf) as doc:
  ims=[i for i in doc[75].get_images() if i[2:4]==(809,979)]
  if len(ims)!=1:raise ValueError('unexpected_inset')
  p=f.Pixmap(doc,ims[0][0]);rgb=np.frombuffer(p.samples,dtype=np.uint8).reshape(p.height,p.width,3).astype(float)
 # Select red fill AND red-overprinted buildings. Roads/parcel lines are pale.
 raw=(rgb[:,:,0]>1.6*rgb[:,:,1])&(rgb[:,:,0]-rgb[:,:,2]>35)
 a=json.loads((ROOT.parent/'bex-alignment/alignment.json').read_text());ang,scale,tx,ty=a['parameters'];R=np.array([[np.cos(ang),-np.sin(ang)],[np.sin(ang),np.cos(ang)]]);P0=np.array(a['source_origin_y_flipped']);Q0=np.array(a['reference_origin_lv95'])
 def geographic(x,y,z=None):
  xy=np.column_stack((x,-np.asarray(y)));q=(xy-P0)@R.T*scale+[tx,ty]+Q0;return q[:,0],q[:,1]
 variants=[];masks={}
 for radius in [1,2,3]:
  yy,xx=np.mgrid[-radius:radius+1,-radius:radius+1];closed=ndimage.binary_closing(raw,structure=xx*xx+yy*yy<=radius*radius);lab,n=ndimage.label(closed);sizes=np.bincount(lab.ravel());sizes[0]=0;largest=lab==sizes.argmax();filled=ndimage.binary_fill_holes(largest)
  boxes=[]
  for y,row in enumerate(filled):
   d=np.diff(np.r_[False,row,False].astype(int));starts=np.flatnonzero(d==1);ends=np.flatnonzero(d==-1)
   boxes.extend(box(int(x0),y,int(x1),y+1) for x0,x1 in zip(starts,ends))
  polygon=unary_union(boxes).simplify(.5,preserve_topology=True)
  if polygon.geom_type!='Polygon' or not polygon.is_valid:raise ValueError('non_single_outline')
  g=transform(geographic,polygon);variants.append({'type':'Feature','properties':{'closing_radius_pixels':radius,'source_pixel_area':float(polygon.area),'area_m2':g.area,'raw_red_pixels':int(raw.sum()),'filled_pixels':int(filled.sum()),'excluded_red_pixels':int((raw&~filled).sum()),'label':'PUM.7 — Pôle d’urbanisation mixte Bex-Gare','review_status':'review_required'},'geometry':mapping(g),'source_pixel_geometry':mapping(polygon)});masks['radius'+str(radius)]=filled
 output.mkdir(parents=True,exist_ok=True);np.savez_compressed(output/'masks.npz',raw=raw,**masks)
 report={'source_sha256':SHA,'pdf_page':76,'coordinate_system':'LV95_EPSG2056','source_coordinate_system':'image_pixels_top_left','selection':'radius2 is provisional; radius1/3 quantify cleanup sensitivity','limitations':'Closing bridges narrow raster gaps and hole filling infers interiors across pale parcel lines/roads. Perimeter still requires refinement per source; not surveyed or legal geometry.','features':variants};(output/'variants.json').write_text(json.dumps(report,indent=2)+'\n')
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 fig,ax=plt.subplots(figsize=(9,11));ax.imshow(rgb.astype(np.uint8))
 for v,color in zip(variants,['yellow','cyan','blue']):
  q=np.array(v['source_pixel_geometry']['coordinates'][0]);ax.plot(q[:,0],q[:,1],color=color,lw=1,label='Closing radius %spx'%v['properties']['closing_radius_pixels'])
 ax.legend();ax.set_title('Bex PUM.7 — raster cleanup sensitivity; approximate candidate outline');fig.tight_layout();fig.savefig(output/'outline-review.png',dpi=140);plt.close(fig);return report
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--pdf',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);args=p.parse_args();r=extract(args.pdf,args.output);print(json.dumps([f['properties'] for f in r['features']]))
