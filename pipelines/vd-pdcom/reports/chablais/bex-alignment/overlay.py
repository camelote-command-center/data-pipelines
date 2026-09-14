import argparse,hashlib,json,pathlib,numpy as np,pymupdf as f
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
root=pathlib.Path(__file__).resolve().parent;p=argparse.ArgumentParser();p.add_argument('--pdf',type=pathlib.Path,required=True);args=p.parse_args()
if hashlib.sha256(args.pdf.read_bytes()).hexdigest()!='7d2fa9b86cb8bb49757eb935bd0735ff3a1476aebc8de39aed4cd39d905f42d5':raise ValueError('source_sha_mismatch')
with f.open(args.pdf) as d:
 im=next(i for i in d[75].get_images() if i[2:4]==(809,979));p=f.Pixmap(d,im[0]);rgb=np.frombuffer(p.samples,dtype=np.uint8).reshape(p.height,p.width,3)
r=json.loads((root/'alignment.json').read_text());a,s,tx,ty=r['parameters'];R=np.array([[np.cos(a),-np.sin(a)],[np.sin(a),np.cos(a)]]);P0=np.array(r['source_origin_y_flipped']);Q0=np.array(r['reference_origin_lv95'])
def inverse(q):
 xy=(np.asarray(q)-Q0-[tx,ty])@R/s+P0;xy[...,1]*=-1;return xy
fc=json.loads((root/'official-footprints.geojson').read_text());qa=json.loads((root/'footprint-review.json').read_text());polys=[]
for ft in fc['features']:
 g=ft['geometry'];rings=[g['coordinates']] if g['type']=='Polygon' else g['coordinates']
 for poly in rings:polys.append(inverse(poly[0]))
fig,axes=plt.subplots(2,3,figsize=(14,11));axes=axes.ravel()
for ax in axes:ax.imshow(rgb)
for i,ax in enumerate(axes):
 for poly in polys:ax.plot(poly[:,0],poly[:,1],color='cyan',lw=.6)
 if i==0:ax.set_xlim(0,809);ax.set_ylim(979,0);ax.set_title('Current official building footprints over source inset')
 elif i<=4:
  out=qa['outliers'][i-1];x,y=inverse(out['lv95']);ax.scatter([x],[y],c='yellow',edgecolors='black',s=40);ax.set_xlim(x-65,x+65);ax.set_ylim(y+65,y-65);ax.set_title('Holdout index %s; footprint distance %.2f m'%(out['source_component_index'],out['nearest_footprint_distance_m']))
 else:ax.clear();ax.axis('off')
fig.suptitle('Bex PUM.7 — exploratory alignment QA; cyan = official footprint outlines\nNo legal boundary or surveyed positional accuracy inferred');fig.tight_layout();fig.savefig(root/'alignment-review.png',dpi=140);plt.close(fig)
