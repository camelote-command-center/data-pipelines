import pymupdf as fitz,json,sys,numpy as np
from pathlib import Path
import argparse
ap=argparse.ArgumentParser();ap.add_argument('--pdf',required=True);ap.add_argument('--output',default='prangins-alignment.json');args=ap.parse_args()
from scipy.spatial import cKDTree
from scipy.optimize import least_squares
from scipy.signal import fftconvolve
sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'pdcom-parser'/'src'))
from pdcom_parser.extract import drawing_to_polygon
import hashlib
assert hashlib.sha256(Path(args.pdf).read_bytes()).hexdigest()=='a4079a88201b716ede0793150102306f8b1faddb55c391a7c2c1696244a96d02', 'Unreviewed PDF version'
p=fitz.open(args.pdf)[0]
P=[]
for d in p.get_drawings():
 if d.get('layer')!='BATI' or not d.get('fill'):continue
 g=drawing_to_polygon(d,p.rect.height,min_area=8)
 if g is not None and g.area<15000:P.append([g.centroid.x,g.centroid.y])
P=np.unique(np.round(P,2),axis=0);Q=np.array([r[1:] for r in json.load(open(Path(__file__).with_name('reference-buildings.json')))])
P0=P.mean(axis=0);Q0=Q.mean(axis=0);P-=P0;Q-=Q0
# hold out one fifth of the PDF buildings before alignment.
train=P[np.arange(len(P))%5!=0];test=P[np.arange(len(P))%5==0]
cell=12.;origin=-6500.;size=1100
def grid(points):
 z=np.zeros((size,size));a=((points-origin)/cell).astype(int);v=(a>=0).all(1)&(a<size).all(1);a=a[v];np.add.at(z,(a[:,1],a[:,0]),1);return z
G=grid(Q);best=[]
for angle in range(0,360,2):
 a=np.deg2rad(angle);R=np.array([[np.cos(a),-np.sin(a)],[np.sin(a),np.cos(a)]])
 points=train@R.T*(5000*.0254/72)
 corr=fftconvolve(G,grid(points)[::-1,::-1],mode='full');iy,ix=np.unravel_index(np.argmax(corr),corr.shape)
 shift=np.array([ix-(size-1),iy-(size-1)])*cell
 best.append((corr[iy,ix],angle,shift))
best.sort(key=lambda x:-x[0]);print('top',[(round(b[0]),b[1],b[2].tolist()) for b in best[:4]],flush=True)
tree=cKDTree(Q)
def trans(P,x):
 a,s,tx,ty=x;R=np.array([[np.cos(a),-np.sin(a)],[np.sin(a),np.cos(a)]]);return P@R.T*s+[tx,ty]
solutions=[]
for _,angle,shift in best[:5]:
 x=np.array([np.deg2rad(angle),5000*.0254/72,*shift])
 for k in range(12):
  d,idx=tree.query(trans(train,x));mask=d<max(10,50-k*4)
  if mask.sum()<20:break
  x=least_squares(lambda z:(trans(train[mask],z)-Q[idx[mask]]).ravel(),x,loss='soft_l1',f_scale=4).x
 train_d,_=tree.query(trans(train,x));d,_=tree.query(trans(test,x));solutions.append((int((train_d<10).sum()),x,d))
solutions.sort(key=lambda x:-x[0]);_,x,d=solutions[0];n=int((d<10).sum())
print('P',len(P),'Q',len(Q),'x',x,'holdout',len(test),'under10',n,'median',np.median(d),'q90',np.quantile(d,.9),flush=True)
out=dict(source='Prangins PDF BATI layer',reference='bronze_ch.vd_batiment_rcb building centroids',parameters=x.tolist(),pdf_origin=P0.tolist(),reference_origin=Q0.tolist(),pdf_points=len(P),reference_points=len(Q),holdout_points=len(test),holdout_under10m=n,holdout_median_m=float(np.median(d)),holdout_p90_m=float(np.quantile(d,.9)),holdout_rmse_m=float(np.sqrt(np.mean(d*d))),status='diagnostic_only_not_validated')
json.dump(out,open(args.output,'w'),indent=2)
