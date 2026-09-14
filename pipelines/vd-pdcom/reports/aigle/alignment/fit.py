import json,pathlib,numpy as np
from scipy.spatial import cKDTree
from scipy.optimize import least_squares
from scipy.signal import fftconvolve
root=pathlib.Path(__file__).resolve().parent
P=np.array(json.loads((root/'pdf-points.json').read_text()));Q=np.array([r[1:] for r in json.loads((root/'reference-points.json').read_text())]);P0=P.mean(0);Q0=Q.mean(0);P-=P0;Q-=Q0
train=P[np.arange(len(P))%5!=0];test=P[np.arange(len(P))%5==0]
reject=json.loads((root/'alignment.json').read_text())['rejected_paths']
cell=12;size=700;origin=-4200
def grid(points):
 z=np.zeros((size,size));a=((points-origin)/cell).astype(int);a=a[(a>=0).all(1)&(a<size).all(1)];np.add.at(z,(a[:,1],a[:,0]),1);return z
G=grid(Q);tree=cKDTree(Q);best=[]
def trans(p,x):
 a,s,tx,ty=x;R=np.array([[np.cos(a),-np.sin(a)],[np.sin(a),np.cos(a)]]);return p@R.T*s+[tx,ty]
for ang in [-2,-1,0,1,2]:
 for scale in [3.4,3.5,3.6]:
  angle=np.deg2rad(ang);corr=fftconvolve(G,grid(trans(train,[angle,scale,0,0]))[::-1,::-1],mode='full');iy,ix=np.unravel_index(corr.argmax(),corr.shape);shift=np.array([ix-size+1,iy-size+1])*cell;best.append((corr[iy,ix],[angle,scale,*shift]))
solutions=[]
for _,x in sorted(best,key=lambda z:-z[0])[:5]:
 for k in range(15):
  dist,idx=tree.query(trans(train,x));mask=dist<max(8,50-3*k)
  if mask.sum()<30:break
  x=least_squares(lambda z:(trans(train[mask],z)-Q[idx[mask]]).ravel(),x,loss='soft_l1',f_scale=3).x
 dist,_=tree.query(trans(train,x));solutions.append(((dist<10).sum(),x))
_,x=max(solutions,key=lambda z:z[0]);dist,_=tree.query(trans(test,x))
out={'document_sha256':'bbd1d3d0474d5f491ac4cc17b05d7bf1c7912861d9f93f9347c719bf7522fedc','parameters':list(x),'pdf_origin_y_flipped':P0.tolist(),'reference_origin_lv95':Q0.tolist(),'pdf_points':len(P),'reference_points':len(Q),'holdout_count':len(test),'holdout_under10m':int((dist<10).sum()),'holdout_median_m':float(np.median(dist)),'holdout_p90_m':float(np.quantile(dist,.9)),'holdout_rmse_m':float(np.sqrt(np.mean(dist**2))),'rejected_paths':dict(reject),'interpretation':'Building centroid to nearest registered location-point diagnostics only; not surveyed accuracy. Fixed prefit every-fifth holdout. Main-map only; inset excluded. Review required.'}
(root/'alignment.json').write_text(json.dumps(out,indent=2));print(json.dumps(out),flush=True)
