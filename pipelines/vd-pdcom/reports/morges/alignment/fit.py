import json,pathlib,numpy as np
from scipy.spatial import cKDTree
from scipy.optimize import least_squares
from scipy.signal import fftconvolve
root=pathlib.Path(__file__).resolve().parent
P=np.array(json.loads((root/'source-points.json').read_text()));Q=np.array([r[1:] for r in json.loads((root/'reference-points.json').read_text())]);P0=P.mean(0);Q0=Q.mean(0);P-=P0;Q-=Q0
train=P[np.arange(len(P))%5!=0];test=P[np.arange(len(P))%5==0]
cell=8;size=900;origin=-3600
def grid(points):
 z=np.zeros((size,size));a=((points-origin)/cell).astype(int);a=a[(a>=0).all(1)&(a<size).all(1)];np.add.at(z,(a[:,1],a[:,0]),1);return z
assert np.all((Q>=origin)&(Q<origin+size*cell)),"reference_grid_truncation"
G=grid(Q);tree=cKDTree(Q)
def trans(p,x):
 a,s,tx,ty=x;R=np.array([[np.cos(a),-np.sin(a)],[np.sin(a),np.cos(a)]]);return p@R.T*s+[tx,ty]
best=[]
for ang in range(30,61,5):
 for scale in [1.72,1.76,1.80]:
  angle=np.deg2rad(ang);corr=fftconvolve(G,grid(trans(train,[angle,scale,0,0]))[::-1,::-1],mode='full');iy,ix=np.unravel_index(corr.argmax(),corr.shape);shift=np.array([ix-size+1,iy-size+1])*cell;best.append((float(corr[iy,ix]),[angle,scale,*shift]))
solutions=[]
for _,x in sorted(best,key=lambda z:-z[0])[:8]:
 for k in range(20):
  dist,idx=tree.query(trans(train,x));mask=dist<max(5,40-2*k)
  if mask.sum()<20:break
  x=least_squares(lambda z:(trans(train[mask],z)-Q[idx[mask]]).ravel(),x,loss='soft_l1',f_scale=2,bounds=([.4,1.6,-3000,-3000],[1.2,1.9,3000,3000])).x
 dist,_=tree.query(trans(train,x));solutions.append({'training_under5m':int((dist<5).sum()),'training_median_m':float(np.median(dist)),'parameters':list(x)})
solutions.sort(key=lambda z:(-z['training_under5m'],z['training_median_m']));x=solutions[0]['parameters'];dist,idx=tree.query(trans(test,x));out={'parameters':x,'source_origin_y_flipped':P0.tolist(),'reference_origin_lv95':Q0.tolist(),'source_count':len(P),'reference_count':len(Q),'train_count':len(train),'holdout_count':len(test),'holdout_under5m':int((dist<5).sum()),'holdout_median_m':float(np.median(dist)),'holdout_p90_m':float(np.quantile(dist,.9)),'holdout_rmse_m':float(np.sqrt(np.mean(dist**2))),'solutions':solutions,'interpretation':'Exploratory nearest RCB point fit; fixed prefit every fifth source holdout. Not surveyed accuracy, identified point correspondence or validated geographic sectors.'};(root/'alignment.json').write_text(json.dumps(out,indent=2));print(json.dumps(out),flush=True)
