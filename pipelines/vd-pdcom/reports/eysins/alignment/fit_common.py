import json,pathlib,numpy as np
from scipy.spatial import cKDTree
from scipy.optimize import differential_evolution,least_squares
p=pathlib.Path(__file__).resolve().parent
c=json.loads((p/'black-components.json').read_text());ref=json.loads((p/'reference-points.json').read_text());ids=[24,27,28,30,33,34,38,41,43,45,47,48,50,51,52,53,54,55,56,58,59,60,61,63,64]
X=np.array([c[i]['point'] for i in ids]);X[:,1]*=-1;P=X.mean(0);X-=P
Y=np.array([[r['x'],r['y']] for r in ref['rows']]);Q=np.array([2505100,1137450]);Y-=Q;tree=cKDTree(Y);train=np.arange(len(X))%4!=0
def trans(par,x=X):
 theta,scale,tx,ty=par;t=np.deg2rad(theta);R=np.array([[np.cos(t),-np.sin(t)],[np.sin(t),np.cos(t)]]);return x@R.T*scale+np.array([tx,ty])
def objective(par):
 ds,_=tree.query(trans(par)[train]);return np.mean(np.minimum(ds,25)**2)
