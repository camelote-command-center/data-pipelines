from fit_common import *
# Two well-separated training components anchor each hypothesis; holdouts unused.
a=ids.index(27);b=ids.index(63);assert train[a] and train[b]
v=X[b]-X[a];vn=np.linalg.norm(v);i,j=np.triu_indices(len(Y),1);i,j=np.r_[i,j],np.r_[j,i];V=Y[j]-Y[i];sc=np.linalg.norm(V,axis=1)/vn;th=np.arctan2(V[:,1],V[:,0])-np.arctan2(v[1],v[0]);ok=(sc>=1)&(sc<=5);i,j,sc,th=i[ok],j[ok],sc[ok],th[ok]
best=[]
for start in range(0,len(i),3000):
 sl=slice(start,start+3000);cos=np.cos(th[sl]);sin=np.sin(th[sl]);R=np.stack([cos,-sin,sin,cos],axis=1).reshape(-1,2,2);mapped=np.einsum('nj,bkj->bnk',X-X[a],R)*sc[sl,None,None]+Y[i[sl],None,:];ds,_=tree.query(mapped[:,train]);score=np.mean(np.minimum(ds,25)**2,axis=1)
 for k in np.argsort(score)[:4]:
  idx=start+k;tx=Y[i[idx]]-(X[a]@R[k].T)*sc[idx];best.append((float(score[k]),[float(np.rad2deg(th[idx])),float(sc[idx]),float(tx[0]),float(tx[1])]))
best.sort();out=[]
for score,par in best[:30]:
 res=least_squares(lambda par:np.minimum(tree.query(trans(par)[train])[0],25),par,max_nfev=300)
 xy=trans(res.x);ds,idx=tree.query(xy)
 result={'parameters':res.x.tolist(),'objective':objective(res.x),'origin_pixel_x_yup':P.tolist(),'origin_lv95':Q.tolist(),'anchor_components':[27,63],'matches':[{'component':i,'split':'train' if train[j] else 'holdout','egid':ref['rows'][int(idx[j])]['egid'],'distance_m':float(ds[j]),'pixel':c[i]['point'],'reference':[float(v) for v in Y[idx[j]]+Q]} for j,i in enumerate(ids)]};out.append(result)
out.sort(key=lambda d:d['objective']);p.joinpath('pair-fit.json').write_text(json.dumps(out[:5],indent=2)+'\n');print([(r['parameters'],r['objective'],[m['distance_m'] for m in r['matches'] if m['split']=='holdout']) for r in out[:3]])
