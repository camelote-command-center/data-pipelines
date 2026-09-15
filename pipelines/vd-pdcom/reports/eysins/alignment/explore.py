from fit_common import *
results=[]
for seed in [7,17,27]:
 res=differential_evolution(objective,[(0,90),(1.0,3.5),(-650,650),(-650,650)],seed=seed,popsize=25,maxiter=350,tol=1e-7,polish=True)
 xy=trans(res.x);ds,idx=tree.query(xy);result={'seed':seed,'parameters':res.x.tolist(),'objective':res.fun,'origin_pixel_x_yup':P.tolist(),'origin_lv95':Q.tolist(),'matches':[{'component':i,'split':'train' if train[j] else 'holdout','egid':ref['rows'][int(idx[j])]['egid'],'distance_m':float(ds[j]),'pixel':c[i]['point'],'reference':[float(v) for v in Y[idx[j]]+Q]} for j,i in enumerate(ids)]};results.append(result);print(seed,res.x,res.fun,'train<10',sum(ds[train]<10),'holdout',ds[~train])
p.joinpath('exploratory-fit.json').write_text(json.dumps(results,indent=2)+'\n')
