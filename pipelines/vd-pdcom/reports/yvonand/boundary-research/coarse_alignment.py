import pathlib,json,numpy as np
from shapely.geometry import shape
from scipy.spatial import cKDTree
from scipy.optimize import least_squares
root=pathlib.Path(__file__).resolve().parent;s=json.load(open(root/'black-path-candidates.json'))['candidates'];t=json.load(open(root/'current-footprints.geojson'))['features'];scale=5000*.0254/72
sg=[shape(x['geometry_pdf_display_points']) for x in s];tg=[shape(x['geometry']) for x in t]
S=np.array([[g.centroid.x*scale,-g.centroid.y*scale] for g in sg]);T=np.array([[g.centroid.x,g.centroid.y] for g in tg]);sa=np.array([g.area*scale**2 for g in sg]);ta=np.array([g.area for g in tg]);sw=np.array([(g.bounds[2]-g.bounds[0])*scale for g in sg]);sh=np.array([(g.bounds[3]-g.bounds[1])*scale for g in sg]);tw=np.array([g.bounds[2]-g.bounds[0] for g in tg]);th=np.array([g.bounds[3]-g.bounds[1] for g in tg]);train=np.array([x['drawing_index']%5!=0 for x in s]);eligible=(sa>70)&train
pairs=np.argwhere(eligible[:,None] & (abs(sa[:,None]/ta-1)<.15)&(abs(sw[:,None]/tw-1)<.15)&(abs(sh[:,None]/th-1)<.15))
delta=T[pairs[:,1]]-S[pairs[:,0]];bins=np.round(delta/10).astype(int);uniq,counts=np.unique(bins,axis=0,return_counts=True);tops=np.argsort(counts)[-10:][::-1];print('peaks',[(uniq[i].tolist(),int(counts[i])) for i in tops[:4]])
tree=cKDTree(T);best=None
for k in tops:
 shift=uniq[k]*10;dist,idx=tree.query(S[eligible]+shift);score=np.sum(dist<8)
 if best is None or score>best[0]:best=(score,shift)
shift=best[1].astype(float)
# Translation-only robust refinement, fixed printed scale/orientation. No heldout source participates.
for _ in range(5):
 dist,idx=tree.query(S+shift);good=eligible&(dist<8)&(abs(sa/ta[idx]-1)<.15);shift=np.median(T[idx[good]]-S[good],axis=0)
dist,idx=tree.query(S+shift);good=eligible&(dist<5)&(abs(sa/ta[idx]-1)<.15);hold=(~train)&(sa>70)&(dist<8)&(abs(sa/ta[idx]-1)<.15)
result={'method':'exploratory fixed-scale north-up translation; histogram then training-only median refinement','scale_metres_per_pdf_point':scale,'translation':shift.tolist(),'training_source_rule':'drawing_index %5 !=0 and source area>70m2','training_matches':int(sum(good)),'training_rmse_m':float(np.sqrt(np.mean(dist[good]**2))),'reserved_source_candidates':int(sum((~train)&(sa>70))),'reserved_proximity_area_matches':int(sum(hold)),'reserved_rmse_m':float(np.sqrt(np.mean(dist[hold]**2))),'limitations':'Proximity/area filtered exploratory correspondences; not independent surveyed controls or verified building identities. No sector geography authorised by this fit. All other held-out candidates remain unmatched, not excluded from success denominator.','matches':[{'source_drawing_index':s[i]['drawing_index'],'reference_OBJECTID':t[idx[i]]['properties']['OBJECTID'],'reference_EGID':t[idx[i]]['properties']['EGID'],'distance_m':float(dist[i]),'source_area_m2':float(sa[i]),'target_area_m2':float(ta[idx[i]]),'role':'reserved' if not train[i] else 'training'} for i in np.where(good|hold)[0]]}
(root/'exploratory-alignment.json').write_text(json.dumps(result,indent=2));print({k:v for k,v in result.items() if k!='matches'})
