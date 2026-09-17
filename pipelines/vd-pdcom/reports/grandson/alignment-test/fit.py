"""Training-only coarse pose search; freeze transform before holdout checks."""
import pathlib,json,hashlib,math,numpy as np
from shapely.geometry import Polygon,shape
from shapely.affinity import affine_transform
from scipy.spatial import cKDTree
p=pathlib.Path(__file__).resolve().parent;base=p.parent/'alignment-controls';srcfile=base/'source-candidates.json';reffile=base/'official-footprints.json';sources=json.loads(srcfile.read_text())['candidates'];refs=json.loads(reffile.read_text())['features'];manifest=json.loads((base/'manifest.json').read_text());assert hashlib.sha256(reffile.read_bytes()).hexdigest()==manifest['sha256']
# Split is fixed before any pose search or nearest-neighbour inspection.
train=[x for x in sources if int(hashlib.sha256(str(x['drawing_index']).encode()).hexdigest()[:8],16)%3!=0]
hold=[x for x in sources if x not in train]
(p/'split.json').write_text(json.dumps({'rule':'SHA256(decimal drawing_index) first8hex modulo3:0reserved,otherwise training','training':[x['drawing_index'] for x in train],'reserved':[x['drawing_index'] for x in hold]},indent=2))
refpolys=[];refattrs=[];invalid=[]
for x in refs:
 # ArcGIS exterior orientation negative, holes positive: symmetric difference preserves rings.
 rings=[Polygon(r) for r in x['geometry']['rings']]
 if any(not r.is_valid for r in rings):invalid.append(x['attributes']['OBJECTID']);continue
 g=rings[0]
 for r in rings[1:]:g=g.symmetric_difference(r)
 if not g.is_valid or g.is_empty:invalid.append(x['attributes']['OBJECTID']);continue
 refpolys.append(g);refattrs.append(x['attributes'])
R=np.array([[g.centroid.x,g.centroid.y] for g in refpolys]);RA=np.array([g.area for g in refpolys]);tree=cKDTree(R)
scale=5000*.0254/72
T=np.array([[x['centroid_pdf'][0],-x['centroid_pdf'][1]] for x in train]);TA=np.array([x['area_pdf_points_squared']*scale**2 for x in train])
def orientation(g):
 a=np.array(g.minimum_rotated_rectangle.exterior.coords);v=np.diff(a,axis=0);lens=np.linalg.norm(v,axis=1);v=v[np.argmax(lens)];return math.atan2(v[1],v[0]),max(lens)/min(lens)
refangles=np.array([orientation(g)[0] for g in refpolys]);refaspects=np.array([orientation(g)[1] for g in refpolys]);best=None;poses=0
for si in sorted(range(len(train)),key=lambda i:TA[i],reverse=True)[:60]:
 g=affine_transform(shape(train[si]['geometry_pdf_coordinates']),[1,0,0,-1,0,0]);angle,aspect=orientation(g)
 if aspect<1.3:continue
 candidates=np.flatnonzero((RA/TA[si]>.85)&(RA/TA[si]<1.18)&(refaspects/aspect>.85)&(refaspects/aspect<1.18))
 for ri in candidates:
  for flip in [0,math.pi]:
   theta=refangles[ri]-angle+flip;rot=np.array([[math.cos(theta),-math.sin(theta)],[math.sin(theta),math.cos(theta)]]);off=R[ri]-scale*T[si]@rot.T;dist,idx=tree.query(scale*T@rot.T+off);good=(dist<8)&(RA[idx]/TA>.65)&(RA[idx]/TA<1.5);score=int(good.sum());poses+=1
   if best is None or score>best[0]:best=(score,theta,off)
assert best is not None
score,theta,off=best
# Fixed printed scale. Refine only on training centroid matches within8m and area ratio gate.
for iteration in range(20):
 rot=np.array([[math.cos(theta),-math.sin(theta)],[math.sin(theta),math.cos(theta)]]);dist,idx=tree.query(scale*T@rot.T+off);good=np.flatnonzero((dist<8)&(RA[idx]/TA>.65)&(RA[idx]/TA<1.5));good=sorted(good,key=lambda i:dist[i]);seen=set();good=[i for i in good if not (idx[i] in seen or seen.add(idx[i]))];assert len(good)>10
 a=scale*T[good];b=R[idx[good]];am=a.mean(axis=0);bm=b.mean(axis=0);u,sv,vt=np.linalg.svd((a-am).T@(b-bm));rr=vt.T@u.T
 if np.linalg.det(rr)<0:vt[-1]*=-1;rr=vt.T@u.T
 newtheta=math.atan2(rr[1,0],rr[0,0]);newoff=bm-am@rr.T
 if abs(newtheta-theta)<1e-12 and np.linalg.norm(newoff-off)<1e-7:theta,off=newtheta,newoff;break
 theta,off=newtheta,newoff
rot=np.array([[math.cos(theta),-math.sin(theta)],[math.sin(theta),math.cos(theta)]]);coeff=[scale*rot[0,0],-scale*rot[0,1],scale*rot[1,0],-scale*rot[1,1],*off]
# Transform now frozen; holdouts cannot affect fit or thresholds.
def evaluate(rows,blocked=set()):
 out=[];seen=set()
 for x in rows:
  g=affine_transform(shape(x['geometry_pdf_coordinates']),coeff);dist,ri=tree.query([g.centroid.x,g.centroid.y]);ri=int(ri);ref=refpolys[ri];oid=refattrs[ri]['OBJECTID'];iou=g.intersection(ref).area/g.union(ref).area;hd=g.hausdorff_distance(ref)
  ok=dist<8 and .65<ref.area/g.area<1.5 and oid not in blocked and oid not in seen
  if ok:seen.add(oid)
  out.append({'drawing_index':x['drawing_index'],'nearest_oid':oid,'distance_m':float(dist),'iou':iou,'hausdorff_m':hd,'accepted_correspondence':bool(ok),'blocked_training_oid':oid in blocked,'duplicate_oid':oid in seen and not ok})
 return out
training=evaluate(train);blocked={x['nearest_oid'] for x in training if x['accepted_correspondence']};reserved=evaluate(hold,blocked)
def stats(rows):
 accepted=[x for x in rows if x['accepted_correspondence']];return {'total':len(rows),'accepted':len(accepted),'unmatched_or_rejected':len(rows)-len(accepted),'rmse_accepted_m':float(np.sqrt(np.mean([x['distance_m']**2 for x in accepted]))) if accepted else None,'median_iou_accepted':float(np.median([x['iou'] for x in accepted])) if accepted else None,'iou_ge_085_and_hausdorff_le_5m':sum(x['iou']>=.85 and x['hausdorff_m']<=5 for x in accepted)}
result={'source_sha256':'1f349cf0dcf2d9f5fb7e13ccddaee37fa041c223c4c95834fef990c9765a360a','reference_sha256':manifest['sha256'],'candidate_sha256':hashlib.sha256(srcfile.read_bytes()).hexdigest(),'scale_m_per_pdf_point':scale,'theta_radians_on_reflected_pdf':theta,'affine_pdf_to_lv95':coeff,'seed_pose_count':poses,'seed_training_score':score,'invalid_reference_oids':invalid,'training_summary':stats(training),'reserved_summary':stats(reserved),'training':training,'reserved':reserved,'limits':['Automated nearest-centroid correspondences; require visual checks and changed-footprint review.','Reserved rows excluded before fitting and cannot tune transform.','Reference objects used by accepted training excluded from reserved acceptance.','Printed scale fixed; source indicative historical cadastre, not surveyed accuracy.','No runtime sectors or parcel joins.']}
(p/'results.json').write_text(json.dumps(result,indent=2)+'\n');print({k:result[k] for k in ['seed_pose_count','seed_training_score','affine_pdf_to_lv95','training_summary','reserved_summary']})
