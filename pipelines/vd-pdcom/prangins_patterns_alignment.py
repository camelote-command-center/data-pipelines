"""Reproduce the frozen transport registration; no surveyed/thematic precision claim."""
import hashlib
import numpy as np
from shapely.geometry import shape, MultiPoint

def validate_alignment(b):
    rows=b['ground_control_evidence']['controls']
    if len(rows)!=964 or len({r['official_attributes']['OBJECTID'] for r in rows})!=964 or len({r['official_attributes']['EGID'] for r in rows})!=964:
        raise ValueError('patterns_control_identity')
    prior=0
    for r in rows:
        if r['identity_source_artifact']=='page49-network-ground-fit.json':
            prior+=1
        elif r['role']!=('heldout' if int(hashlib.sha256(('official-building:'+str(r['official_attributes']['OBJECTID'])).encode()).hexdigest(),16)%3==0 else 'training'):
            raise ValueError('patterns_new_identity_role')
        for key,pt in [('source_geometry_pdf','source_point'),('reference_geometry_lv95','reference_point')]:
            g=shape(r[key])
            if not g.is_valid or not np.allclose(list(g.centroid.coords[0]),r[pt],rtol=0,atol=1e-7):raise ValueError('patterns_control_geometry')
    tr=[r for r in rows if r['role']=='training'];hold=[r for r in rows if r['role']=='heldout']
    if (prior,len(tr),len(hold))!=(562,642,322):raise ValueError('patterns_control_partition')
    z=np.array([complex(r['source_point'][0],-r['source_point'][1]) for r in tr]);w=np.array([complex(*r['reference_point']) for r in tr]);a=np.vdot(z-z.mean(),w-w.mean())/np.vdot(z-z.mean(),z-z.mean());t=w.mean()-a*z.mean();m=[a.real,a.imag,a.imag,-a.real,t.real,t.imag]
    if not np.allclose(m,b['affine'],atol=1e-7,rtol=0):raise ValueError('patterns_independent_fit')
    errors=[float(abs(a*complex(r['source_point'][0],-r['source_point'][1])+t-complex(*r['reference_point']))) for r in hold]
    if abs(max(errors)-b['alignment']['heldout_max_m'])>1e-6 or abs(np.sqrt(np.mean(np.square(errors)))-b['alignment']['holdout_rmse_m'])>1e-6:raise ValueError('patterns_heldout_evidence')
    hull=MultiPoint([r['source_point'] for r in tr]).convex_hull
    for f in b['features']:
        for p in f['source_paths']:
            if not hull.covers(shape(p['geometry_base_pdf'])) or p['ground_hull_fraction']!=1:raise ValueError('patterns_outside_training_hull')
    if b['source_precision_m'] is not None:raise ValueError('patterns_precision_not_certified')
