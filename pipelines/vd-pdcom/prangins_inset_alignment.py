"""Reproduce the independent inset frame control contract, not surveyed precision."""
import numpy as np
from shapely.geometry import shape,MultiPoint

def validate_alignment(b):
    rows=b['ground_control_evidence']['controls']
    if len(rows)!=25 or len({r['official_attributes']['OBJECTID'] for r in rows})!=25 or len({r['official_attributes']['EGID'] for r in rows})!=25:raise ValueError('inset_control_identity')
    for r in rows:
        if not r['official_attributes'].get('EGID') or r['official_attributes']['EGID']<=0:raise ValueError('inset_missing_building_identity')
        if r['role']!=('heldout' if r['inset_part']%3==0 else 'training'):raise ValueError('inset_preregistered_split')
        for key,pt in [('source_geometry_pdf','source_point'),('reference_geometry_lv95','reference_point')]:
            g=shape(r[key])
            if not g.is_valid or not np.allclose(list(g.centroid.coords[0]),r[pt],rtol=0,atol=1e-7):raise ValueError('inset_control_geometry')
    train=[r for r in rows if r['role']=='training'];hold=[r for r in rows if r['role']=='heldout']
    if (len(train),len(hold))!=(17,8):raise ValueError('inset_control_partition')
    z=np.array([complex(r['source_point'][0],-r['source_point'][1]) for r in train]);w=np.array([complex(*r['reference_point']) for r in train]);a=np.vdot(z-z.mean(),w-w.mean())/np.vdot(z-z.mean(),z-z.mean());t=w.mean()-a*z.mean();m=[a.real,a.imag,a.imag,-a.real,t.real,t.imag]
    if not np.allclose(m,b['affine'],atol=1e-7,rtol=0):raise ValueError('inset_independent_fit')
    err=[abs(a*complex(r['source_point'][0],-r['source_point'][1])+t-complex(*r['reference_point'])) for r in hold]
    if max(err)>.1 or abs(np.sqrt(np.mean(np.square(err)))-b['alignment']['holdout_rmse_m'])>1e-6:raise ValueError('inset_heldout_evidence')
    hull=MultiPoint([r['source_point'] for r in train]).convex_hull
    for f in b['features']:
        for p in f['source_paths']:
            g=shape(p['geometry_pdf']);fraction=g.intersection(hull).area/g.area
            if not hull.covers(g):raise ValueError('inset_outside_control_hull')
            if abs(fraction-p['ground_hull_fraction'])>1e-8:raise ValueError('inset_extrapolation_evidence')
    if b['source_precision_m'] is not None:raise ValueError('inset_precision_not_certified')
