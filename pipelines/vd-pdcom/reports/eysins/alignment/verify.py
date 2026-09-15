"""Recompute saved candidate residuals without rerunning a stochastic search."""
from fit_common import *
import hashlib
for name,digest in json.loads((p/'frozen-hashes.json').read_text()).items():
    assert hashlib.sha256((p/name).read_bytes()).hexdigest()==digest,name
for name in ['initial-rejected.json','exploratory-fit.json','pair-fit.json']:
    for r in json.loads((p/name).read_text()):
        ds,idx=tree.query(trans(r['parameters']))
        assert np.allclose(ds,[m['distance_m'] for m in r['matches']],rtol=0,atol=1e-8)
        assert [ref['rows'][int(i)]['egid'] for i in idx]==[m['egid'] for m in r['matches']]
        assert abs(objective(r['parameters'])-r['objective'])<1e-8
        assert sum(ds[~train]<5)<4
source=json.loads((p/'official-footprints-source.json').read_text())
features=json.loads((p/'official-footprints.json').read_text())['features']
assert len(features)==source['count']==690
assert len({f['attributes']['OBJECTID'] for f in features})==690
assert len(ref['rows'])==617
assert json.loads((p/'qa.json').read_text())['status']=='rejected_no_geographic_release'
print('Frozen sources, 11 rejected candidate residuals/identities and reference counts verified.')
