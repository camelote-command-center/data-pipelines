"""Verify frozen provenance, deterministic replay and research release gates."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

P = Path(__file__).resolve().parent
for manifest in ['inputs.json', 'frozen-hashes.json']:
    for name, digest in json.loads((P / manifest).read_text()).items():
        assert hashlib.sha256((P / name).read_bytes()).hexdigest() == digest, name
names = ['matches.json', 'correspondence.json', 'historical-map-overlay.png', 'landmark-contexts.png']
before = {name: (P / name).read_bytes() for name in names}
subprocess.run([sys.executable, str(P / 'match.py')], check=True)
assert all((P / name).read_bytes() == data for name, data in before.items())
r = json.loads(before['correspondence.json'])
assert not r['independent_accuracy_verified'] and not r['parcel_matching_ready']
assert r['receiver_release'] == 'not_ready'
assert [f['id'] for f in r['features']] == [f'E{i}' for i in range(1, 8)]
assert r['fit_inlier_count'] == 10
assert 2.51 < r['fit_inlier_rmse_m'] < 2.52
assert len(r['reference_edge_checks']) == 25
assert all(f['fraction_outside_inlier_hull'] > .95 for f in r['features'] if f['id'] != 'E3')
assert all(f['inliers'] <= 3 for f in json.loads(before['matches.json']) if f['mode'] == 'dark')
print('Verified source provenance, exact replay and research-only release gates.')
