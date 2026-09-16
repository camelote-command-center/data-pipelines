"""Verify provenance and unchanged-transform diagnostic replay."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

P = Path(__file__).resolve().parent
for manifest in ['inputs.json', 'frozen-hashes.json']:
    for name, digest in json.loads((P / manifest).read_text()).items():
        assert hashlib.sha256((P / name).read_bytes()).hexdigest() == digest, name
original = json.loads((P / '../alignment/official-footprints.json').read_text())['features']
converted = json.loads((P / 'reference-polygons-lv03.json').read_text())['features']
assert [f['attributes'] for f in original] == [f['attributes'] for f in converted]
assert len({f['attributes']['OBJECTID'] for f in converted}) == 690
names = ['qa.json', 'current-footprint-overlay.png', 'flagged-component-contexts.png']
names += [f'{n}-correspondence-review.png' for n in ['north', 'east', 'south']]
before = {n: (P / n).read_bytes() for n in names}
subprocess.run([sys.executable, str(P / 'check.py')], check=True)
assert all((P / n).read_bytes() == data for n, data in before.items())
qa = json.loads(before['qa.json'])
assert not qa['parcel_matching_ready'] and not qa['independent_accuracy_verified']
assert qa['receiver_release'] == 'not_ready'
assert qa['summary']['outside_5m_component_ids'] == [30, 41, 48, 59, 63]
assert not any(r['identity_verified'] for r in qa['checks'])
print('Verified 690 reference identities, exact diagnostic replay, and no release.')
