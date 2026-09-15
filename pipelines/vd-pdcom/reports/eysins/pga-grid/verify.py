import hashlib,json,subprocess,sys
from pathlib import Path
P=Path(__file__).resolve().parent
for name,digest in json.loads((P/'frozen-hashes.json').read_text()).items():
    assert hashlib.sha256((P/name).read_bytes()).hexdigest()==digest,name
before={n:(P/n).read_bytes() for n in ['calibration.json','reference-overlay.png']}
subprocess.run([sys.executable,str(P/'calibrate.py')],check=True)
assert all((P/n).read_bytes()==b for n,b in before.items())
d=json.loads(before['calibration.json']);assert not d['SDAN_alignment_verified'] and not d['independent_geographic_accuracy_verified']
assert d['receiver_release']=='not_ready'
assert len([r for r in d['residuals'] if r['split']=='check'])==4
assert 1.94<d['check_axis_rmse_m']<1.96
print('PGA source/grid provenance and exact calibration replay verified; SDAN remains unaligned.')
