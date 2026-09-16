"""Verify frozen grid/legend evidence and deterministic replay."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from PIL import Image
import pymupdf

P=Path(__file__).resolve().parent
for name,digest in json.loads((P/'frozen-hashes.json').read_text()).items():
    assert hashlib.sha256((P/name).read_bytes()).hexdigest()==digest,name
names=['page-8.png','page-11.png','calibration.json','grid-control-overlay.png','grid-control-contexts.png','current-commune-overlay.png']
before={n:(P/n).read_bytes() for n in names}
subprocess.run([sys.executable,str(P/'calibrate.py')],check=True)
assert all((P/n).read_bytes()==data for n,data in before.items())
r=json.loads(before['calibration.json'])
assert len(r['raster_layers'])==15
assert 5.56<r['check_rmse_m']<5.57 and 6.27<r['check_max_m']<6.28
assert len([v for v in r['residuals'] if v['split']=='check'])==2
assert not r['independent_ground_accuracy_verified'] and r['receiver_release']=='not_ready'
s=json.loads((P/'semantic-review.json').read_text())
assert len(s['destinations'])==len(s['broad_areas'])==15
assert len({v['id'] for v in s['destinations']})==15
with pymupdf.open(P/'../source-review/part-2.pdf') as pdf:
    for xref in [99,100]:
        raw=pdf.extract_image(xref)['image']
        assert raw==(P/f'legend-xref-{xref}.jpeg').read_bytes()
for v in s['destinations']:
    im=Image.open(P/f"legend-xref-{v['symbol_xref']}.jpeg")
    x1,y1,x2,y2=v['symbol_bbox_native_pixels']
    assert 0<=x1<x2<=im.width and 0<=y1<y2<=im.height
assert s['receiver_release']=='not_ready'
print('Verified complete raster composite, grid/check replay, 15 legend symbols and no release.')
