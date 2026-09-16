"""Calibrate the original composited raster against its printed coordinate grid."""
import hashlib
import json
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw
import pymupdf

P = Path(__file__).resolve().parent
c = json.loads((P / 'controls.json').read_text())
raw = (P / c['source_pdf']).read_bytes()
assert hashlib.sha256(raw).hexdigest() == c['source_sha256']
with pymupdf.open(stream=raw, filetype='pdf') as pdf:
    page = pdf[c['pdf_page']-1]
    assert not page.get_drawings()
    page.get_pixmap(matrix=pymupdf.Matrix(c['render_scale'], c['render_scale'])).save(P/'page-8.png')
    pdf[10].get_pixmap(matrix=pymupdf.Matrix(4,4)).save(P/'page-11.png')
    layers = [{'xref':i[0], 'width':i[2], 'height':i[3], 'filter':i[8],
               'placements_pdf_points':[list(r) for r in page.get_image_rects(i[0])]} for i in page.get_images(full=True)]
train = [r for r in c['points'] if r['split']=='train']
X = np.array([[*r['pixel'],1] for r in train])
Y = np.array([r['lv03'] for r in train])
A = np.linalg.lstsq(X,Y,rcond=None)[0].T
residuals=[]
for r in c['points']:
    calculated=np.r_[r['pixel'],1]@A.T
    delta=calculated-np.array(r['lv03'])
    residuals.append({**r,'calculated_lv03':calculated.tolist(),'delta_m':delta.tolist(),'distance_m':float(np.linalg.norm(delta))})
checks=[r['distance_m'] for r in residuals if r['split']=='check']
result={'status':'printed_grid_research_calibration','affine_pixel_to_lv03':A.tolist(),
        'residuals':residuals,'check_rmse_m':float(np.sqrt(np.mean(np.square(checks)))),
        'check_max_m':max(checks),'independent_ground_accuracy_verified':False,
        'receiver_release':'not_ready','raster_layers':layers,
        'limits':['Six manually read grid intersections; only four fitted, two withheld.',
                  'Printed-grid consistency does not establish real-world accuracy or current parcel rights.',
                  'Scan deformation and manual line reading remain unresolved.',
                  'Page consists of composited image tiles; no single embedded color tile is the complete map.']}
(P/'calibration.json').write_text(json.dumps(result,indent=2)+'\n')
im=Image.open(P/'page-8.png').convert('RGB');assert list(im.size)==c['render_size'];draw=ImageDraw.Draw(im)
for r in c['points']:
    x,y=r['pixel'];color='blue' if r['split']=='train' else 'red'
    draw.ellipse((x-12,y-12,x+12,y+12),outline=color,width=4);draw.text((x+14,y-10),r['id'],fill=color,stroke_width=1,stroke_fill='white')
im.resize((1190,1684)).save(P/'grid-control-overlay.png')
# Preserve each landmark context at a common square aspect ratio.
canvas=Image.new('RGB',(720,780),'white');d=ImageDraw.Draw(canvas);base=Image.open(P/'page-8.png')
for i,r in enumerate(c['points']):
    x,y=r['pixel'];col=i%3;row=i//3
    canvas.paste(base.crop((x-30,y-30,x+30,y+30)).resize((240,240)),(col*240,row*390+30))
    d.text((col*240,row*390+5),f"{r['id']} {r['split']} {r['pixel']}",fill='black')
canvas.save(P/'grid-control-contexts.png')
print(json.dumps({'affine':A.tolist(),'check_rmse_m':result['check_rmse_m'],'check_max_m':result['check_max_m']}))
from shapely.geometry import shape
ref=json.loads((P/'current-commune-lv03.json').read_text());geom=shape(ref['geometry'])
parts=[geom] if geom.geom_type=='Polygon' else list(geom.geoms)
im=Image.open(P/'page-8.png').convert('RGB');draw=ImageDraw.Draw(im);inv=np.linalg.inv(A[:,:2])
for part in parts:
    for ring in [part.exterior,*part.interiors]:
        xy=(np.array(ring.coords)-A[:,2])@inv.T
        draw.line([tuple(v) for v in xy],fill='blue',width=4)
im.resize((1190,1684)).save(P/'current-commune-overlay.png')
