"""Calibrate the adjacent PGA's printed LV03 grid, not the SDAN image."""
import hashlib,json,io
from pathlib import Path
import numpy as np
from PIL import Image,ImageDraw
import pymupdf
P=Path(__file__).resolve().parent;d=json.loads((P/'controls.json').read_text());pdf=P/d['source_pdf'];assert hashlib.sha256(pdf.read_bytes()).hexdigest()==d['source_sha256']
doc=pymupdf.open(pdf);asset=doc.extract_image(d['image_xref']);assert [asset['width'],asset['height']]==d['native_image_size']
im=Image.open(io.BytesIO(asset['image'])).transpose(Image.Transpose.ROTATE_270).resize(tuple(d['review_image_size'])).convert('RGB')
coeff=[];residuals=[]
for axis in ['E','N']:
 rows=[(pt,line['value']) for line in d['grid_lines'] if line['axis']==axis and line['split']=='train' for pt in line['points']];X=np.array([[*pt,1] for pt,val in rows]);y=np.array([val for pt,val in rows]);a=np.linalg.lstsq(X,y,rcond=None)[0];coeff.append(a)
 for line in d['grid_lines']:
  if line['axis']!=axis:continue
  for pt in line['points']:residuals.append({'axis':axis,'split':line['split'],'point':pt,'grid_value':line['value'],'axis_residual_m':float(np.dot([*pt,1],a)-line['value'])})
A=np.array(coeff);check=np.array([r['axis_residual_m'] for r in residuals if r['split']=='check']);qa={'status':'adjacent_PGA_grid_calibration_only','affine_pixel_to_lv03':A.tolist(),'determinant':float(np.linalg.det(A[:,:2])),'residuals':residuals,'check_axis_rmse_m':float(np.sqrt(np.mean(check**2))),'check_axis_max_abs_m':float(max(abs(check))),'independent_geographic_accuracy_verified':False,'SDAN_alignment_verified':False,'receiver_release':'not_ready'}
assert np.linalg.det(A[:,:2])<0
(P/'calibration.json').write_text(json.dumps(qa,indent=2)+'\n')
# Inverse-project official footprint outlines for a visual comparison only.
ref=json.loads((P/'footprint-lines-lv03.json').read_text());draw=ImageDraw.Draw(im);inv=np.linalg.inv(A[:,:2])
for line in ref['geometry']['coordinates']:
 xy=(np.array(line)-A[:,2])@inv.T
 draw.line([tuple(pt) for pt in xy],fill=(255,0,160),width=1)
for line in d['grid_lines']:
 color=(0,60,255) if line['split']=='train' else (255,70,0)
 draw.line([tuple(pt) for pt in line['points']],fill=color,width=1)
 for x,y in line['points']:draw.ellipse((x-3,y-3,x+3,y+3),outline=color,width=2)
im.save(P/'reference-overlay.png')
print(json.dumps({'check_axis_rmse_m':qa['check_axis_rmse_m'],'check_axis_max_abs_m':qa['check_axis_max_abs_m'],'transform':A.tolist()}))
