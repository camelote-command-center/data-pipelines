from pathlib import Path
import json,hashlib,numpy as np
from PIL import Image,ImageDraw
p=Path(__file__).resolve().parent;v=json.loads((p/'controls.json').read_text());source=p/v['display_image'];assert hashlib.sha256(source.read_bytes()).hexdigest()==v['display_image_sha256']
controls=v['controls'];train=[c for c in controls if c['role']=='train'];check=[c for c in controls if c['role']=='check']
origin=np.array([2561000.,1140000.]);A=[];B=[]
for c in train:
 x,y=c['pixel'];e,n=np.array(c['lv95'])-origin;A.extend([[x,y,1,0],[-y,x,0,1]]);B.extend([e,n])
a,b,tx,ty=np.linalg.lstsq(A,B,rcond=None)[0];M=np.array([[a,b],[b,-a]]);T=origin+np.array([tx,ty]);results=[]
for c in controls:
 predicted=M@c['pixel']+T;delta=predicted-c['lv95'];results.append({**c,'predicted_lv95':predicted.tolist(),'delta_m':delta.tolist(),'error_m':float(np.linalg.norm(delta))})
summary={'matrix':M.tolist(),'translation':T.tolist(),'metres_per_pixel':float(np.hypot(a,b)),'controls_sha256':hashlib.sha256((p/'controls.json').read_bytes()).hexdigest(),'results':results}
for role in ['train','check']:
 r=[c['error_m'] for c in results if c['role']==role];summary[role]={'n':len(r),'rmse_m':float(np.sqrt(np.mean(np.square(r)))),'max_m':max(r),'within5m':sum(x<5 for x in r)}
summary['interpretation']='Exploratory visual-candidate fit; narrow one-building training footprint, two external stream/road checks and one same-building check. Historical feature stability and independent ground accuracy not established. No control adjustment after fit.';summary['receiver_release']=False
(p/'fit.json').write_text(json.dumps(summary,indent=2)+'\n')
im=Image.open(source).convert('RGB');draw=ImageDraw.Draw(im);inverse=np.linalg.inv(M)
for name,color in [('villa-neighbor-parcels','#00aaee'),('villa-footprints','#e600bb')]:
 data=json.loads((p.parent/'cadastral-controls'/f'{name}.json').read_text())
 for f in data['features']:
  for ring in f['geometry']['rings']:
   points=[tuple(inverse@(np.array(pt)-T)) for pt in ring];draw.line(points,fill=color,width=1)
for c in controls:
 x,y=c['pixel'];col='red' if c['role']=='train' else 'blue';draw.ellipse((x-4,y-4,x+4,y+4),outline=col,width=2);draw.text((x+8,y-15),c['id'],fill=col)
im.save(p/'reference-overlay.png');print(json.dumps({k:summary[k] for k in ['matrix','translation','metres_per_pixel','train','check']},indent=2))
