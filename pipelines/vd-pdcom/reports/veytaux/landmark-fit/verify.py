from pathlib import Path
import json,hashlib,numpy as np
from shapely.geometry import Polygon
p=Path(__file__).resolve().parent;c=json.loads((p/'controls.json').read_text());f=json.loads((p/'fit.json').read_text());assert hashlib.sha256((p/'controls.json').read_bytes()).hexdigest()==f['controls_sha256']
assert hashlib.sha256((p/c['display_image']).read_bytes()).hexdigest()==c['display_image_sha256']
refs={}
for file in ['villa-footprints.json','villa-neighbor-parcels.json']:
 for row in json.loads((p.parent/'cadastral-controls'/file).read_text())['features']:refs[row['attributes']['OBJECTID']]=row
for item,objectid,index in zip(c['controls'],[46315544]*3+[52058008,52058008,46315544],[19,14,13,14,0,28]):assert np.allclose(item['lv95'],refs[objectid]['geometry']['rings'][0][index],rtol=0,atol=1e-8)
M=np.array(f['matrix']);T=np.array(f['translation']);assert np.linalg.det(M)<0;assert np.allclose(M.T@M,np.eye(2)*f['metres_per_pixel']**2)
for item in f['results']:
 predicted=M@item['pixel']+T;assert np.allclose(predicted,item['predicted_lv95'],rtol=0,atol=1e-8);assert abs(np.linalg.norm(predicted-item['lv95'])-item['error_m'])<1e-8
assert f['check']['within5m']==1 and f['check']['rmse_m']>5 and not f['receiver_release']
g=json.loads((p/'pixel-outline.json').read_text());poly=Polygon(g['ring']);assert poly.is_valid and poly.area==g['area_pixel2'] and not g['geographic_release']
print('Frozen control identities, similarity equations, check errors and source-only polygon verified')
