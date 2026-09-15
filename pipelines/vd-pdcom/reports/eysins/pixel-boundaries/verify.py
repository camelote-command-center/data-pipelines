"""Validate frozen pixel digitization and render reproducible source overlays."""
import hashlib,json
from pathlib import Path
from PIL import Image,ImageDraw
from shapely.geometry import Polygon,box
P=Path(__file__).resolve().parent;d=json.loads((P/'digitization.json').read_text());source=P/d['source_image']
assert hashlib.sha256(source.read_bytes()).hexdigest()==d['source_image_sha256']
im=Image.open(source).convert('RGB');assert list(im.size)==d['image_size'];w,h=im.size
assert d['crs'] is None and d['receiver_release']=='not_ready'
polys=[];rows=[];draw=ImageDraw.Draw(im)
colors={'medium_density_housing':(0,140,0),'activities_and_shops':(220,0,200),'possible_long_term_housing_option':(0,110,255)}
for f in d['features']:
    poly=Polygon(f['ring_pixels']);assert poly.is_valid and not poly.is_empty
    assert box(0,0,w,h).covers(poly);polys.append(poly)
    draw.line([tuple(x) for x in f['ring_pixels']],fill=colors[f['category']],width=1)
    pt=poly.representative_point();draw.text((pt.x,pt.y),f['id'],fill='black',stroke_width=1,stroke_fill='white')
    scenarios=[{'buffer_pixels':b,'area_pixels_squared':poly.buffer(b).area} for b in [-2,-1,0,1,2]]
    rows.append({'id':f['id'],'valid':True,'area_pixels_squared':poly.area,'buffer_sensitivity':scenarios})
assert len(polys)==7
for i,p in enumerate(polys):
 for q in polys[i+1:]: assert p.intersection(q).area==0
assert [f['category'] for f in d['features']].count('medium_density_housing')==5
assert sum(f['category']=='possible_long_term_housing_option' for f in d['features'])==1
qa={'source_image_sha256':d['source_image_sha256'],'digitization_sha256':hashlib.sha256((P/'digitization.json').read_bytes()).hexdigest(),'features':rows,'positive_area_overlap_pairs':0,'buffer_note':'Illustrative pixel sensitivity, not measured positional error or geographic metres.','geographic_validation':'pending','new_deliverable_sectors':0}
(P/'qa.json').write_text(json.dumps(qa,indent=2)+'\n');im.resize((w*3,h*3)).save(P/'overlay.png')
print('7 valid, disjoint, image-bounded indicative contours; 35 pixel sensitivity scenarios.')
if (P/'frozen-hashes.json').exists():
    for name,digest in json.loads((P/'frozen-hashes.json').read_text()).items():
        assert hashlib.sha256((P/name).read_bytes()).hexdigest()==digest, name
    print('Frozen reviewed artifact hashes match.')
