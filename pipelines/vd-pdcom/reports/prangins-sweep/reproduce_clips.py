import ast,json,math,numpy as np,pymupdf as fitz
from pathlib import Path
from shapely.geometry import Polygon,box,mapping,shape
from shapely.ops import unary_union
from shapely.affinity import affine_transform
from PIL import Image,ImageDraw
p=Path(__file__).parent
import sys,hashlib
sys.path.insert(0,str(p.parents[1]))
from prangins_source_support import compound
assert hashlib.sha256((p/'source.pdf').read_bytes()).hexdigest()=='a4079a88201b716ede0793150102306f8b1faddb55c391a7c2c1696244a96d02'
pg=fitz.open(p/'source.pdf')[0];ds=pg.get_drawings(extended=True);m=json.load(open(p/'transform.json'))['affine'];stack=[];features=[];ledger=[]
for i,d in enumerate(ds):
 stack=[z for z in stack if z['level']<d['level']]
 if d['type']=='clip' and not(len(d['items'])==1 and d['items'][0][0]=='re'):
  e={'index':i,'layer':d.get('layer'),'items':len(d['items'])}
  try:
   g=compound(d['items'],d.get('even_odd',False));xmin,ymin,xmax,ymax=g.bounds
   if xmax>2388 or (xmin>=1790 and ymin<740):raise ValueError('legend_inset_or_margin')
   for z in stack:
    if z['type']=='clip':
     clip=box(*z['items'][0][1]) if len(z['items'])==1 and z['items'][0][0]=='re' else compound(z['items'],z.get('even_odd',False));g=g.intersection(clip)
   if g.is_empty or not g.is_valid:raise ValueError('empty_or_invalid')
   children=[]
   for j,z in enumerate(ds[i+1:],i+1):
    if z['level']<=d['level']:break
    if z['type'] not in ('clip','group'):children.append({'index':j,'type':z['type'],'fill':z.get('fill'),'color':z.get('color')})
   e['status']='candidate';f={'path_id':i,'layer':d.get('layer'),'children':children,'geometry_pdf':mapping(g),'geometry_lv95':mapping(affine_transform(g,m))};features.append(f)
   im=Image.open(p/'source.png').convert('RGB');dr=ImageDraw.Draw(im)
   for z in g.geoms if g.geom_type=='MultiPolygon' else [g]:dr.line([(x*.5,y*.5) for x,y in z.exterior.coords],fill='magenta',width=3)
   im.save(p/f'clip-{i}.jpg')
  except Exception as ex:e['status']=str(ex)
  ledger.append(e)
 if d['type'] in ('group','clip'):stack.append(d)
(p/'clip-features.json').write_text(json.dumps(features));(p/'clip-accounting.json').write_text(json.dumps(ledger));print([(x['path_id'],x['layer'],len(x['children']),round(shape(x['geometry_lv95']).area)) for x in features]);print([x for x in ledger if x['status']!='candidate'])
