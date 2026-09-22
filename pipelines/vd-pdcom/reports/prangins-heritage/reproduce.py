import numpy as np
import pymupdf as fitz,json,math,ast,collections,hashlib
from pathlib import Path
from shapely.geometry import Polygon,shape,mapping,box
from shapely.ops import unary_union
from shapely.affinity import affine_transform
from PIL import Image,ImageDraw
p=Path(__file__).parent;old=p.parent/'prangins';a=json.loads((old/'alignment.json').read_text());doc=fitz.open(p/'source.pdf');page=doc[0];h=page.rect.height;assert hashlib.sha256((p/'source.pdf').read_bytes()).hexdigest()=='a4079a88201b716ede0793150102306f8b1faddb55c391a7c2c1696244a96d02';page.get_pixmap(matrix=fitz.Matrix(.5,.5)).save(p/'source.png')
# Same strict single-contour adaptive decoder already reviewed for Epalinges.
base=p.parent/'epalinges/alignment/extract_medium_density.py';tree=ast.parse(base.read_text());exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('flatten','polygon')],type_ignores=[]),'decoder','exec'))
angle,scale,tx,ty=a['parameters'];c=math.cos(angle);s=math.sin(angle);o=a['pdf_origin'];q=a['reference_origin'];m=[scale*c,scale*s,scale*s,-scale*c,q[0]+tx-scale*c*o[0]-scale*s*(h-o[1]),q[1]+ty-scale*s*o[0]+scale*c*(h-o[1])]
stack=[];ledger=[];groups=collections.defaultdict(list);wanted={'recensement archi'}
for i,d in enumerate(page.get_drawings(extended=True)):
 stack=[z for z in stack if z['level']<d['level']];r={'index':i,'layer':d.get('layer'),'type':d['type']};ledger.append(r)
 if d['type'] in ('group','clip'):stack.append(d);r['outcome']='structural';continue
 if d.get('layer') not in wanted or not d.get('fill'):r['outcome']='other_layer_or_stroke';continue
 if d['rect'].x1>2388 or (d['rect'].x0>=1790 and d['rect'].y0<740):r['outcome']='legend_inset_or_margin';continue
 try:
  g=polygon(d['items'])
  for z in stack:
   if z['type']=='clip':
    if len(z['items'])!=1 or z['items'][0][0]!='re':raise ValueError('unsupported_clip')
    g=g.intersection(box(*list(z['items'][0][1])))
  if g.is_empty or not g.is_valid:raise ValueError('invalid_or_empty')
  fill=tuple(round(v,3) for v in d['fill']);opacity=d['fill_opacity']*math.prod(z['opacity'] for z in stack if z['type']=='group');key=d['layer']+'|'+','.join(map(str,fill))
  f={'path_id':i,'layer':d['layer'],'fill':fill,'opacity':opacity,'geometry_pdf':mapping(g),'geometry_lv95':mapping(affine_transform(g,m))};groups[key].append(f);r['outcome']='candidate';r['key']=key
 except (ValueError,TypeError) as e:r['outcome']=str(e)
result=[]
for n,(key,fs) in enumerate(groups.items()):
 g=unary_union([shape(f['geometry_lv95']) for f in fs]);im=Image.open(p/'source.png').convert('RGB');dr=ImageDraw.Draw(im);sx=im.width/page.rect.width;sy=im.height/page.rect.height
 for f in fs:
  geo=shape(f['geometry_pdf'])
  for z in list(geo.geoms) if geo.geom_type=='MultiPolygon' else [geo]:dr.line([(x*sx,y*sy) for x,y in z.exterior.coords],fill='magenta',width=3)
 dr.rectangle((0,0,1500,30),fill='white');dr.text((8,8),f'{n} {key}: {len(fs)} paths',fill='black');im.save(p/f'group-{n}.jpg',quality=88)
 result.append({'number':n,'key':key,'paths':fs,'geometry_lv95':mapping(g),'area':g.area,'valid':g.is_valid})
(p/'groups.json').write_text(json.dumps(result));(p/'path-accounting.json').write_text(json.dumps(ledger));(p/'transform.json').write_text(json.dumps({'affine':m,'alignment':a,'source_sha256':hashlib.sha256((p/'source.pdf').read_bytes()).hexdigest()}))
print(json.dumps([{'number':f['number'],'key':f['key'],'paths':len(f['paths']),'area':round(f['area']),'valid':f['valid']} for f in result],indent=2));print(collections.Counter(f['outcome'] for f in ledger))
