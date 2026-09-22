import hashlib,json,math,ast,numpy as np,pymupdf as fitz
from pathlib import Path
from shapely.geometry import Polygon,box,mapping,shape
from shapely.ops import unary_union
from shapely.affinity import affine_transform
from PIL import Image,ImageDraw
p=Path(__file__).parent;old=p.parent/'aigle';tree=ast.parse((old/'extract_source_paths.py').read_text());exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('flatten','polygon')],type_ignores=[]),'decoder','exec'))
assert hashlib.sha256((p/'source.pdf').read_bytes()).hexdigest()=='bbd1d3d0474d5f491ac4cc17b05d7bf1c7912861d9f93f9347c719bf7522fedc', 'exact source PDF required'
a=json.load(open(old/'alignment/alignment.json'));page=fitz.open(p/'source.pdf')[18];ds=page.get_drawings(extended=True);angle,scale,tx,ty=a['parameters'];co=math.cos(angle);si=math.sin(angle);o=a['pdf_origin_y_flipped'];q=a['reference_origin_lv95'];h=page.rect.height;m=[scale*co,scale*si,scale*si,-scale*co,q[0]+tx-scale*co*o[0]-scale*si*(h-o[1]),q[1]+ty-scale*si*o[0]+scale*co*(h-o[1])]
def rings(items):
 parts=[];cur=[];end=None
 for it in items:
  if it[0] not in ('l','c'):raise ValueError('unsupported_operator')
  start=list(it[1])
  if cur and start!=end:parts.append(cur);cur=[]
  cur.append(it);end=list(it[-1])
 if cur:parts.append(cur)
 result=[polygon(it) for it in parts]
 for i,x in enumerate(result):
  for y in result[:i]:
   if x.intersection(y).area>0:raise ValueError('overlapping_or_nested_rings_require_winding_review')
 return result
selected={3317:('built_street_front','Front de rue bâti'),3463:('limit_growth','Limiter la croissance'),3718:('villa_mutation','Accompagner la mutation de la zone villas'),7143:('built_street_front','Front de rue bâti'),7290:('limit_growth','Limiter la croissance'),7545:('villa_mutation','Accompagner la mutation de la zone villas')};stack=[];fs=[];ledger=[]
for i,d in enumerate(ds):
 stack=[z for z in stack if z['level']<d['level']]
 if i in selected:
  key,label=selected[i]
  try:rs=rings(d['items'])
  except ValueError as e:
   ledger.append({'index':i,'key':key,'outcome':str(e)})
   if d['type'] in ('group','clip'):stack.append(d)
   continue
  g=unary_union(rs);contexts=[]
  for z in stack:
   if z['type']=='clip':
    if len(z['items'])!=1 or z['items'][0][0]!='re':raise ValueError('unsupported_ancestor_clip')
    rect=list(z['items'][0][1]);g=g.intersection(box(*rect));contexts.append(rect)
  g=g.intersection(box(*page.rect));entry={'index':i,'key':key,'source_type':d['type'],'ring_count':len(rs),'even_odd':d.get('even_odd'),'clip_rectangles':contexts,'visible_area_pdf':g.area};ledger.append(entry)
  if g.is_empty:entry['outcome']='fully_clipped_out';continue
  assert g.is_valid
  children=[]
  if d['type']=='clip':
   for child in ds[i+1:]:
    if child['level']<=d['level']:break
    if child['type']=='s':children.append({'color':list(child['color']) if child.get('color') else None,'width':child.get('width'),'opacity':child.get('stroke_opacity')})
   assert children
  entry.update(outcome='selected',children=children,fill=d.get('fill'),effective_opacity=(d.get('fill_opacity') or 1)*math.prod(z['opacity'] for z in stack if z['type']=='group'),blends=[z.get('blendmode') for z in stack if z['type']=='group'])
  world=affine_transform(g,m);fs.append({'key':key,'label':label,'page':19,'category':key,'path_ids':[i],'properties':[entry],'source_paths':[{'id':i,'geometry_pdf':mapping(g),'geometry_lv95':mapping(world)}],'geometry_lv95':mapping(world),'bounds_lv95':list(world.bounds),'invalid_reference_envelope_overlap':[]})
  im=Image.open(p/'page-19.png').convert('RGB');dr=ImageDraw.Draw(im);sx=im.width/page.rect.width;sy=im.height/page.rect.height
  for z in list(g.geoms) if g.geom_type=='MultiPolygon' else [g]:dr.line([(x*sx,y*sy) for x,y in z.exterior.coords],fill='magenta',width=3)
  dr.rectangle((0,0,im.width,30),fill='white');dr.text((8,8),f'{key}: explicit source support, {len(rs)} disjoint rings',fill='black');im.save(p/(key+'.jpg'),quality=90)
 if d['type'] in ('group','clip'):stack.append(d)
(p/'groups.json').write_text(json.dumps(fs));(p/'clip-accounting.json').write_text(json.dumps(ledger,indent=2));(p/'transform.json').write_text(json.dumps({'affine':m,'alignment':a}));print(json.dumps([{'key':x['key'],'sourcepaths':x['path_ids'],'area':shape(x['geometry_lv95']).area} for x in fs]));print([(x['index'],x.get('ring_count'),x['outcome']) for x in ledger])
