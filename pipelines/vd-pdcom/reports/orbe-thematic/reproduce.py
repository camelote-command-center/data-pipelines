import pymupdf as fitz,json,math,hashlib
from pathlib import Path
from shapely.geometry import Polygon,box,mapping
from shapely.ops import unary_union
from shapely.affinity import affine_transform
from PIL import Image,ImageDraw
p=Path(__file__).parent;sha='8b78d42eb83210b6b63404dd8b4270fedd15d1b7e3d65d2a403d6edcee654a3c';assert hashlib.sha256((p/'source.pdf').read_bytes()).hexdigest()==sha
pg=fitz.open(p/'source.pdf')[0];pg.get_pixmap(matrix=fitz.Matrix(.7,.7)).save(p/'page1.png');ds=pg.get_drawings(extended=True);old=p.parent/'orbe';a=json.load(open(old/'alignment/alignment.json'));ang,s,tx,ty=a['parameters'];c=math.cos(ang);z=math.sin(ang);o=a['source_origin_y_flipped'];q=a['reference_origin_lv95'];m=[s*c,s*z,s*z,-s*c,q[0]+tx-s*c*o[0]+s*z*o[1],q[1]+ty-s*z*o[0]-s*c*o[1]]
classes={(.937,.561,.847):('mixed_medium','À moyen terme : mixte'),(.859,.925,.651):('sports_expand','Infrastructures et équipements sportifs et de loisirs à étendre'),(.384,.753,1.):('public_develop','Offre en équipements publics à développer'),(.624,.553,.435):('infrastructure_adapt','Infrastructures à adapter aux nouveaux besoins communaux')};groups={};ledger=[];stack=[]
for i,d in enumerate(ds):
 stack=[x for x in stack if x['level']<d['level']];col=tuple(round(x,3) for x in d['fill']) if d.get('fill') else None;r=d.get('rect')
 if col in classes and r and 750<r.x0 and r.x1<1500:
  k,label=classes[col];e={'index':i,'key':k,'fill':d['fill'],'page':1,'source_sha256':sha,'category':k,'clip_ancestors':[]}
  try:
   if r.y0<400 or r.y1>1150:raise ValueError('outside_original_control_roi')
   rings=[];pts=[]
   for it in d['items']:
    if it[0]=='re':
     if pts:rings.append(Polygon(pts));pts=[]
     rings.append(box(*it[1]));continue
    if it[0]!='l':raise ValueError('nonlinear_operator')
    if pts and math.dist(pts[-1],it[1])>.001:rings.append(Polygon(pts));pts=[]
    if not pts:pts=[tuple(it[1])]
    pts.append(tuple(it[2]))
   if pts:rings.append(Polygon(pts))
   if any(not g.is_valid or g.is_empty for g in rings):raise ValueError('invalid_source_ring')
   g=rings[0]
   for other in rings[1:]:
    if not d['even_odd'] and g.intersection(other).area>0:raise ValueError('nonzero_winding_overlap')
    g=g.symmetric_difference(other) if d['even_odd'] else g.union(other)
   for ancestor in stack:
    if ancestor['type']=='clip':
     it=ancestor['items']
     if len(it)==1 and it[0][0]=='re':clip=box(*it[0][1])
     elif all(z[0]=='l' for z in it) and all(math.dist(it[j][-1],it[j+1][1])<.001 for j in range(len(it)-1)):clip=Polygon([tuple(it[0][1])]+[tuple(z[2]) for z in it])
     else:raise ValueError('unsupported_clip_ancestor')
     if not clip.is_valid:raise ValueError('invalid_clip_ancestor')
     g=g.intersection(clip);e['clip_ancestors'].append(mapping(clip))
   if not g.is_valid or g.is_empty or g.area<=0:raise ValueError('empty_or_invalid_clipped')
   lv=affine_transform(g,m);assert lv.is_valid
   e['status']='selected';groups.setdefault(k,{'key':k,'label':label,'category':k,'page':1,'path_ids':[],'properties':[],'source_paths':[]})
   f=groups[k];f['path_ids'].append(i);f['properties'].append(e);f['source_paths'].append({'id':i,'geometry_pdf':mapping(g),'geometry_lv95':mapping(lv)})
  except Exception as ex:e['status']=str(ex)
  ledger.append(e)
 if d['type'] in ('clip','group'):stack.append(d)
from shapely.geometry import shape
for k,f in groups.items():
 g=unary_union([shape(z['geometry_lv95']) for z in f['source_paths']]);f.update(geometry_lv95=mapping(g),bounds_lv95=list(g.bounds),invalid_reference_envelope_overlap=[])
 im=Image.open(p/'page1.png').convert('RGB');dr=ImageDraw.Draw(im)
 for item in f['source_paths']:
  x=shape(item['geometry_pdf']);ps=[x] if x.geom_type=='Polygon' else x.geoms
  for poly in ps:dr.line([(x*.7,y*.7) for x,y in poly.exterior.coords],fill='magenta',width=2)
 im.save(p/(k+'.jpg'))
(p/'groups.json').write_text(json.dumps(list(groups.values())));(p/'accounting.json').write_text(json.dumps(ledger));(p/'transform.json').write_text(json.dumps({'affine':m,'alignment':a}));print([(k,len(f['path_ids'])) for k,f in groups.items()]);print([(x['index'],x['status']) for x in ledger if x['status']!='selected'])
