import pathlib,json,math,numpy as np,pymupdf as fitz
from shapely.geometry import Polygon,box,mapping
from shapely.affinity import affine_transform
import argparse,hashlib
ap=argparse.ArgumentParser();ap.add_argument('--pdf',required=True);args=ap.parse_args()
root=pathlib.Path(__file__).resolve().parent;a=json.load(open(root/'alignment.json'))
assert hashlib.sha256(pathlib.Path(args.pdf).read_bytes()).hexdigest()==a['document_sha256'], 'Unreviewed source bytes'
page=fitz.open(args.pdf)[98]
def flatten(p0,p1,p2,p3,depth=0):
 p0,p1,p2,p3=map(np.array,(p0,p1,p2,p3));v=p3-p0;length=np.linalg.norm(v)
 deviation=max(abs((v[0]*(p1-p0)[1]-v[1]*(p1-p0)[0])),abs((v[0]*(p2-p0)[1]-v[1]*(p2-p0)[0])))/length if length>1e-9 else max(np.linalg.norm(p1-p0),np.linalg.norm(p2-p0))
 if deviation<=.03:return [p0.tolist(),p3.tolist()]
 if depth>=20:raise ValueError('curve_flatten_limit')
 q0=(p0+p1)/2;q1=(p1+p2)/2;q2=(p2+p3)/2;r0=(q0+q1)/2;r1=(q1+q2)/2;mid=(r0+r1)/2
 return flatten(p0,q0,r0,mid,depth+1)[:-1]+flatten(mid,r1,q2,p3,depth+1)
def polygon(items):
 pts=[]
 for it in items:
  if it[0] not in ('l','c'):raise ValueError('unreviewed_path_operator')
  start=list(it[1]);end=list(it[-1])
  if pts and pts[-1]!=start:raise ValueError('multiple_contours_require_review')
  if not pts:pts.append(start)
  pts.extend([end] if it[0]=='l' else flatten(*[list(p) for p in it[1:]])[1:])
 g=Polygon(pts)
 if not g.is_valid:raise ValueError('invalid_source_polygon')
 return g
stack=[];features=[];angle,scale,tx,ty=a['parameters'];cos=math.cos(angle);sin=math.sin(angle);P0=a['pdf_origin_y_flipped'];Q0=a['lv95_origin']
# Top-left PDF -> Y-flipped centred PDF -> LV95 similarity.
m=[scale*cos,scale*sin,scale*sin,-scale*cos,Q0[0]+tx-scale*cos*P0[0]-scale*sin*(page.rect.height-P0[1]),Q0[1]+ty-scale*sin*P0[0]+scale*cos*(page.rect.height-P0[1])]
for index,d in enumerate(page.get_drawings(extended=True)):
 stack=[s for s in stack if s['level']<d['level']]
 if d['type'] in ('group','clip'):stack.append(d);continue
 if not d.get('fill') or d['rect'].x1>=855:continue
 if not all(abs(x-y)<.002 for x,y in zip(d['fill'],(.943,.332,.195))):continue
 opacity=d['fill_opacity']*math.prod(s['opacity'] for s in stack if s['type']=='group')
 if abs(opacity-.44)>.002:continue
 if any(s.get('blendmode','Normal')!='Normal' for s in stack):raise ValueError('unreviewed_blend_mode')
 g=polygon(d['items'])
 for s in stack:
  if s['type']=='clip':
   if len(s['items'])!=1 or s['items'][0][0]!='re':raise ValueError('unreviewed_clip_shape')
   g=g.intersection(box(*list(s['items'][0][1])))
 world=affine_transform(g,m)
 features.append({'type':'Feature','properties':{'extended_drawing_index':index,'page_number':99,'label':"Secteur destiné principalement à l’habitation - densité moyenne (délimitation indicative)",'effective_opacity':opacity,'source_fill':list(d['fill']),'status':'review_required','reservation':'Cantonal reservation on residential/mixed-zone resizing principles, chapter2.2/pages21-22.','curve_flattening_tolerance_pdf_points':.03},'geometry':mapping(world)})
assert len(features)==6
(root/'medium-density-lv95.geojson').write_text(json.dumps({'type':'FeatureCollection','crs':{'type':'name','properties':{'name':'EPSG:2056'}},'features':features},ensure_ascii=False,indent=2))
print('features',len(features),'area_m2',sum(__import__('shapely').geometry.shape(f['geometry']).area for f in features))
