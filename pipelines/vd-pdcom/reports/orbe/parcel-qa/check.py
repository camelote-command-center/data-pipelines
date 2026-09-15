"""Source-scoped official parcel QA; does not create runtime candidate rows."""
import json,pathlib,requests,hashlib,re
from collections import Counter
from shapely.geometry import shape,mapping
root=pathlib.Path(__file__).resolve().parent
fc=json.loads((root.parent/'alignment/priority-band-lv95.geojson').read_text());variants={f['properties']['boundary_variant']:shape(f['geometry']) for f in fc['features']};outer=variants['outer'];bounds=[outer.bounds[0]-15,outer.bounds[1]-15,outer.bounds[2]+15,outer.bounds[3]+15]
endpoint='https://ags.map.vd.ch/ags/rest/services/API/APIGeo/MapServer/21/query';params={'geometry':','.join(map(str,bounds)),'geometryType':'esriGeometryEnvelope','inSR':2056,'spatialRel':'esriSpatialRelIntersects','where':'NO_COM_FED=5757'}
r=requests.get(endpoint,params=dict(params,returnCountOnly='true',f='json'),timeout=45);r.raise_for_status();count=r.json()['count'];count_url=r.url;features=[];pages=[]
if count<=0:raise ValueError('empty_reference_query_requires_review')
for offset in range(0,count,1000):
 r=requests.get(endpoint,params=dict(params,outFields='OBJECTID,EGRID,NUMERO,NO_COM_FED,GENRE_TXT,SUPERFICIE_MO,SUPERFICIE_RF',outSR=2056,f='geojson',orderByFields='OBJECTID',resultOffset=offset,resultRecordCount=1000),timeout=45);r.raise_for_status();data=r.json();features+=data['features'];pages.append({'url':r.url,'response_sha256':hashlib.sha256(r.content).hexdigest(),'rows':len(data['features'])})
if len(features)!=count or len({f['properties']['EGRID'] for f in features})!=count:raise ValueError('reference_count_or_identity_mismatch')
for f in features:
 p=f['properties'];g=shape(f['geometry'])
 if p['NO_COM_FED']!=5757 or not re.fullmatch(r'CH\d{12}',p['EGRID']) or g.geom_type not in ('Polygon','MultiPolygon') or not g.is_valid or g.is_empty:raise ValueError('invalid_reference_feature')
(root/'official-parcels.geojson').write_text(json.dumps({'type':'FeatureCollection','features':features}))
results={};intersections=[]
for name,g in {**variants,'inner_minus5m':variants['inner'].buffer(-5),'outer_plus5m':outer.buffer(5),'inner_minus10m':variants['inner'].buffer(-10),'outer_plus10m':outer.buffer(10)}.items():
 rows=[]
 for f in features:
  p=f['properties'];pg=shape(f['geometry']);overlap=g.intersection(pg)
  if overlap.area<=0:continue
  row={'egrid':p['EGRID'],'numero':p['NUMERO'],'kind':p['GENRE_TXT'],'overlap_m2':overlap.area,'parcel_area_m2':pg.area,'overlap_fraction':overlap.area/pg.area};rows.append(row)
  if name in ('inner','outer'):intersections.append({'type':'Feature','properties':dict(row,variant=name),'geometry':mapping(overlap)})
 results[name]={'rows':len(rows),'kinds':dict(Counter(r['kind'] for r in rows)),'under1m2':sum(r['overlap_m2']<1 for r in rows),'under1pct':sum(r['overlap_fraction']<.01 for r in rows),'pairs':rows}
inner_ids={r['egrid'] for r in results['inner']['pairs']};outer_ids={r['egrid'] for r in results['outer']['pairs']};assert inner_ids<=outer_ids
report={'checked_at':'2026-09-15','source_document_id':'464c3687-bdd2-55b4-9e35-f8ece05099cb','source_sha256':'8b78d42eb83210b6b63404dd8b4270fedd15d1b7e3d65d2a403d6edcee654a3c','commune_bfs':5757,'count_url':count_url,'bounds_lv95':bounds,'reference_count':count,'response_pages':pages,'variants':results,'outer_only_egrids':sorted(outer_ids-inner_ids),'limits':'Research intersections only. Band edges and 5/10m buffers are sensitivity scenarios, not surveyed error bounds. Land and DDP rights may overlap; do not sum as exclusive land area. Source version/currentness and other categories remain unresolved.'}
(root/'parcel-review.json').write_text(json.dumps(report,indent=2)+'\n');(root/'intersections-lv95.geojson').write_text(json.dumps({'type':'FeatureCollection','coordinate_system':'EPSG:2056','features':intersections}));print(json.dumps({'reference_count':count,'variants':{k:{a:b for a,b in v.items() if a!='pairs'} for k,v in results.items()},'outer_only':len(outer_ids-inner_ids)}))
