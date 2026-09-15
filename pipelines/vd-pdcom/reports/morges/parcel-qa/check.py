"""Official Morges parcel intersections and sensitivity, research only."""
import json,pathlib,requests,hashlib,re,datetime
from collections import Counter
from shapely.geometry import shape,mapping
from shapely.ops import unary_union
root=pathlib.Path(__file__).resolve().parent
fc=json.loads((root.parent/'alignment/full-hatch-lv95.geojson').read_text());sectors={str(f['properties']['drawing_index']):shape(f['geometry']) for f in fc['features']};total=unary_union(list(sectors.values()));bounds=[total.bounds[0]-15,total.bounds[1]-15,total.bounds[2]+15,total.bounds[3]+15]
assert 0<bounds[2]-bounds[0]<=2000 and 0<bounds[3]-bounds[1]<=2000
endpoint='https://ags.map.vd.ch/ags/rest/services/API/APIGeo/MapServer/21/query';params={'geometry':','.join(map(str,bounds)),'geometryType':'esriGeometryEnvelope','inSR':2056,'spatialRel':'esriSpatialRelIntersects','where':'NO_COM_FED=5642'}
r=requests.get(endpoint,params=dict(params,returnCountOnly='true',f='json'),timeout=45);r.raise_for_status();count=r.json()['count'];count_url=r.url;features=[];pages=[]
if count<=0:raise ValueError('empty_reference_query_requires_review')
for offset in range(0,count,1000):
 r=requests.get(endpoint,params=dict(params,outFields='OBJECTID,EGRID,NUMERO,NO_COM_FED,GENRE_TXT,SUPERFICIE_MO,SUPERFICIE_RF',outSR=2056,f='geojson',orderByFields='OBJECTID',resultOffset=offset,resultRecordCount=1000),timeout=45);r.raise_for_status();data=r.json();features+=data['features'];pages.append({'url':r.url,'response_sha256':hashlib.sha256(r.content).hexdigest(),'rows':len(data['features'])})
if len(features)!=count or len({f['properties']['EGRID'] for f in features})!=count:raise ValueError('reference_count_or_identity_mismatch')
for f in features:
 p=f['properties'];g=shape(f['geometry'])
 if p['NO_COM_FED']!=5642 or not re.fullmatch(r'CH\d{12}',p['EGRID']) or g.geom_type not in ('Polygon','MultiPolygon') or not g.is_valid or g.is_empty:raise ValueError('invalid_reference_feature')
(root/'official-parcels.geojson').write_text(json.dumps({'type':'FeatureCollection','features':features}))
results={};intersections=[]
for name,base in sectors.items():
 variants={}
 for offset in [-10,-5,0,5,10]:
  g=base if offset==0 else base.buffer(offset);rows=[]
  for f in features:
   p=f['properties'];pg=shape(f['geometry']);overlap=g.intersection(pg)
   if overlap.area<=0:continue
   row={'egrid':p['EGRID'],'numero':p['NUMERO'],'kind':p['GENRE_TXT'],'overlap_m2':overlap.area,'parcel_area_m2':pg.area,'overlap_fraction':overlap.area/pg.area,'boundary_within10m':pg.intersects(base.boundary.buffer(10))};rows.append(row)
   if offset==0:intersections.append({'type':'Feature','properties':dict(row,drawing_index=int(name)),'geometry':mapping(overlap)})
  variants[str(offset)]={'rows':len(rows),'kinds':dict(Counter(r['kind'] for r in rows)),'under1m2':sum(r['overlap_m2']<1 for r in rows),'under1pct':sum(r['overlap_fraction']<.01 for r in rows),'pairs':rows}
 for lo,hi in zip([-10,-5,0,5],[-5,0,5,10]):assert {r['egrid'] for r in variants[str(lo)]['pairs']}<={r['egrid'] for r in variants[str(hi)]['pairs']}
 results[name]=variants
rows=[r for v in results.values() for r in v['0']['pairs']];unique={r['egrid']:r['kind'] for r in rows}
report={'checked_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_document_id':'b4deac7f-9349-5463-b2f5-0afdd2bf78e6','source_sha256':'9ac3058bc9f837696f69886815f54e65dab848bd67e62df90deb476cebd85300','commune_bfs':5642,'count_url':count_url,'bounds_lv95':bounds,'reference_count':count,'response_pages':pages,'sectors':results,'base_pairs':len(rows),'base_unique_egrids':len(unique),'base_unique_kinds':dict(Counter(unique.values())),'limits':'Research base-outline intersections only. Overprints/grouping, partial boundaries and later currentness unresolved.5/10m buffers are sensitivity scenarios, not error bounds. Land and DDP rights can overlap; no exclusive area sum or parcel entitlement implied. Adjacent-commune parcels outside scope.'}
(root/'parcel-review.json').write_text(json.dumps(report,indent=2)+'\n');(root/'intersections-lv95.geojson').write_text(json.dumps({'type':'FeatureCollection','coordinate_system':'EPSG:2056','features':intersections}));print(json.dumps({'reference_count':count,'pairs':len(rows),'unique':len(unique),'kinds':report['base_unique_kinds'],'sectors':{k:{a:{'rows':b['rows'],'under1m2':b['under1m2'],'under1pct':b['under1pct']} for a,b in v.items()} for k,v in results.items()}}))
