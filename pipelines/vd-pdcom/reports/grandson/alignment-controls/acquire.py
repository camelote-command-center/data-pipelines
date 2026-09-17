"""Acquire a count-checked official footprint snapshot for Grandson."""
import pathlib,requests,json,hashlib,datetime
p=pathlib.Path(__file__).resolve().parent;u='https://ags.map.vd.ch/ags/rest/services/API/APIGeo/MapServer/22/query';base={'f':'json','where':'NO_COM_FED=5561','outSR':'2056'}
def get(params):
 r=requests.get(u,params={**base,**params},timeout=40);r.raise_for_status();d=r.json();assert 'error' not in d,d.get('error');return d
n=get({'returnCountOnly':'true'})['count'];assert 0<n<10000
features=[];receipts=[]
for offset in range(0,n,2000):
 params={'outFields':'OBJECTID,EGID,NO_COM_FED,NUMERO,GENRE_TXT','returnGeometry':'true','orderByFields':'OBJECTID ASC','resultOffset':offset,'resultRecordCount':2000};d=get(params);features.extend(d['features']);receipts.append({'offset':offset,'count':len(d['features']),'exceededTransferLimit':d.get('exceededTransferLimit',False)})
assert len(features)==n;assert len({x['attributes']['OBJECTID'] for x in features})==n;assert all(x['attributes']['NO_COM_FED']==5561 for x in features)
assert get({'returnCountOnly':'true'})['count']==n
content=json.dumps({'spatialReference':{'wkid':2056},'features':features},ensure_ascii=False,indent=2)+'\n';(p/'official-footprints.json').write_text(content)
manifest={'source_url':u,'query':base,'outFields':'OBJECTID,EGID,NO_COM_FED,NUMERO,GENRE_TXT','retrieved_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'count_before_after':n,'pages':receipts,'sha256':hashlib.sha256(content.encode()).hexdigest(),'null_egid':sum(x['attributes']['EGID'] is None for x in features),'scope':'Current official Grandson footprint objects, not historical correspondences or parcel records. No ownership fields.'};(p/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2));print(manifest)
