import json,pathlib,hashlib,requests,numpy as np
from shapely.geometry import shape,Point,Polygon,mapping
from scipy.spatial import cKDTree
root=pathlib.Path(__file__).resolve().parent;a=json.loads((root/'alignment.json').read_text());P=np.array(json.loads((root/'source-points.json').read_text()));Q=np.array([r[1:] for r in json.loads((root/'reference-points.json').read_text())]);angle,scale,tx,ty=a['parameters'];R=np.array([[np.cos(angle),-np.sin(angle)],[np.sin(angle),np.cos(angle)]]);P0=np.array(a['source_origin_y_flipped']);Q0=np.array(a['reference_origin_lv95'])
def trans(p):return (p-P0)@R.T*scale+[tx,ty]+Q0
XY=trans(P);bounds=np.r_[XY.min(0)-50,XY.max(0)+50].tolist();endpoint='https://ags.map.vd.ch/ags/rest/services/API/APIGeo/MapServer/22/query';params={'geometry':','.join(map(str,bounds)),'geometryType':'esriGeometryEnvelope','inSR':2056,'spatialRel':'esriSpatialRelIntersects','where':'1=1'}
r=requests.get(endpoint,params=dict(params,returnCountOnly='true',f='json'),timeout=45);r.raise_for_status();count=r.json()['count'];count_url=r.url;features=[];receipts=[]
for offset in range(0,count,1000):
 r=requests.get(endpoint,params=dict(params,outFields='OBJECTID,EGID,NO_COM_FED,GENRE_TXT',outSR=2056,f='geojson',orderByFields='OBJECTID',resultOffset=offset,resultRecordCount=1000),timeout=45);r.raise_for_status();data=r.json();features+=data['features'];receipts.append({'url':r.url,'sha256':hashlib.sha256(r.content).hexdigest(),'rows':len(data['features'])})
assert len(features)==count and len({f['properties']['OBJECTID'] for f in features})==count
(root/'official-footprints.geojson').write_text(json.dumps({'type':'FeatureCollection','features':features}))
geoms=[shape(f['geometry']) for f in features];assert all(g.is_valid and not g.is_empty for g in geoms)
idx=np.arange(len(P))[::5];dist,_=cKDTree(Q).query(XY[idx]);meta=json.loads((root/'source-components.json').read_text());out=[]
for i,d in zip(idx,dist):
 if d<5:continue
 point=Point(XY[i]);ds=[g.distance(point) for g in geoms];j=int(np.argmin(ds));source=np.array(meta[i]['source_geometry']);source[:,1]*=-1;poly=Polygon(trans(source));overlaps=[poly.intersection(g).area for g in geoms];k=int(np.argmax(overlaps));union=poly.union(geoms[k]).area
 out.append({'source_index':int(i),'rcb_distance_m':float(d),'nearest_footprint_distance_m':ds[j],'nearest_footprint_properties':features[j]['properties'],'best_overlap_footprint_properties':features[k]['properties'] if overlaps[k]>0 else None,'source_area_m2':poly.area,'overlap_area_m2':overlaps[k],'intersection_over_union':overlaps[k]/union,'lv95':XY[i].tolist()})
receipt={'count_url':count_url,'bounds_lv95':bounds,'count':count,'pages':receipts,'outliers':out,'limit':'Current official footprints help explain centroid offsets; not surveyed control or proof of source currentness. Shared cadastral provenance may remain.'};(root/'footprint-review.json').write_text(json.dumps(receipt,indent=2));print(json.dumps({'count':count,'outliers':len(out),'inside_footprints':sum(x['nearest_footprint_distance_m']==0 for x in out),'outlier_iou':[round(x['intersection_over_union'],3) for x in out]}))
