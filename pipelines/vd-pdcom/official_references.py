"""Scoped official cadastral snapshots inside the PDCom parser, not the federal mirror."""
import hashlib,json,re,uuid
from pathlib import Path
import requests
from psycopg2.extras import Json
URL='https://ags.map.vd.ch/ags/rest/services/API/APIGeo/MapServer/21/query'
def validate(response,count,bfs):
    if 'error' in response or response.get('exceededTransferLimit') or count is None or count<=0 or count!=len(response.get('features',[])):raise ValueError('incomplete_official_parcel_snapshot')
    seen=set()
    for f in response['features']:
        p=f['properties'];e=p.get('EGRID')
        if not isinstance(e,str) or not re.fullmatch(r'CH\d{12}',e) or e in seen:raise ValueError('invalid_or_duplicate_reference_egrid')
        if p.get('NO_COM_FED')!=bfs:raise ValueError('reference_commune_mismatch')
        if not f.get('geometry') or f['geometry']['type'] not in ('Polygon','MultiPolygon'):raise ValueError('reference_polygon_required')
        seen.add(e)
    return response['features']

def refresh(conn,document_id,bfs,bounds):
    if len(bounds)!=4 or not (0<bounds[2]-bounds[0]<=2000 and 0<bounds[3]-bounds[1]<=2000):raise ValueError('reference_extent_too_large')
    with conn,conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_document_communes dc JOIN bronze_ch.vd_pdcom_communes c USING(commune_bfs)
                     WHERE dc.document_id=%s AND dc.commune_bfs=%s AND c.is_current''',(document_id,bfs))
        if c.fetchone()[0]!=1:raise ValueError('reference_scope_not_current')
    params={'geometry':','.join(map(str,bounds)),'geometryType':'esriGeometryEnvelope','inSR':2056,'spatialRel':'esriSpatialRelIntersects','where':f'NO_COM_FED={int(bfs)}'}
    cr=requests.get(URL,params={**params,'returnCountOnly':'true','f':'json'},timeout=(15,60));cr.raise_for_status();count=cr.json().get('count')
    fr=requests.get(URL,params={**params,'outFields':'EGRID,NUMERO,NO_COM_FED,GENRE_TXT,SUPERFICIE_MO,SUPERFICIE_RF','outSR':2056,'f':'geojson'},timeout=(15,60));fr.raise_for_status()
    features=validate(fr.json(),count,bfs);sha=hashlib.sha256(fr.content).hexdigest();sid=str(uuid.uuid5(uuid.NAMESPACE_URL,str(document_id)+'#'+fr.url+'#'+sha))
    receipt={'snapshot_id':sid,'document_id':str(document_id),'commune_bfs':bfs,'query_url':fr.url,'response_sha256':sha,'count':count}
    output=Path('vd-pdcom-output');output.mkdir(exist_ok=True);(output/('reference-'+sid+'.json')).write_text(json.dumps(receipt,indent=2))
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='60s'")
        c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_reference_snapshots(id,document_id,commune_bfs,query_url,count_url,response_sha256,feature_count,bounds)
        VALUES(%s,%s,%s,%s,%s,%s,%s,ST_MakeEnvelope(%s,%s,%s,%s,2056)) ON CONFLICT(id) DO UPDATE SET last_checked_at=now()''',(sid,document_id,bfs,fr.url,cr.url,sha,count,*bounds))
        for f in features:
            p=f['properties'];c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_references(snapshot_id,egrid,commune_bfs,geom,official_attributes)
            VALUES(%s,%s,%s,ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056)),%s) ON CONFLICT DO NOTHING''',(sid,p['EGRID'],bfs,json.dumps(f['geometry']),Json(p)))
        c.execute('''SELECT count(*),bool_and(ST_Intersects(r.geom,s.bounds)) FROM bronze_ch.vd_pdcom_parcel_references r JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=r.snapshot_id WHERE r.snapshot_id=%s''',(sid,))
        actual,within=c.fetchone()
        if actual!=count or not within:raise ValueError('persisted_reference_count_or_extent_mismatch')
    return {'snapshot_id':sid,'count':count,'query_url':fr.url,'response_sha256':sha}
