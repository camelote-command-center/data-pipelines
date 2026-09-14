"""Enrich PDCom review EGRIDs with official Vaud cadastral object kinds.

A surface right and its underlying land parcel are distinct records. Never
silently deduplicate them or sum their intersections as exclusive land area.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import requests
import psycopg2
from psycopg2.extras import Json

URL='https://ags.map.vd.ch/ags/rest/services/API/APIGeo/MapServer/21/query'
KINDS={'parcelle privée':'bien_fonds','DP cantonal':'bien_fonds','DP communal':'bien_fonds',
       'DDP superficie':'ddp_superficie','DDP source':'ddp_source'}


def classify(response, expected, evidence):
    if 'error' in response or response.get('exceededTransferLimit') or 'features' not in response:
        raise ValueError('incomplete_cadastral_type_response')
    result={}
    for feature in response['features']:
        p=feature['properties'];egrid=p['EGRID']
        if egrid not in expected or egrid in result:raise ValueError('unexpected_or_duplicate_egrid')
        if p['NO_COM_FED']!=expected[egrid]:raise ValueError('cadastral_commune_mismatch')
        kind=KINDS.get(p.get('GENRE_TXT'),'unknown')
        result[egrid]=(kind,{**evidence,'status':'matched' if kind!='unknown' else 'unrecognized_kind',
                            'official_attributes':p})
    for egrid in expected.keys()-result.keys():
        result[egrid]=('unknown',{**evidence,'status':'not_found'})
    return result


def refresh(conn, output):
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='30s'")
        c.execute('''SELECT DISTINCT p.egrid,c.commune_bfs FROM bronze_ch.vd_pdcom_parcel_candidates p
                     LEFT JOIN silver_ch.cadastral_plots c ON c.egrid=p.egrid ORDER BY p.egrid''')
        rows=c.fetchall()
    expected={}
    for egrid,bfs in rows:
        if not re.fullmatch(r'CH\d{12}',egrid) or bfs is None:raise ValueError('unresolved_candidate_identity')
        if egrid in expected and expected[egrid]!=bfs:raise ValueError('ambiguous_candidate_commune')
        expected[egrid]=bfs
    updates={};items=list(expected)
    for start in range(0,len(items),100):
        batch=items[start:start+100]
        params={'f':'geojson','where':'EGRID IN ('+','.join("'"+e+"'" for e in batch)+')',
                'outFields':'EGRID,NUMERO,NO_COM_FED,ORIGINE,GENRE_TXT,SUPERFICIE_MO,SUPERFICIE_RF',
                'returnGeometry':'false'}
        r=requests.get(URL,params=params,timeout=(15,45));r.raise_for_status();response=r.json()
        count_response=requests.get(URL,params={**params,'f':'json','returnCountOnly':'true'},timeout=(15,45))
        count_response.raise_for_status();count=count_response.json().get('count')
        if count!=len(response.get('features',[])):raise ValueError('cadastral_type_count_mismatch')
        evidence={'source_url':r.url,'response_sha256':hashlib.sha256(r.content).hexdigest(),
                  'checked_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'count_verified':count}
        updates.update(classify(response,{e:expected[e] for e in batch},evidence))
    # No network calls inside the write transaction. Any failed batch leaves old values intact.
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='30s'")
        for egrid,(kind,evidence) in updates.items():
            c.execute('''UPDATE bronze_ch.vd_pdcom_parcel_candidates SET cadastral_object_kind=%s,
                         cadastral_type_evidence=%s WHERE egrid=%s''',(kind,Json(evidence),egrid))
        c.execute('''SELECT cadastral_object_kind,count(*),count(DISTINCT egrid)
                     FROM bronze_ch.vd_pdcom_parcel_candidates GROUP BY 1 ORDER BY 1''')
        counts=[dict(kind=k,pairs=n,egrids=u) for k,n,u in c.fetchall()]
    result={'checked_egrids':len(updates),'counts':counts,'release_status':'internal_review_only',
            'aggregation_rule':'Keep rights and land EGRIDs distinct; do not sum them as exclusive land area.'}
    output=Path(output);output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(result,indent=2));return result

if __name__=='__main__':
    conn=psycopg2.connect(os.environ['RE_LLM_DB_URL'],connect_timeout=15)
    try:print(json.dumps(refresh(conn,'vd-pdcom-output/cadastral-types.json')))
    finally:conn.close()
