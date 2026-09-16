"""Vaud PDCom coverage census and reproducible pilot PDF inspection.

Acquisition success is NOT geographic coverage completion. No PDF-coordinate
shape is promoted as a geographic sector without independent validation.
"""
import argparse
import collections
import csv
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import uuid

import pymupdf as fitz
import psycopg2
from psycopg2.extras import Json
import requests
from candidates import extract as extract_candidates
from spatial_pilot import persist as persist_spatial_pilot

ROOT = Path(__file__).resolve().parent
CODE = 'vd_pdcom_coverage'
WORKFLOW = 'vd_pdcom.yml'


def roster_url(date):
    return 'https://www.agvchapp.bfs.admin.ch/api/communes/snapshot?date='+date.strftime('%d-%m-%Y')


def parse_roster(text):
    rows = list(csv.DictReader(io.StringIO(text.lstrip('\ufeff'))))
    by_id = {r['HistoricalCode']: r for r in rows}
    result = []
    for r in rows:
        if r['Level'] != '3':
            continue
        parent = by_id[r['Parent']]
        district = parent['Name'] if parent['Level'] == '2' else ''
        seen = set()
        while parent['Level'] != '1':
            if parent['HistoricalCode'] in seen:
                raise ValueError('cyclic_roster_parent')
            seen.add(parent['HistoricalCode'])
            parent = by_id[parent['Parent']]
        if parent['ShortName'] == 'VD':
            result.append(dict(commune_bfs=int(r['BfsCode']), commune_name=r['Name'],
                               historical_code=int(r['HistoricalCode']), district=district))
    codes = [r['commune_bfs'] for r in result]
    if len(codes) != len(set(codes)) or not all(5400 <= c <= 5999 for c in codes):
        raise ValueError('invalid_or_duplicate_federal_codes')
    if not result:
        raise ValueError('empty_VD_roster')
    return sorted(result, key=lambda r: r['commune_bfs'])


def inspect_pdf(data):
    if not data.startswith(b'%PDF'):
        raise ValueError('response_not_pdf')
    result = []
    with fitz.open(stream=data, filetype='pdf') as doc:
        if len(doc)>500:
            raise ValueError('pdf_page_limit')
        for number,page in enumerate(doc,1):
            drawings=page.get_drawings()
            colors=collections.Counter(tuple(round(x,3) for x in d['fill'])
                                       for d in drawings if d.get('fill'))
            result.append(dict(page_number=number,width=page.rect.width,height=page.rect.height,
                               vector_paths=len(drawings), text=page.get_text()[:100000],
                               fills=[{'rgb':list(k),'paths':v} for k,v in colors.most_common(100)],
                               embedded_georef=bool(doc.xref_get_key(page.xref,'VP')[0]!='null')))
    return result


def fetch(url,max_bytes=100_000_000):
    # Sources are reviewed, checked-in official HTTPS URLs, not arbitrary inputs.
    from urllib.parse import urlparse
    allowed={'www.agvchapp.bfs.admin.ch','www.lausanne.ch','www.morges.ch','prangins.ch'}
    for _ in range(6):
        u=urlparse(url)
        if u.scheme!='https' or u.hostname not in allowed or u.username or u.password:
            raise ValueError('unapproved_source_host')
        with requests.get(url,timeout=(15,90),stream=True,allow_redirects=False) as r:
            if r.is_redirect:
                from urllib.parse import urljoin
                url=urljoin(url,r.headers['Location']);continue
            r.raise_for_status()
            data=bytearray()
            for chunk in r.iter_content(1024*128):
                data.extend(chunk)
                if len(data)>max_bytes:raise ValueError('source_size_limit')
            return bytes(data)
    raise ValueError('redirect_limit')


class Monitor:
    def __init__(self,code=CODE):
        self.code=code
    def call(self,method,table,**kwargs):
        r=requests.request(method,os.environ['CMD_URL'].rstrip('/')+'/rest/v1/'+table,
            headers={'apikey':os.environ['CMD_KEY'],'Authorization':'Bearer '+os.environ['CMD_KEY'],
                     'Prefer':'return=representation'},timeout=45,**kwargs)
        if not r.ok:raise RuntimeError('pixxels_http_'+str(r.status_code))
        return r.json() if r.content else None
    def begin(self,run_id):
        rows=self.call('GET','datasets',params={'code':'eq.'+self.code,'select':'id,startup_id'})
        if len(rows)!=1:raise ValueError('registration_missing_or_ambiguous')
        self.dataset=rows[0]
        logs=self.call('GET','acquisition_logs',params={'dataset_id':'eq.'+self.dataset['id'],
            'status':'eq.running','triggered_by':'eq.dashboard',
            'started_at':'gte.'+(dt.datetime.now(dt.timezone.utc)-dt.timedelta(minutes=30)).isoformat(),
            'notes':'eq.[github_action] dispatched '+WORKFLOW,'order':'started_at.desc','limit':'1'})
        self.log_id=logs[0]['id'] if logs else str(run_id)
        note='VD PDCom '+self.code+'; '+os.environ.get('GITHUB_RUN_ID','local')
        if logs:self.call('PATCH','acquisition_logs',params={'id':'eq.'+self.log_id},json={'notes':note})
        else:self.call('POST','acquisition_logs',json={'id':self.log_id,'dataset_id':self.dataset['id'],
            'status':'running','triggered_by':'github_actions','notes':note})
    def finish(self,report,success):
        now=dt.datetime.now(dt.timezone.utc).isoformat()
        count=report.get('record_count',report.get('current_communes',0))
        self.call('PATCH','acquisition_logs',params={'id':'eq.'+self.log_id},json={
            'status':'success' if success else 'failed','completed_at':now,
            'records_fetched':count,'error_details':report,
            'error_message':None if success else 'VD PDCom acquisition failed; inspect report'})
        patch={'status':'active' if success else 'error','record_count':count,
               'last_error':None if success else 'VD PDCom acquisition failed; inspect report'}
        if success:patch.update(last_acquired_at=now,last_db_update_at=now)
        self.call('PATCH','datasets',params={'id':'eq.'+self.dataset['id'],
            'startup_id':'eq.'+self.dataset['startup_id']},json=patch)


def persist_pilot_source(c, source, doc_id, sha, inspection, candidates):
    """Refresh one version without replacing accumulated review evidence."""
    c.execute('''INSERT INTO bronze_ch.vd_pdcom_documents
    (id,source_url,landing_url,sha256,title,plan_status,scope,status_evidence_url,page_count,inspection)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(source_url,sha256) DO NOTHING''',
    (doc_id,source['pdf_url'],source['landing_url'],sha,source['title'],source['plan_status'],source['scope'],source['status_evidence_url'],len(inspection),Json(inspection)))
    c.execute('''INSERT INTO bronze_ch.vd_pdcom_document_communes(document_id,commune_bfs,evidence_url)
    VALUES(%s,%s,%s) ON CONFLICT DO NOTHING''',(doc_id,source['commune_bfs'],source['landing_url']))
    c.execute('''UPDATE bronze_ch.vd_pdcom_communes SET discovery_status='sources_found',
    extraction_status=CASE WHEN COALESCE(evidence,'[]'::jsonb) @> %s THEN extraction_status ELSE %s END,
    delivery_status=CASE WHEN COALESCE(evidence,'[]'::jsonb) @> %s THEN delivery_status ELSE 'not_ready' END,
    last_checked_at=now(),
    evidence=CASE WHEN COALESCE(evidence,'[]'::jsonb) @> %s THEN evidence ELSE COALESCE(evidence,'[]'::jsonb) || %s END,
    blocker=CASE WHEN COALESCE(evidence,'[]'::jsonb) @> %s THEN blocker ELSE %s END WHERE commune_bfs=%s''',
    (Json([{'document_id':doc_id}]),'candidate_vectors' if candidates['paths'] else 'downloaded',Json([{'document_id':doc_id}]),
     Json([{'document_id':doc_id}]),Json([{'url':source['landing_url'],'document_id':doc_id}]),Json([{'document_id':doc_id}]),
     'Independent alignment and spatial QA pending; selected categories only' if candidates['paths'] else 'Source changed: template review required',
     source['commune_bfs']))


def run(args):
    date=dt.date.fromisoformat(args.date) if args.date else dt.datetime.now(dt.timezone.utc).date()
    run_id=uuid.uuid4();url=roster_url(date)
    report={'run_id':str(run_id),'roster_date':str(date),'roster_url':url,'coverage_complete':False,'errors':[]}
    monitor=Monitor() if args.monitor else None
    conn=None
    try:
        if monitor:monitor.begin(run_id)
        data=fetch(url);roster=parse_roster(data.decode('utf-8-sig'))
        report.update(roster_sha256=hashlib.sha256(data).hexdigest(),current_communes=len(roster))
        args.output.mkdir(parents=True,exist_ok=True)
        (args.output/'roster.csv').write_bytes(data)
        if args.persist:
            conn=psycopg2.connect(os.environ['RE_LLM_DB_URL'],connect_timeout=15)
            with conn,conn.cursor() as c:
                c.execute('SELECT count(*) FROM bronze_ch.vd_pdcom_communes WHERE is_current')
                old_count=c.fetchone()[0]
                if old_count and abs(len(roster)-old_count)>max(5,old_count*.05):
                    raise ValueError('large_roster_change_requires_review')
                c.execute("INSERT INTO bronze_ch.vd_pdcom_runs(id,status,roster_date,roster_url,roster_sha256) VALUES(%s,'running',%s,%s,%s)",
                          (str(run_id),date,url,report['roster_sha256']))
                for row in roster:
                    c.execute('''INSERT INTO bronze_ch.vd_pdcom_communes
                    (commune_bfs,commune_name,historical_code,district,roster_date,roster_run_id)
                    VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(commune_bfs) DO UPDATE SET
                    commune_name=excluded.commune_name,historical_code=excluded.historical_code,
                    district=excluded.district,roster_date=excluded.roster_date,roster_run_id=excluded.roster_run_id,
                    last_seen_at=now(),is_current=true,retired_at=null''',
                    (row['commune_bfs'],row['commune_name'],row['historical_code'],row['district'],date,str(run_id)))
                c.execute('''UPDATE bronze_ch.vd_pdcom_communes SET is_current=false,retired_at=now()
                  WHERE is_current AND NOT(commune_bfs=ANY(%s))''',([r['commune_bfs'] for r in roster],))
        current={r['commune_bfs'] for r in roster};documents=[]
        for source in json.loads((ROOT/'pilot_sources.json').read_text()):
            try:
                if source['commune_bfs'] not in current:raise ValueError('source_commune_not_in_current_roster')
                pdf=fetch(source['pdf_url']);sha=hashlib.sha256(pdf).hexdigest()
                doc_id=str(uuid.uuid5(uuid.NAMESPACE_URL,source['pdf_url']+'#'+sha))
                inspection=inspect_pdf(pdf)
                candidates=extract_candidates(pdf,source['commune_bfs'])
                inspection[0]['candidate_extraction']=candidates
                path=args.output/(str(source['commune_bfs'])+'-'+sha[:12]+'.pdf');path.write_bytes(pdf)
                item={**source,'id':doc_id,'sha256':sha,'pages':len(inspection),
                      'vector_paths':sum(p['vector_paths'] for p in inspection),
                      'candidate_paths':len(candidates['paths']),'status':candidates['status']}
                documents.append(item)
                (args.output/(str(source['commune_bfs'])+'-inspection.json')).write_text(json.dumps(inspection,ensure_ascii=False,indent=2))
                if conn:
                    with conn,conn.cursor() as c:
                        persist_pilot_source(c,source,doc_id,sha,inspection,candidates)
                    if source['commune_bfs']==5725:
                        report['spatial_pilot']=persist_spatial_pilot(conn,doc_id,sha)
            except Exception as e:
                report['errors'].append({'commune_bfs':source['commune_bfs'],'error_type':type(e).__name__})
                if conn:
                    conn.rollback()
                    with conn,conn.cursor() as c:
                        c.execute("UPDATE bronze_ch.vd_pdcom_communes SET blocker=%s,last_checked_at=now() WHERE commune_bfs=%s",(type(e).__name__,source['commune_bfs']))
        report['documents']=documents
        if conn:
            with conn,conn.cursor() as c:
                c.execute('''SELECT discovery_status,extraction_status,delivery_status,count(*) FROM bronze_ch.vd_pdcom_communes
                             WHERE is_current GROUP BY 1,2,3 ORDER BY 1,2,3''')
                report['coverage_counts']=[dict(zip(['discovery','extraction','delivery','communes'],r)) for r in c.fetchall()]
                c.execute('''SELECT commune_bfs,commune_name,discovery_status,extraction_status,delivery_status,blocker
                             FROM bronze_ch.vd_pdcom_communes WHERE is_current ORDER BY commune_bfs''')
                with (args.output/'commune-coverage.csv').open('w') as f:
                    w=csv.writer(f);w.writerow([d.name for d in c.description]);w.writerows(c.fetchall())
                c.execute("UPDATE bronze_ch.vd_pdcom_runs SET status=%s,completed_at=now(),report=%s WHERE id=%s",
                          ('failed' if report['errors'] else 'success',Json(report),str(run_id)))
        if monitor:monitor.finish(report,not report['errors'])
    except Exception as e:
        report['errors'].append({'error_type':type(e).__name__})
        if conn:
            conn.rollback()
            with conn,conn.cursor() as c:
                c.execute("UPDATE bronze_ch.vd_pdcom_runs SET status='failed',completed_at=now(),report=%s WHERE id=%s",(Json(report),str(run_id)))
        if monitor and hasattr(monitor,'log_id'):monitor.finish(report,False)
        raise
    finally:
        args.output.mkdir(parents=True,exist_ok=True)
        (args.output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
        if conn:conn.close()
    if report['errors']:raise RuntimeError('source_acquisition_errors')
    print(json.dumps(report,ensure_ascii=False))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--persist',action='store_true');p.add_argument('--monitor',action='store_true')
    p.add_argument('--date');p.add_argument('--output',type=Path,default=Path('vd-pdcom-output'))
    run(p.parse_args())
