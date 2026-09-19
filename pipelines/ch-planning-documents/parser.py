#!/usr/bin/env python3
"""Pixxels-controlled, append-only Swiss planning document acquisition."""
import argparse
import concurrent.futures
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import shutil
import ipaddress
import tempfile
import time
import uuid
from urllib.parse import urlparse, urljoin
import requests
import psycopg2
from psycopg2.extras import Json, execute_values
from pypdf import PdfReader
from bs4 import BeautifulSoup
from sources import STAC, ZH, CANTONS, RESTRICTED, parse_xtf_zip, parse_zh_listing, stable_hash

CODE = 'ch_planning_documents'
SOURCE = 'ch_planning_documents'
NS = uuid.uuid5(uuid.NAMESPACE_URL, 'pixxels:ch-planning-documents:v1')

def uid(value):
    return str(uuid.uuid5(NS,value))

def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()

def safe_url(url):
    p=urlparse(url)
    if p.scheme not in ('https','http') or not p.hostname or p.username or p.password:
        raise ValueError('unsafe_source_url')
    if p.port not in (None,80,443):
        raise ValueError('unsafe_source_port')
    for item in socket.getaddrinfo(p.hostname, p.port or (443 if p.scheme=='https' else 80),type=socket.SOCK_STREAM):
        if not ipaddress.ip_address(item[4][0]).is_global:
            raise ValueError('nonpublic_source_address')
    return url

def fetch(url, maximum=100_000_000):
    """No credentials sent to sources; bounded bytes and redirect destinations."""
    for hop in range(6):
        safe_url(url)
        with requests.get(url,headers={'User-Agent':'Pixxels-planning-parser/1.0 (+https://lamap.ch)'},
                          timeout=(15,90),stream=True,allow_redirects=False) as r:
            if r.is_redirect:
                url=urljoin(url,r.headers['Location']);continue
            r.raise_for_status()
            if int(r.headers.get('Content-Length','0'))>maximum:
                raise ValueError('download_size_limit')
            out=io.BytesIO()
            for chunk in r.iter_content(262144):
                out.write(chunk)
                if out.tell()>maximum:raise ValueError('download_size_limit')
            return out.getvalue(),url,r.headers.get('Content-Type','')
    raise ValueError('redirect_limit')

def failure_code(exc):
    # Never expose DB connection details / credentials via exception repr.
    if isinstance(exc,requests.HTTPError):return 'http_'+str(exc.response.status_code)
    if isinstance(exc,requests.Timeout):return 'timeout'
    if isinstance(exc,ValueError):return str(exc)[:120]
    return type(exc).__name__

def discover(cantons,cache):
    data,_,_=fetch(STAC,10_000_000);j=json.loads(data)
    if any(l.get('rel')=='next' for l in j.get('links',[])):
        raise ValueError('stac_pagination_requires_adapter_update')
    features={f['id'].rsplit('-',1)[-1]:f for f in j['features']}
    rows=[];report={}
    for canton in cantons:
        if canton in RESTRICTED:
            report[canton]={'status':'restricted','documents':0};continue
        if canton=='ZH':
            try:
                html,_,_=fetch(ZH+'?themen%5Bid%5D%5B%5D=1',30_000_000)
                found=parse_zh_listing(html.decode('utf-8'));rows.extend(found)
                report[canton]={'status':'catalogued','documents':len(found),'scope':'Nutzungsplanung allgemein'}
            except Exception as e:report[canton]={'status':'error','error':failure_code(e)}
            continue
        if canton not in features:
            report[canton]={'status':'not_published','documents':0};continue
        try:
            asset=features[canton]['assets']['interlis'];url=asset['href']
            path=cache/(canton+'.zip')
            # Cache is an explicit local fixture optimization, not a freshness claim.
            if not path.exists():
                content,_,_=fetch(url,300_000_000);path.write_bytes(content)
            found=parse_xtf_zip(path,canton,url)
            rows.extend(found)
            report[canton]={'status':'catalogued' if found else 'no_document_records',
                            'documents':len(found),'with_url':sum(bool(x['document_url']) for x in found),
                            'source_asset':url}
        except Exception as e:report[canton]={'status':'error','error':failure_code(e)}
        print(json.dumps({'canton':canton,**report[canton]},ensure_ascii=False),flush=True)
    if not rows:raise ValueError('empty_catalog')
    # Exact duplicate objects can appear in multi-basket exports.
    return list({(r['source'],r['canton_code'],r['source_key']):r for r in rows}.values()),report

def clean_text(text):
    return text.replace('\x00','').encode('utf-8',errors='replace').decode('utf-8')

DOCUMENT_TIME_LIMIT_S=30*60   # one oversized scan must not consume a whole bounded run
RETRY_AFTER='7 days'          # error / needs_ocr references are retried at most weekly
CRASH_LIMIT=2                 # claims that never completed twice (runner killed) -> quarantined for good

class Budget:
    """Wall-clock budget for one run (monotonic). 0 minutes = unbounded (development runs).

    New documents stop being started at `minutes`; documents already in flight must finish
    before `minutes + grace` or are abandoned untouched, so the run always reaches its own
    reporting before the GitHub job limit instead of being killed mid-OCR."""
    def __init__(self,minutes=0,grace_minutes=20,clock=time.monotonic):
        self.clock=clock;self.start=clock();self.minutes=minutes
        self.stop_new_at=self.start+minutes*60 if minutes else None
        self.hard_at=self.start+(minutes+grace_minutes)*60 if minutes else None
    def accepting(self):
        return self.stop_new_at is None or self.clock()<self.stop_new_at
    def document_deadline(self):
        own=self.clock()+DOCUMENT_TIME_LIMIT_S
        return own if self.hard_at is None else min(own,self.hard_at)
    def hard_expired(self):
        return self.hard_at is not None and self.clock()>=self.hard_at

def extract(url,deadline=None):
    data,final,ctype=fetch(url)
    digest=hashlib.sha256(data).hexdigest()
    def check_deadline():
        if deadline is not None and time.monotonic()>deadline:raise ValueError('document_time_limit')
    check_deadline()
    pages=[]
    if data.startswith(b'%PDF-'):
        reader=PdfReader(io.BytesIO(data))
        if reader.is_encrypted and not reader.decrypt(''):raise ValueError('encrypted_pdf')
        if len(reader.pages)>2000:raise ValueError('pdf_page_limit')
        unresolved=False
        with tempfile.TemporaryDirectory() as temp:
            pdf_path=Path(temp)/'source.pdf';pdf_path.write_bytes(data)
            for index,page in enumerate(reader.pages,1):
                check_deadline()
                try:
                    text=(page.extract_text(extraction_mode='layout',layout_mode_strip_rotated=False) or '').strip() if '/Contents' in page else ''
                except Exception:
                    try:text=(page.extract_text() or '').strip()
                    except Exception:text=''
                text=clean_text(text)
                method='pypdf-layout-v1'
                if len(text)<40 and '/Contents' in page:
                    if os.getenv('PLANNING_OCR')=='1' and shutil.which('tesseract') and shutil.which('pdftoppm'):
                        prefix=str(Path(temp)/'page')
                        try:
                            subprocess.run(['pdftoppm','-f',str(index),'-l',str(index),'-singlefile','-scale-to','2400','-png',str(pdf_path),prefix],check=True,timeout=90,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                            ocr=subprocess.run(['tesseract',prefix+'.png','stdout','-l',os.getenv('OCR_LANGUAGES','deu+fra+ita+eng')],check=True,timeout=120,capture_output=True)
                            text=clean_text(ocr.stdout.decode('utf-8').strip());method='tesseract-v1'
                        except subprocess.TimeoutExpired:
                            # 2026-09-14: one slow page (large-format plan) used to abort the whole document
                            # as error=TimeoutExpired (231 of 243 failing URLs), discarding every page
                            # already read. The page stays unresolved instead -> the document is kept as
                            # needs_ocr (never published as text, retried weekly), like any unreadable scan.
                            method='tesseract-timeout-v1'
                    if len(text)<40:unresolved=True
                pages.append({'page_number':index,'text':text,'method':method})
        status='extracted' if sum(len(p['text']) for p in pages)>=300 and not unresolved else 'needs_ocr'
        ctype='application/pdf'
    elif 'html' in ctype:
        soup=BeautifulSoup(data,'html.parser')
        for node in soup.select('script,style,nav,header,footer'):node.decompose()
        root=soup.find('main') or soup.find('article')
        # Do not mistake a generic portal / login page for legal document text.
        if root is None:raise ValueError('html_requires_source_adapter')
        text=root.get_text('\n',strip=True)
        if len(text)<300:raise ValueError('html_requires_source_adapter')
        pages=[{'page_number':None,'text':text}];status='html_unverified'
    else:raise ValueError('unsupported_content_type')
    return dict(content_hash=digest,final_url=final,content_type=ctype,byte_count=len(data),pages=pages,extraction_status=status)

def db_connect():
    uri=os.environ['RE_LLM_DB_URL']
    conn=psycopg2.connect(uri,connect_timeout=20)
    with conn.cursor() as c:c.execute("set statement_timeout='120s'; set lock_timeout='10s'")
    conn.commit();return conn

def store_catalog(conn,run_id,rows):
    with conn:
        with conn.cursor() as c:
            values=[]
            for row in rows:
                rid=uid(row['source']+':'+row['canton_code']+':'+row['source_key'])
                values.append((rid,*[row[k] for k in ('source','canton_code','source_key','title','document_url','commune_bfs','language','legal_status','document_type')],Json(row['source_metadata']),stable_hash(row),run_id,'pending' if row['document_url'] else 'missing_url'))
            execute_values(c,"""INSERT INTO bronze_ch.planning_document_sources
                (id,source,canton_code,source_key,title,document_url,commune_bfs,language,legal_status,document_type,source_metadata,catalog_hash,last_seen_run,extraction_status)
                VALUES %s ON CONFLICT(source,canton_code,source_key) DO UPDATE SET
                title=excluded.title,document_url=excluded.document_url,commune_bfs=excluded.commune_bfs,
                language=excluded.language,legal_status=excluded.legal_status,document_type=excluded.document_type,
                source_metadata=excluded.source_metadata,catalog_hash=excluded.catalog_hash,
                last_seen_run=excluded.last_seen_run,last_seen_at=now()""",values,page_size=500)

def bounded_map(pool, fn, items, concurrency=3, accepting=lambda:True, submitted=None, on_submit=None):
    """Yield fn(item) results with at most `concurrency` in flight. Stops submitting new items
    once accepting() is False; `submitted` (a list) receives the count of items started.
    `on_submit(item)` runs in the caller's thread just before an item starts (crash claim)."""
    iterator=iter(items)
    pending=set();started=0
    def submit_next():
        nonlocal started
        if not accepting():return
        item=next(iterator,None)
        if item is not None:
            if on_submit is not None:on_submit(item)
            pending.add(pool.submit(fn,item));started+=1
    for _ in range(concurrency):submit_next()
    while pending:
        done,pending=concurrent.futures.wait(pending,return_when=concurrent.futures.FIRST_COMPLETED)
        for future in done:
            yield future.result()
            submit_next()
    if submitted is not None:submitted.append(started)

def chunk_pages(pages):
    result=[]
    for p in pages:
        text=p['text']
        for start in range(0,len(text),2000):
            content=text[start:start+2000].strip()
            if content:result.append((p['page_number'],content))
    return result

def store_extraction(conn,row,payload):
    source_id,title,url,canton,bfs,language,legal,meta=row
    version_id=uid('version:'+url+':'+payload['content_hash'])
    doc_id=uid('knowledge:'+url+':'+payload['content_hash'])
    chunks=chunk_pages(payload['pages'])
    # Text PDFs only become searchable. Maps/scans/HTML remain explicit bronze gaps.
    publish=payload['extraction_status']=='extracted'
    with conn:
        with conn.cursor() as c:
            c.execute("select id::text from bronze_ch.planning_document_versions where id=%s or (source_id=%s and content_hash=%s) order by id limit 1",(version_id,source_id,payload["content_hash"]))
            existing=c.fetchone()
            exists=existing is not None
            if existing:version_id=existing[0]
            if not exists:
                c.execute("""insert into bronze_ch.planning_document_versions
                (id,source_id,content_hash,final_url,content_type,byte_count,pages,extraction_status,knowledge_document_id)
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(version_id,source_id,payload['content_hash'],payload['final_url'],payload['content_type'],payload['byte_count'],Json(payload['pages']),payload['extraction_status'],doc_id if publish else None))
            if exists and publish:
                c.execute("update bronze_ch.planning_document_versions set pages=%s,extraction_status='extracted',knowledge_document_id=%s where id=%s and extraction_status<>'extracted'",(Json(payload['pages']),doc_id,version_id))
            c.execute('select 1 from knowledge_ch.documents where id=%s',(doc_id,))
            knowledge_exists=c.fetchone() is not None
            if publish and not knowledge_exists:
                # Mandatory bulk discipline: short transaction, restore before commit;
                # rollback also restores original trigger state on failure.
                c.execute('ALTER TABLE knowledge_ch.documents DISABLE TRIGGER classify_on_insert')
                c.execute('ALTER TABLE knowledge_ch.chunks DISABLE TRIGGER classify_on_insert')
                c.execute('select canton_code,commune_bfs,legal_status,id::text from bronze_ch.planning_document_sources where document_url=%s',(url,))
                scopes=[{'canton':ct,'commune_bfs':b,'legal_status':l,'source_id':sid} for ct,b,l,sid in c.fetchall()]
                scoped_cantons={r['canton'] for r in scopes}
                canton=next(iter(scoped_cantons)) if len(scoped_cantons)==1 else None
                metadata={'parser':SOURCE,'reference_scopes':scopes,'version_id':version_id,
                          'content_hash':payload['content_hash'],
                          'final_url':payload['final_url'],'fetched_at':now(), 'extraction_methods':sorted({p.get('method','unknown') for p in payload['pages']})}
                c.execute("""INSERT INTO knowledge_ch.documents
                    (id,title,source,original_url,document_type,publisher,language,country,canton_code,
                     ingestion_status,chunk_count,is_active,raw_metadata,categorization_status,domain,accessible_to_products)
                    VALUES(%s,%s,%s,%s,'planning_document',%s,%s,'CH',%s,'completed',%s,true,%s,'pending','real_estate','{lamap,lbi}')""",
                    (doc_id,title,SOURCE,url,'Official Swiss planning registers',language,canton,len(chunks),Json(metadata)))
                execute_values(c,"""INSERT INTO knowledge_ch.chunks
                    (id,document_id,chunk_index,content,page_number,metadata,categorization_status,domain,accessible_to_products)
                    VALUES %s""",[(uid(doc_id+':'+str(i)),doc_id,i,text,page,Json({'source_url':url,'content_hash':payload['content_hash']}),'pending','real_estate',['lamap','lbi']) for i,(page,text) in enumerate(chunks)],page_size=200)
                c.execute("""UPDATE knowledge_ch.documents SET is_active=false,updated_at=now()
                    WHERE source=%s AND original_url=%s AND id<>%s AND is_active""",(SOURCE,url,doc_id))
                c.execute('ALTER TABLE knowledge_ch.chunks ENABLE TRIGGER classify_on_insert')
                c.execute('ALTER TABLE knowledge_ch.documents ENABLE TRIGGER classify_on_insert')
            c.execute("""UPDATE bronze_ch.planning_document_sources SET current_version_id=%s,
                last_attempt_at=now(),last_success_at=now(),extraction_status=%s,error_code=null,crash_count=0 WHERE id=%s""",
                (version_id,payload['extraction_status'],source_id))
    return not exists,publish

def due_targets(conn,catalog_run,cantons):
    """References owed text extraction in one catalog run. Never-attempted first; references that
    errored or still need OCR are retried at most every RETRY_AFTER so a long tail of dead links
    or unreadable scans cannot consume every bounded run."""
    with conn.cursor() as c:
        c.execute(f"""select id::text,title,document_url,canton_code,commune_bfs,language,legal_status,source_metadata
          from bronze_ch.planning_document_sources where last_seen_run=%s and canton_code=any(%s) and document_url is not null
          and (
            (last_success_at is null and (extraction_status<>'error' or last_attempt_at is null or last_attempt_at < now()-interval '{RETRY_AFTER}'))
            or last_success_at < now()-interval '365 days'
            or (extraction_status='needs_ocr' and (last_attempt_at is null or last_attempt_at < now()-interval '{RETRY_AFTER}'))
          )
          -- crash quarantine: a document whose claim never completed killed its runner (e.g. out of
          -- memory). Retried once after RETRY_AFTER, then never again, so it cannot block the queue.
          and crash_count < {CRASH_LIMIT}
          and (crash_count = 0 or claimed_at < now()-interval '{RETRY_AFTER}')
          order by crash_count,(last_attempt_at is not null),
                   case when title ~* '(reglement|règlement|bauordnung|bau.?und.?zonenordnung|RCU)' then 0 else 1 end,canton_code,id""",(catalog_run,cantons))
        rows=c.fetchall()
    conn.commit()
    return rows

class Monitor:
    def __init__(self,code=CODE):
        self.code=code
        self.url=os.environ['CMD_URL'].rstrip('/')
        self.headers={'apikey':os.environ['CMD_KEY'],'Authorization':'Bearer '+os.environ['CMD_KEY'],'Prefer':'return=representation'}
        rows=self.call('GET','datasets',params={'code':'eq.'+self.code,'select':'id,startup_id,last_acquired_at'})
        if len(rows)!=1:raise ValueError('dataset_registration_missing_or_ambiguous')
        self.dataset=rows[0]
        previous=self.call('GET','acquisition_logs',params={'dataset_id':'eq.'+self.dataset['id'],'status':'eq.success','select':'completed_at','order':'completed_at.desc','limit':'1'})
        self.last_success=previous[0]['completed_at'] if previous else None
    def call(self,method,table,**kwargs):
        r=requests.request(method,self.url+'/rest/v1/'+table,headers=self.headers,timeout=45,**kwargs)
        if not r.ok:raise RuntimeError('command_center_http_'+str(r.status_code))
        return r.json() if r.content else None
    def begin(self,run_id,run_url):
        cutoff=(dt.datetime.now(dt.timezone.utc)-dt.timedelta(minutes=30)).isoformat()
        dispatched=self.call('GET','acquisition_logs',params={'dataset_id':'eq.'+self.dataset['id'],
            'status':'eq.running','triggered_by':'eq.dashboard','started_at':'gte.'+cutoff,
            'notes':'eq.[github_action] dispatched ch_planning_documents.yml','order':'started_at.desc','limit':'1','select':'id'})
        self.log_id=dispatched[0]['id'] if dispatched else run_id
        if dispatched:
            rows=self.call('PATCH','acquisition_logs',params={'id':'eq.'+self.log_id,'status':'eq.running'},json={'notes':run_url})
            if len(rows)!=1:raise ValueError('dispatch_log_claim_failed')
        else:
            self.call('POST','acquisition_logs',json={'id':self.log_id,'dataset_id':self.dataset['id'],'status':'running','triggered_by':'github_actions','notes':run_url})
    def finish(self,run_id,report,complete,partial=False):
        """partial=True: a bounded run stopped by its time budget after real progress, with work
        remaining. Logged as 'partial' (never 'success'), no error state, and freshness is not
        advanced — the next scheduled continuation carries on."""
        run_id=getattr(self,'log_id',run_id)
        partial=partial and not complete
        status='success' if complete else ('partial' if partial else 'failed')
        error=None if (complete or partial) else 'Incomplete acquisition: see run report for source failures and coverage gaps'
        self.call('PATCH','acquisition_logs',params={'id':'eq.'+run_id},json={'status':status,'completed_at':now(),'records_fetched':report.get('catalogued',0),'records_new':report.get('versions_new',0),'error_message':error,'error_details':report})
        patch={'status':'active' if (complete or partial) else 'error','last_error':error,'record_count':report.get('catalogued',0) if self.code==CODE else report.get('stored_versions',0)}
        if complete:patch.update(last_acquired_at=now(),last_db_update_at=now())
        if not complete:
            patch['last_acquired_at']=self.last_success
            if self.last_success is None:patch['next_acquisition_at']=None
        # Restore verified completion after the legacy dispatch timestamp side effect.
        rows=self.call('PATCH','datasets',params={'code':'eq.'+self.code,'startup_id':'eq.'+self.dataset['startup_id']},json=patch)
        if len(rows)!=1:raise ValueError('dataset_patch_not_verified')

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--cantons',default=','.join(CANTONS));ap.add_argument('--max-documents',type=int,default=0)
    ap.add_argument('--catalog-only',action='store_true');ap.add_argument('--dry-run',action='store_true');ap.add_argument('--cache-dir');ap.add_argument('--report',default='planning-report.json');ap.add_argument('--no-monitor',action='store_true')
    ap.add_argument('--time-budget-minutes',type=int,default=0,help='stop starting documents after N minutes (0 = unbounded)')
    ap.add_argument('--resume',action='store_true',help='extract against the latest stored catalog; no re-discovery, text dataset only')
    args=ap.parse_args();cantons=args.cantons.split(',')
    if not set(cantons)<=set(CANTONS):raise ValueError('unknown_canton')
    if args.max_documents<0:raise ValueError('negative_document_limit')
    if args.time_budget_minutes<0:raise ValueError('negative_time_budget')
    if args.resume and (args.catalog_only or args.dry_run or args.cache_dir):raise ValueError('resume_is_extraction_only')
    budget=Budget(args.time_budget_minutes)
    run_id=str(uuid.uuid4());run_url='https://github.com/'+os.getenv('GITHUB_REPOSITORY','camelote-command-center/data-pipelines')+'/actions/runs/'+os.getenv('GITHUB_RUN_ID','local')
    report={'run_id':run_id,'mode':'resume' if args.resume else 'full','scope':cantons,'started_at':now(),
            'time_budget_minutes':args.time_budget_minutes,'versions_new':0,'extracted':0,'errors':0}
    monitors=[];finished_monitors=set();conn=None;complete=False;partial=False
    try:
        if not args.dry_run:
            conn=db_connect()
            if args.resume:
                # Continuation of the most recently persisted catalog. A scheduled continuation with
                # nothing due is a silent no-op: no run row, no monitor log, exit 0.
                with conn.cursor() as c:
                    c.execute("select last_seen_run::text from bronze_ch.planning_document_sources where last_seen_run is not null order by last_seen_at desc nulls last limit 1")
                    found=c.fetchone()
                conn.commit()
                if not found:raise ValueError('resume_without_catalog')
                catalog_run=found[0]
                if os.getenv('GITHUB_EVENT_NAME')=='schedule' and not due_targets(conn,catalog_run,cantons):
                    report.update(catalog_run_id=catalog_run,due_documents=0,noop=True,complete=False)
                    print(json.dumps(report,ensure_ascii=False),flush=True)
                    return 0
            else:
                catalog_run=run_id
            monitors=[] if args.no_monitor else ([Monitor('ch_planning_document_text')] if args.resume else [Monitor(CODE),Monitor('ch_planning_document_text')])
            for mon in monitors:mon.begin(uid(run_id+':'+mon.code),run_url)
            with conn:
                with conn.cursor() as c:c.execute('insert into bronze_ch.planning_document_runs(id,scope,workflow_url) values(%s,%s,%s)',(run_id,Json(vars(args)),run_url))
        if args.resume:
            report['catalog_run_id']=catalog_run
            with conn.cursor() as c:
                c.execute("select count(*),count(*) filter (where document_url is not null) from bronze_ch.planning_document_sources where last_seen_run=%s",(catalog_run,))
                report['catalogued'],report['with_url']=c.fetchone()
            conn.commit();coverage={}
        else:
            with tempfile.TemporaryDirectory() as temp:
                cache=Path(args.cache_dir or temp);cache.mkdir(parents=True,exist_ok=True)
                rows,coverage=discover(cantons,cache)
            report.update(catalogued=len(rows),coverage=coverage,with_url=sum(bool(r['document_url']) for r in rows),explicit_communes=len({(r['canton_code'],r['commune_bfs']) for r in rows if r['commune_bfs']}))
            if args.dry_run:return 0
            store_catalog(conn,run_id,rows)
            report['catalog_persisted']=True
            catalog_ok=(set(cantons)==set(CANTONS) and not any(v['status']=='error' for v in coverage.values()))
            Path(args.report).write_text(json.dumps(report,ensure_ascii=False,indent=2))
            for mon in monitors:
                if mon.code==CODE:
                    mon.finish(uid(run_id+':'+mon.code),report,catalog_ok)
                    finished_monitors.add(mon.code)
        if not args.catalog_only:
            targets=due_targets(conn,catalog_run,cantons)
            groups={}
            for row in targets:groups.setdefault(row[2],[]).append(row)
            report['due_references']=len(targets)
            targets=list(groups.values())
            report['due_documents']=len(targets)
            selected=targets[:args.max_documents] if args.max_documents else targets
            report['deferred_documents']=len(targets)-len(selected)
            report['abandoned_at_budget']=0
            # Download/extract concurrently; serialize short database transactions.
            def worker(group):
                try:return group,extract(group[0][2],deadline=budget.document_deadline()),None
                except Exception as e:return group,None,failure_code(e)
            submitted=[]
            def claim(group):
                # Committed BEFORE the download starts: if the runner is killed mid-document the
                # counter stays raised and the queue skips this document next run. Every completion
                # path below resets it to 0.
                with conn:
                    with conn.cursor() as c:
                        c.execute("update bronze_ch.planning_document_sources set crash_count=crash_count+1,claimed_at=now() where id=any(%s::uuid[])",([r[0] for r in group],))
            with conn.cursor() as c:
                c.execute("select count(*) from bronze_ch.planning_document_sources where crash_count>=%s",(CRASH_LIMIT,))
                report['quarantined_after_runner_crash']=c.fetchone()[0]
            conn.commit()
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                for i,(group,payload,error) in enumerate(bounded_map(pool,worker,selected,accepting=budget.accepting,submitted=submitted,on_submit=claim),1):
                    if error=='document_time_limit' and budget.hard_expired():
                        # Cut off by the run budget, not by its own size: not an error. Stamp the
                        # attempt so never-attempted documents go first next run.
                        report['abandoned_at_budget']+=1
                        with conn:
                            with conn.cursor() as c:
                                c.execute("update bronze_ch.planning_document_sources set last_attempt_at=now(),crash_count=0 where id=any(%s::uuid[])",([r[0] for r in group],))
                    elif error:
                        report['errors']+=1
                        with conn:
                            with conn.cursor() as c:
                                c.execute("update bronze_ch.planning_document_sources set extraction_status='error',error_code=%s,last_attempt_at=now(),crash_count=0 where id=any(%s::uuid[])",(error,[r[0] for r in group]))
                    else:
                        for row in group:
                            new,publish=store_extraction(conn,row,payload)
                            report['versions_new']+=int(new)
                        report['extracted']+=int(publish)
                        key=payload['extraction_status'];report.setdefault('extraction_outcomes',{});report['extraction_outcomes'][key]=report['extraction_outcomes'].get(key,0)+1
                    if i%10==0:
                        print(json.dumps({'processed':i,'selected':len(selected),'errors':report['errors']}),flush=True)
                        Path(args.report).write_text(json.dumps(report,ensure_ascii=False,indent=2))
                    if i%50==0:
                        for mon in monitors:
                            if mon.code!='ch_planning_document_text':continue
                            mon.call('PATCH','acquisition_logs',params={'id':'eq.'+mon.log_id},json={'records_fetched':i,'notes':run_url+'; '+str(i)+'/'+str(len(selected))+' URLs processed; '+str(report['errors'])+' errors'})
            started=submitted[0] if submitted else 0
            report['processed']=started
            report['deferred_documents']+=len(selected)-started
            report['stopped_by_budget']=(len(selected)-started>0) or report['abandoned_at_budget']>0
            with conn.cursor() as c:
                # Everything still owed by the catalog, ignoring the weekly retry throttle: completion
                # keeps its original meaning (no reference left unextracted or failing).
                c.execute("""select count(*) from bronze_ch.planning_document_sources where last_seen_run=%s
                  and canton_code=any(%s) and document_url is not null
                  and (last_success_at is null or last_success_at < now()-interval '365 days' or extraction_status in ('needs_ocr','error'))""",(catalog_run,cantons))
                report['outstanding_references']=c.fetchone()[0]
            conn.commit()
        remaining=report.get('deferred_documents',0)+report.get('abandoned_at_budget',0)
        complete=(not args.catalog_only and not args.max_documents and set(cantons)==set(CANTONS)
                  and not any(v['status']=='error' for v in coverage.values()) and report['errors']==0
                  and remaining==0 and report.get('outstanding_references',0)==0)
        # A budget-bounded run that made progress and still has work is healthy partial progress; a run
        # that could not start a single document is a real failure and stays red.
        partial=(not complete and not args.catalog_only and not args.max_documents
                 and report.get('stopped_by_budget',False) and report.get('processed',0)>0)
        return 0 if (complete or partial) else 2
    except Exception as e:
        report['fatal_error']=failure_code(e);raise
    finally:
        report['complete']=complete;report['partial']=partial;report['completed_at']=now();Path(args.report).write_text(json.dumps(report,ensure_ascii=False,indent=2))
        if conn:
            conn.rollback()
            with conn:
                with conn.cursor() as c:
                    c.execute('select count(*) from bronze_ch.planning_document_versions');report['stored_versions']=c.fetchone()[0]
                    c.execute('update bronze_ch.planning_document_runs set completed_at=now(),status=%s,report=%s where id=%s',('success' if complete else ('partial' if partial else 'incomplete'),Json(report),run_id))
            conn.close()
        for mon in monitors:
            if mon.code in finished_monitors:continue
            catalog_ok=(report.get('catalog_persisted',False) and set(cantons)==set(CANTONS) and not any(v['status']=='error' for v in report.get('coverage',{}).values()))
            if mon.code==CODE:mon.finish(uid(run_id+':'+mon.code),report,catalog_ok)
            else:mon.finish(uid(run_id+':'+mon.code),report,complete,partial=partial)
        Path(args.report).write_text(json.dumps(report,ensure_ascii=False,indent=2))
        print(json.dumps(report,ensure_ascii=False),flush=True)

if __name__=='__main__':
    try:raise SystemExit(main())
    except Exception as e:
        print('Parser failed: '+failure_code(e),flush=True);raise SystemExit(1)
