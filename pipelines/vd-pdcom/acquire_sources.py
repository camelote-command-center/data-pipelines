"""Acquire reviewed official PDCom links; preserve unverified version status.

This inventories document pages, not geographic features. No reviewed link or
successful PDF download advances a commune to validated or delivery verified.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urljoin, urlsplit
import uuid

import pymupdf as fitz
import psycopg2
from psycopg2.extras import Json
import requests
from discovery import public_url

ROOT=Path(__file__).resolve().parent


def download(url,max_bytes=100_000_000):
    """Retry bounded transient GET failures, preserving redirect and size guards."""
    for attempt in range(3):
        try:
            return _download_once(url,max_bytes)
        except (requests.Timeout,requests.ConnectionError,requests.HTTPError) as exc:
            response=getattr(exc,'response',None)
            status=response.status_code if response is not None else None
            retryable=isinstance(exc,(requests.Timeout,requests.ConnectionError)) or status in (429,500,502,503,504)
            if not retryable or attempt==2:raise
            time.sleep(2**(attempt+1))


def _download_once(url,max_bytes=100_000_000):
    host=urlsplit(url).hostname
    for _ in range(5):
        public_url(url)
        if urlsplit(url).hostname not in (host,host.removeprefix('www.'),'www.'+host.removeprefix('www.')):
            raise ValueError('unreviewed_redirect_host')
        with requests.get(url,stream=True,timeout=(15,60),allow_redirects=False) as r:
            if r.is_redirect:
                url=urljoin(url,r.headers['Location']);continue
            r.raise_for_status();data=bytearray()
            for chunk in r.iter_content(131072):
                data.extend(chunk)
                if len(data)>max_bytes:raise ValueError('pdf_size_limit')
            if not data.startswith(b'%PDF'):raise ValueError('response_not_pdf')
            return bytes(data)
    raise ValueError('redirect_limit')


def inspect(data):
    pages=[]
    with fitz.open(stream=data,filetype='pdf') as doc:
        if len(doc)>500:raise ValueError('page_limit')
        for number,page in enumerate(doc,1):
            text=page.get_text()
            pages.append({'page_number':number,'width':page.rect.width,'height':page.rect.height,
                          'text':text[:30000],'vector_paths':len(page.get_drawings()),
                          'embedded_georef':doc.xref_get_key(page.xref,'VP')[0]!='null'})
    return pages


def coverage_members(source):
    members=source.get('commune_bfs_list',[source['commune_bfs']])
    if not members or len(members)!=len(set(members)) or source['commune_bfs'] not in members:
        raise ValueError('invalid_commune_coverage')
    if len(members)>1 and source['scope']!='intercommunal':
        raise ValueError('multi_commune_source_requires_intercommunal_scope')
    return members


def persist(conn,source,sha,pages):
    doc_id=str(uuid.uuid5(uuid.NAMESPACE_URL,source['pdf_url']+'#'+sha))
    with conn,conn.cursor() as c:
        members=coverage_members(source)
        c.execute('SELECT count(*) FROM bronze_ch.vd_pdcom_communes WHERE commune_bfs=ANY(%s) AND is_current',(members,))
        if c.fetchone()[0]!=len(members):raise ValueError('source_commune_not_current')
        c.execute('''INSERT INTO bronze_ch.vd_pdcom_documents
        (id,source_url,landing_url,sha256,title,plan_status,scope,status_evidence_url,page_count,inspection)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(source_url,sha256) DO NOTHING''',
        (doc_id,source['pdf_url'],source['landing_url'],sha,source['title'],source['plan_status'],source['scope'],source['landing_url'],len(pages),Json(pages)))
        for bfs in members:
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_document_communes(document_id,commune_bfs,evidence_url)
            VALUES(%s,%s,%s) ON CONFLICT DO NOTHING''',(doc_id,bfs,source['landing_url']))
            c.execute('''UPDATE bronze_ch.vd_pdcom_communes SET discovery_status='sources_found',
            extraction_status=CASE WHEN extraction_status='pending' THEN %s ELSE extraction_status END,
            last_checked_at=now(),evidence=CASE WHEN evidence @> %s THEN evidence ELSE evidence || %s END
            WHERE commune_bfs=%s''',('needs_ocr' if sum(len(p['text'].strip()) for p in pages)<50 else 'downloaded',Json([{'document_id':doc_id}]),Json([{'document_id':doc_id,'url':source['landing_url'],'version_review':'pending'}]),bfs))
    from reviews import apply_review
    apply_review(conn,doc_id,sha)
    from epalinges_pilot import persist as persist_epalinges
    persist_epalinges(conn,doc_id,sha)
    from aigle_pilot import persist as persist_aigle
    persist_aigle(conn,doc_id,sha)
    from bex_pilot import persist as persist_bex
    persist_bex(conn,doc_id,sha)
    return doc_id


def run(args):
    sources=json.loads((ROOT/'reviewed_sources.json').read_text())
    args.output.mkdir(parents=True,exist_ok=True)
    conn=psycopg2.connect(os.environ['RE_LLM_DB_URL'],connect_timeout=15) if args.persist else None
    report={'operation_id':str(uuid.uuid4()),'documents':[],'errors':[],'coverage_complete':False}
    try:
        for source in sources:
            try:
                data=download(source['pdf_url'],source.get('max_bytes',100_000_000));sha=hashlib.sha256(data).hexdigest()
                pages=inspect(data)
                name=f"{source['commune_bfs']}-{sha[:12]}"
                (args.output/(name+'.pdf')).write_bytes(data)
                (args.output/(name+'-inspection.json')).write_text(json.dumps(pages,ensure_ascii=False))
                doc_id=persist(conn,source,sha,pages) if conn else None
                result={'commune_bfs':source['commune_bfs'],'covered_commune_bfs':coverage_members(source),'document_id':doc_id,'sha256':sha,'pages':len(pages),
                        'vector_pages':sum(p['vector_paths']>0 for p in pages),'plan_status':source['plan_status']}
                report['documents'].append(result);print(json.dumps(result),flush=True)
            except Exception as exc:
                if conn:conn.rollback()
                error={'commune_bfs':source['commune_bfs'],'source_url':source['pdf_url'],'error_type':type(exc).__name__}
                response=getattr(exc,'response',None)
                if response is not None:error['http_status']=response.status_code
                report['errors'].append(error)
                print(json.dumps({'source_error':error}),flush=True)
            (args.output/'source-acquisition-report.json').write_text(json.dumps(report,indent=2))
        return report
    finally:
        if conn:conn.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--persist',action='store_true')
    parser.add_argument('--output',type=Path,default=Path('vd-pdcom-output'))
    args=parser.parse_args();report=run(args)
    if report['errors']:raise SystemExit(1)
